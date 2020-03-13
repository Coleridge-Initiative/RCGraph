#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import statistics
import sys
import unicodedata

DEFAULT_FORCE = False
DEFAULT_PARTITION = None


def troubleshoot_auths (auth_list):
    """
    troubleshoot: dissemin may return a HUGE author list
    """
    auth_stats = {}

    for api, results in auth_list.items():
        auth_stats[api] = len(results)

    threshold = statistics.median(auth_stats.values()) * 2.0

    for api, num_auth in auth_stats.items():
        if num_auth > threshold:
            del auth_list[api]

    return auth_list


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("author")

    # load the author entities
    if not args.force:
        graph.authors.load_entities()

    buckets = graph.authors.gen_temp_buckets()

    # for each publication: gather the author lists
    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        print("PARTITION", partition)
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub["title"] = unicodedata.normalize("NFKD", pub["title"]).strip()
            print("TITLE", pub["title"])

            pub["authors"] = []
            pub_list.append(pub)

            auth_list = troubleshoot_auths(graph.authors.find_authors(schol, pub))

            if len(auth_list) > 0:
                auth_ids = graph.authors.parse_auth_list(graph, auth_list)
                pub["authors"] = auth_ids
                graph.publications.auth_hits += 1
            else:
                ## error: pub has no authors?
                graph.update_misses(partition, pub)

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)

    ## rewrite the author files
    graph.authors.write_entities()

    # report errors
    status = "{} successful author lists".format(graph.publications.auth_hits)
    graph.report_misses(status, "publications that failed author parsing")


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="reconcile the author lists for each publication"
        )

    parser.add_argument(
        "--partition",
        type=str,
        default=DEFAULT_PARTITION,
        help="limit processing to a specified partition"
        )

    parser.add_argument(
        "--force",
        type=bool,
        default=DEFAULT_FORCE,
        help="force re-generating the author entities from scratch"
        )

    main(parser.parse_args())
