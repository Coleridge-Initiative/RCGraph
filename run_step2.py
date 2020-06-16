#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import json
import pprint
import sys
import traceback
import unicodedata

DEFAULT_PARTITION = None


def gather_doi (schol, graph, partition, pub):
    """
    use `title_search()` across scholarly infrastructure APIs to
    identify this publication's DOI, etc.
    """
    title = pub["title"]
    title_match = False

    for api in [schol.openaire, schol.europepmc, schol.dimensions]:
        try:
            if api.has_credentials():
                response = api.title_search(title)

                if response.message:
                    print("Issue with: ", title)
                    print(api.name)
                    print(response.message)
                    continue

                if response.meta and len(response.meta) > 0:
                    title_match = True
                    meta = dict(response.meta)
                    pub[api.name] = meta

        except Exception:
            # debug this as an edge case
            traceback.print_exc()
            print(title)
            print(api.name)
            continue

    # send this publication along into the workflow stream
    return title_match


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step2")

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS, filter=args.partition):
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub["title"] = unicodedata.normalize("NFKD", pub["title"]).strip()
            pub_list.append(pub)

            title_match = gather_doi(schol, graph, partition, pub)

            if title_match:
                graph.publications.title_hits += 1
            else:
                graph.update_misses(partition, pub)

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)

    # report errors
    status = "{} found titles in API calls".format(graph.publications.title_hits)
    trouble = "publications that failed every API lookup"
    graph.report_misses(status, trouble)


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="title search across APIs to identify DOI and other metadata for each publication"
        )

    parser.add_argument(
        "--partition",
        type=str,
        default=DEFAULT_PARTITION,
        help="limit processing to a specified partition"
        )

    main(parser.parse_args())
