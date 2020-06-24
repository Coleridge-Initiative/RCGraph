#!/usr/bin/env python
# encoding: utf-8
import math
import time

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
            message = None

            if api.has_credentials():
                meta, timing, message = api.title_search(title)
        except Exception:
            # debug this as an edge case
            traceback.print_exc()
            print(title)
            print(api.name)
            print(message)
            continue

        if meta and len(meta) > 0:
            title_match = True
            meta = dict(meta)
            pub[api.name] = meta

    # send this publication along into the workflow stream
    return title_match


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step2")

    # The Dimensions Analytics API is limited to 30 requests per IP address per minute. Source https://docs.dimensions.ai/dsl/api.html
    dimensions_requests_limits = 30
    dimensions_time_limit = 60
    t0 = time.time()
    count = 0

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS, filter=args.partition):
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub["title"] = unicodedata.normalize("NFKD", pub["title"]).strip()
            pub_list.append(pub)

            time_elapsed = time.time() - t0

            # already used all the API requests allowed in the time window
            if count == dimensions_requests_limits and time_elapsed < dimensions_time_limit:
                to_sleep = dimensions_time_limit - math.floor(time_elapsed) + 1 # adding some extra margin
                #print("API calls:",count,"time elapsed:", time_elapsed, "- will sleep:",to_sleep)
                time.sleep( to_sleep )
                count = 0
                t0 = time.time()
            # didn't got to the requests limit in the time window
            elif count < dimensions_requests_limits and time_elapsed >= dimensions_time_limit:
                count = 1 # adding some extra margin
                t0 = time.time()
                #print("API calls:", count, "time elapsed:", time_elapsed,"reseting counters...")

            title_match = gather_doi(schol, graph, partition, pub)

            count += 1

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
