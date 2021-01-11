#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import sys
import traceback

import math
import time

DEFAULT_FORCE = False
DEFAULT_PARTITION = None


def lookup_doi (schol, graph, partition, pub):
    """
    use `publication_lookup()` across scholarly infrastructure APIs to
    identify this publication's open access PDFs, etc.
    """
    global DEFAULT_FORCE

    doi_list = []
    doi_match = False

    for source in ["original", schol.crossref.name, schol.europepmc.name, schol.openaire.name]:
        if (source in pub) and ("doi" in pub[source]):
            doi = graph.publications.verify_doi(pub[source]["doi"])

            if doi:
                doi_list.append(doi)
            else:
                message = "BAD DOI: |{}| in {} -- {}".format(pub[source]["doi"], source, pub["title"])
                graph.report_error(message)

    if len(doi_list) > 0:
        doi_tally = graph.tally_list(doi_list)
        doi, count = doi_tally[0]
        pub["doi"] = doi

        for api in [schol.semantic, schol.unpaywall, schol.dissemin]:
            try:
                if api.name in pub and not DEFAULT_FORCE:
                    # skip an API lookup that was performed previously
                    doi_match = True
                    continue
                else:
                    response = api.publication_lookup(doi)
                    if response.message:
                        print("Issue with: ", doi)
                        print(api.name)
                        print(response.message)
                        continue

                    if response.meta and len(response.meta) > 0:
                        doi_match = True
                        meta = dict(response.meta)
                        pub[api.name] = meta

            except Exception:
                # debug this as an edge case
                traceback.print_exc()
                print(pub["title"])
                print(doi)
                print(api.name)
                continue

    # send this publication along into the workflow stream
    return doi_match


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step3")

    # TODO: this logic might be better to be moved into RCApi
    semantic_scholar_requests_limits = 98
    semantic_scholar_time_limit = 59 * 5
    t0 = time.time()
    count = 1

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub_list.append(pub)

            time_elapsed = math.ceil (time.time() - t0)

            # already used all the API requests allowed in the time window
            if count == semantic_scholar_requests_limits and time_elapsed < semantic_scholar_time_limit:
                to_sleep = semantic_scholar_time_limit - math.floor(time_elapsed) + 1  # adding some extra margin
                print("API calls:",count,"time elapsed:", time_elapsed, "- will sleep:",to_sleep)
                time.sleep(to_sleep)
                count = 1
                t0 = time.time()
            # didn't got to the requests limit in the time window
            elif count < semantic_scholar_requests_limits and time_elapsed >= semantic_scholar_time_limit:
                count = 1  # adding some extra margin
                t0 = time.time()
                # print("API calls:", count, "time elapsed:", time_elapsed,"reseting counters...")

            doi_match = lookup_doi(schol, graph, partition, pub)

            count += 1

            if doi_match:
                graph.publications.doi_hits += 1
            else:
                graph.update_misses(partition, pub)

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)

    # report errors
    status = "{} successful DOI lookups".format(graph.publications.doi_hits)
    graph.report_misses(status, "publications that failed every DOI lookup")


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="publication lookup with DOIs across APIs to identify open access PDFs, etc."
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
        help="force API lookups, even if performed previously"
        )

    main(parser.parse_args())
