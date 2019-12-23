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

DEFAULT_PARTITION = None


def gather_doi (pub, partition, schol, graph):
    """
    use `title_search()` across scholarly infrastructure APIs to
    identify this publication's DOI, etc.
    """
    title = pub["title"]
    title_match = False

    doi_list = []

    if "doi" in pub["original"]:
        possible_doi = pub["original"]["doi"]

        if possible_doi:
            doi = graph.publications.verify_doi(possible_doi)

            if doi:
                doi_list.append(doi)
            else:
                message = "BAD DOI: |{}| in {} -- {}".format(doi, "original", pub["title"])
                report_error(message)

    for api in [schol.openaire, schol.europepmc, schol.dimensions]:
        try:
            meta = api.title_search(title)
        except Exception:
            # debugging an edge case
            traceback.print_exc()
            print(title)
            print(api.name)
            pprint.pprint(pub)
            continue

        if meta and len(meta) > 0:
            title_match = True
            meta = dict(meta)
            #pprint.pprint(meta)

            if "doi" in meta:
                doi = graph.publications.verify_doi(meta["doi"])

                if doi:
                    doi_list.append(doi)
                else:
                    message = "BAD DOI: |{}| in {} -- {}".format(doi, api.name, pub["title"])
                    report_error(message)

            pub[api.name] = meta

    # keep track of the titles that fail all API lookups
    if not title_match:
        graph.misses.append(title)

    # select the most frequently reported DOI -- to avoid secondary
    # DOIs, such as SSRN, dominating the metadata
    if len(doi_list) > 0:
        tally = graph.tally_list(doi_list)
        doi, count = tally[0]
        pub["doi"] = doi

    # send this publication along into the workflow stream
    return pub


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step2")

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS, filter=args.partition):
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub_list.append(gather_doi(pub, partition, schol, graph))

            if "doi" in pub:
                graph.publications.doi_hits += 1

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)

    # report titles for publications that failed every API lookup
    status = "{} found DOIs".format(graph.publications.doi_hits)
    graph.report_misses(status)


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
