#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import sys
import traceback

DEFAULT_PARTITION = None


def gather_pdf (schol, graph, partition, pub):
    """
    use `publication_lookup()` across scholarly infrastructure APIs to
    identify this publication's open access PDFs, etc.
    """
    doi_list = []
    doi_match = False

    for source in ["original", schol.dimensions.name, schol.europepmc.name, schol.openaire.name]:
        if (source in pub) and ("doi" in pub[source]):
            doi = graph.publications.verify_doi(pub[source]["doi"])

            if doi:
                doi_list.append(doi)
            else:
                message = "BAD DOI: |{}| in {} -- {}".format(doi, source, pub["title"])
                graph.report_error(message)

    if len(doi_list) > 0:
        doi_tally = graph.tally_list(doi_list)
        doi, count = doi_tally[0]
        pub["doi"] = doi

        for api in [schol.semantic, schol.unpaywall, schol.dissemin]:
            try:
                meta = api.publication_lookup(doi)
            except Exception:
                # debug this as an edge case
                traceback.print_exc()
                print(pub["title"])
                print(doi)
                print(api.name)
                continue

            if meta and len(meta) > 0:
                doi_match = True
                meta = dict(meta)
                pub[api.name] = meta

    # send this publication along into the workflow stream
    return doi_match


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step3")

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub_list.append(pub)
            doi_match = gather_pdf(schol, graph, partition, pub)

            if doi_match:
                graph.publications.doi_hits += 1
            else:
                graph.misses.append(pub["title"])

            if "pdf" in pub:
                graph.publications.pdf_hits += 1

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)

    # report titles for publications that failed every API lookup
    status = "{} successful DOI lookups / {} found PDFs".format(graph.publications.doi_hits, graph.publications.pdf_hits)
    graph.report_misses(status)


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

    main(parser.parse_args())
