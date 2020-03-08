#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from typing import Any, Dict, List, Tuple
import argparse
import json
import pprint
import sys
import unicodedata

DEFAULT_PARTITION = None


def propagate_view (pub, graph, override):
    """
    propagate a view of this publication -- now with its
    enhanced/corrected metadata -- into the workflow stream
    """
    title = pub["title"]

    view = {
        "title": unicodedata.normalize("NFKD", title),
        "datasets": pub["datasets"],
        "authors": pub["authors"]
        }

    # add the DOI, if available
    if "doi" in pub:
        view["doi"] = pub["doi"]

    # pick the best URls
    url_list = graph.publications.extract_urls(pub)

    if len(url_list) > 0:
        tally = graph.tally_list(url_list)
        url, count = tally[0]

        # fix these common errors in DOI-based URLs
        if url.startswith("www.doi"):
            url = url.replace("www.doi", "https://doi")

        if "doi.org10" in url:
            url = url.replace("doi.org10", "doi.org/10")

        view["url"] = url

    # add the PDF, if available
    pdf_list = graph.publications.extract_pdfs(pub)

    if len(pdf_list) > 0:
        view["pdf"] = pdf_list[0]

    # add the abstract, if available
    if "abstract" in pub:
        if pub["abstract"] and len(pub["abstract"]) > 0:
            view["abstract"] = pub["abstract"]

    # add the DOI, if available
    if "keyphrases" in pub:
        view["keyphrases"] = pub["keyphrases"]

    # select the best journal
    journal_list = graph.journals.extract_journals(pub)
    journal = graph.journals.select_best_entity(journal_list)
    view["journal"] = journal["id"]

    # apply the manual override
    if title in override:
        override[title]["used"] = True

        if "omit-corpus" in override[title] and override[title]["omit-corpus"]:
            # omit this publication from the public corpus
            view["omit"] = True

        for key in ["abstract", "doi", "pdf", "journal", "url"]:
            if key in override[title]:
                view[key] = override[title][key]

                # special case for known null values
                if not view[key]:
                    del view[key]

            if "datasets" in override[title]:
                for dataset in override[title]["datasets"]:
                    if not dataset in view["datasets"]:
                        view["datasets"].append(dataset)
            elif "datasets" not in view:
                view["datasets"] = []

            if "authors" in override[title]:
                for author in override[title]["authors"]:
                    if not author in view["authors"]:
                        view["authors"].append(author)
            elif "authors" not in view:
                view["authors"] = []

    #pprint.pprint(view)
    return view


def main (args):
    # initialize the federated API access
    graph = rc_graph.RCGraph("final")

    # finalize the metadata corrections for each publication
    graph.journals.load_entities()
    override = graph.load_override()

    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        pub_list = []
        print("working: {}".format(partition))

        for pub in pub_iter:
            view = propagate_view(pub, graph, override)
            pub_list.append(view)

            if "pdf" in view:
                graph.publications.pdf_hits += 1
            else:
                graph.update_misses(partition, view)

        graph.write_partition(graph.BUCKET_FINAL, partition, pub_list)

    # did we miss any of the manual entries?
    # TODO: refactor these into a partition in RCPublications
    pub_list = []

    for title, pub in override.items():
        if "used" not in pub and "datasets" in pub:
            if "omit-corpus" in pub and pub["omit-corpus"]:
                continue
            else:
                pub["title"] = title
                pub_list.append(pub)

                if "authors" not in pub:
                    pub["authors"] = []

                if "pdf" in pub:
                    graph.publications.pdf_hits += 1
                else:
                    graph.update_misses(partition, pub)

    graph.write_partition(graph.BUCKET_FINAL, "_manual.json", pub_list)

    # report errors
    status = "{} open access PDFs identified".format(graph.publications.pdf_hits)
    graph.report_misses(status, "titles that had no open access PDF")


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="finalize metadata corrections, along with manual override"
        )

    parser.add_argument(
        "--partition",
        type=str,
        default=DEFAULT_PARTITION,
        help="limit processing to a specified partition"
        )

    main(parser.parse_args())
