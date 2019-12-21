#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from typing import Any, Dict, List, Tuple
import json
import pprint
import sys


def load_override (path=rc_graph.RCGraph.PATH_MANUAL):
    """
    load the publications metadata, apply the manually curated
    override metadata, then yield an iterator
    """
    override = {}

    for filename in Path(path).glob("*.json"):
        print("override:", filename)

        with open(filename) as f:
            for elem in json.load(f):
                override[elem["title"]] = elem["manual"]

    return override


def propagate_view (pub, graph, override):
    """
    propagate a view of this publication -- now with its
    enhanced/corrected metadata -- into the workflow stream
    """
    title = pub["title"]

    view = {
        "title": title,
        "datasets": pub["datasets"]
        }

    # pick the best URls
    url_list = graph.publications.extract_urls(pub)

    if len(url_list) > 0:
        tally = graph.tally_list(url_list)
        url, count = tally[0]
        view["url"] = url

    # add the PDF, if available
    pdf_list = graph.publications.extract_pdfs(pub)

    if len(pdf_list) > 0:
        view["pdf"] = pdf_list[0]

    # add the DOI, if available
    if "doi" in pub:
        view["doi"] = pub["doi"]

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

        for key in ["doi", "pdf", "journal", "url"]:
            if key in override[title]:
                view[key] = override[title][key]

                # special case for known null values
                if not view[key]:
                    del view[key]

            if "datasets" in override[title]:
                for dataset in override[title]["datasets"]:
                    if not dataset in view["datasets"]:
                        view["datasets"].append(dataset)

    #pprint.pprint(view)
    return view


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step5")
    graph.journals.load_entities()

    # for each partition, for each publication
    # finalize the metadata corrections 
    override = load_override()

    for partition, pub_iter in graph.iter_publications(path="step3"):
        pub_list = []
        print("working: {}".format(partition))

        for pub in pub_iter:
            view = propagate_view(pub, graph, override)
            pub_list.append(view)

            if "pdf" in view:
                graph.publications.pdf_hits += 1
            else:
                graph.misses.append(view["title"])

        graph.write_partition(graph.BUCKET_FINAL, partition, pub_list)

    # did we miss any of the manual entries?
    # TODO: refactor these into a partition in RCPublications
    pub_list = []

    for title, pub in override.items():
        if "used" not in pub:
            if "omit-corpus" in pub and pub["omit-corpus"]:
                continue
            else:
                pub["title"] = title
                pub_list.append(pub)

                if "pdf" in pub:
                    graph.publications.pdf_hits += 1
                else:
                    graph.misses.append(title)

    graph.write_partition(graph.BUCKET_FINAL, "_manual.json", pub_list)

    # keep track of the titles that had no open access PDF
    graph.report_misses()
    print("PDFs: {}".format(graph.publications.pdf_hits))
