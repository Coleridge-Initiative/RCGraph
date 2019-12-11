#!/usr/bin/env python
# encoding: utf-8

from graph import RCGraph
from richcontext import scholapi as rc_scholapi
import json
import pprint
import sys


def gather_pdf (pub, partition, pub_list, graph):
    """
    scan results from scholarly infrastructure APIs, apply business
    logic to identify this publication's open access PDFs, etc.
    """
    url_list = []

    title = pub["title"]
    pdf_match = False

    # EuropePMC has the best PDFs
    if "EuropePMC" in pub:
        meta = pub["EuropePMC"]

        if "pdf" in meta:
            pdf = meta["pdf"]

            if pdf:
                pub["pdf"] = pdf
                pdf_match = True

    # Unpaywall has mostly reliable metadata, except for PDFs
    if "Unpaywall" in pub:
        meta = pub["Unpaywall"]

        if "is_oa" in meta:
            if meta["is_oa"]:
                best_meta = meta["best_oa_location"]

                url = best_meta["url_for_landing_page"]

                if url and isinstance(url, str):
                    url_list.append(url)

                pdf = best_meta["url_for_pdf"]

                if pdf and not "pdf" in pub:
                    pub["pdf"] = pdf
                    pdf_match = True

    # dissem.in is somewhat sparse / seems iffy
    if "dissemin" in pub and "paper" in pub["dissemin"]:
        records = pub["dissemin"]["paper"]["records"]

        if len(records) > 0:
            meta = records[0]
        
            if "splash_url" in meta:
                url = meta["splash_url"]

                if url and isinstance(url, str):
                    url_list.append(url)

    # Dimensions metadata is verbose, if there
    if "Dimensions" in pub:
        meta = pub["Dimensions"]

        if "linkout" in meta:
            pdf = meta["linkout"]

            if pdf and not "pdf" in pub:
                pub["pdf"] = pdf
                pdf_match = True

    # OpenAIRE is generally good
    if "OpenAIRE" in pub:
        meta = pub["OpenAIRE"]

        if "url" in meta:
            url = meta["url"]

            if url and isinstance(url, str):
                url_list.append(url)

    # Semantic Scholar -- could be better, has good open access but doesn't share it
    if "Semantic Scholar" in pub:
        meta = pub["Semantic Scholar"]

        if "url" in meta:
            url = meta["url"]

            if url and isinstance(url, str):
                url_list.append(url)

    # original metadata from data ingest
    if "original" in pub:
        meta = pub["original"]

        if "url" in meta:
            url = meta["url"]

            if url and isinstance(url, str):
                url_list.append(url)

    # keep track of the titles that had no open access PDF
    if not pdf_match:
        graph.misses.append(title)

    # send a view of this publication along into the workflow stream
    view = {
        "title": title,
        "datasets": pub["datasets"]
        }

    if "doi" in pub:
        view["doi"] = pub["doi"]

    if pdf_match:
        view["pdf"] = pub["pdf"]

    if len(url_list) > 0:
        tally = graph.tally_list(url_list)
        url, count = tally[0]
        view["url"] = url

    # select the best journal
    journal_list = graph.journals.extract_journals(pub)
    journal = graph.journals.select_best_entity(journal_list)
    view["journal"] = journal["id"]

    #pprint.pprint(view)
    pub_list.append(view)


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    graph = RCGraph("step5")
    graph.journals.load_entities()

    # for each publication: gather the open access PDFs, etc.
    pdf_hits = 0

    for partition, pub_iter in graph.iter_publications(path="step3"):
        print("working: {}".format(partition))
        pub_list = []

        for pub in pub_iter:
            gather_pdf(pub, partition, pub_list, graph)

        for pub in pub_list:
            if "pdf" in pub:
                pdf_hits += 1

        with open("step5/" + partition, "w") as f:
            json.dump(pub_list, f, indent=4, sort_keys=True)

    print("PDFs: {}".format(pdf_hits))

    # report titles for publications that failed every API lookup
    graph.report_misses()
