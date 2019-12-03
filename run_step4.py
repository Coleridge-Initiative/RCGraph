#!/usr/bin/env python
# encoding: utf-8

from richcontext import scholapi as rc_scholapi
import glob
import json
import operator
import os
import pprint
import sys
import traceback


def iter_publications (path, filter=None):
    """
    iterate through the publication partitions
    """
    for partition in glob.glob(path + "/*.json"):
        if not filter or partition.endswith(filter):
            with open(partition) as f:
                try:
                    yield os.path.basename(partition), json.load(f)
                except Exception:
                    traceback.print_exc()
                    print(partition)


def tally_list (l):
    """
    sort a list in descending order of most frequent element
    """
    trans = dict([ (x.lower(), x) for x in l])
    lower_l = list(map(lambda x: x.lower(), l))
    keys = set(lower_l)
    enum_dict = {}

    for key in keys:
        enum_dict[trans[key]] = lower_l.count(key)

    return sorted(enum_dict.items(), key=operator.itemgetter(1), reverse=True)


def gather_pdf (pub, partition, pub_list, misses):
    """
    scan results from scholarly infrastructure APIs, apply business
    logic to identify this publication's open access PDFs, etc.
    """
    url_list = []
    journal_list = []

    title = pub["title"]
    pdf_match = False

    # EuropePMC has the best PDFs
    if "EuropePMC" in pub:
        meta = pub["EuropePMC"]

        if "journal" in meta:
            journal = meta["journal"]

            if journal and isinstance(journal, str):
                journal_list.append(journal)

        if "pdf" in meta:
            pdf = meta["pdf"]

            if pdf:
                pub["pdf"] = pdf
                pdf_match = True

    # Unpaywall has mostly reliable metadata, except for PDFs
    if "Unpaywall" in pub:
        meta = pub["Unpaywall"]

        if "journal_name" in meta:
            journal = meta["journal_name"]

            if journal and isinstance(journal, str):
                journal_list.append(journal)

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

            if "journal" in meta:
                journal = meta["journal"]

                if journal and isinstance(journal, str):
                    journal_list.append(journal)

    # Dimensions metadata is verbose, if there
    if "Dimensions" in pub:
        meta = pub["Dimensions"]

        if "linkout" in meta:
            pdf = meta["linkout"]

            if pdf and not "pdf" in pub:
                pub["pdf"] = pdf
                pdf_match = True

        if "journal" in meta:
            if meta["journal"] and "title" in meta["journal"]:
                journal = meta["journal"]["title"]

                if journal and isinstance(journal, str):
                    journal_list.append(journal)

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

        if "venue" in meta:
            journal = meta["venue"]

            if journal and isinstance(journal, str):
                journal_list.append(journal)

    # original metadata from data ingest
    if "original" in pub:
        meta = pub["original"]

        if "url" in meta:
            url = meta["url"]

            if url and isinstance(url, str):
                url_list.append(url)

        if "journal" in meta:
            journal = meta["journal"]

            if journal and isinstance(journal, str):
                journal_list.append(journal)

    # keep track of the titles that had no open access PDF
    if not pdf_match:
        misses.append(title)

    # send a view of this publication along into the workflow stream
    view = {
        "title": title,
        "datasets": pub["datasets"]
        }

    if "doi" in pub:
        view["doi"] = pub["doi"]

    if pdf_match:
        view["pdf"] = pub["pdf"]

    if len(journal_list) > 0:
        tally = tally_list(journal_list)
        journal, count = tally[0]
        view["journal"] = journal

    if len(url_list) > 0:
        tally = tally_list(url_list)
        url, count = tally[0]
        view["url"] = url

    #pprint.pprint(view)
    pub_list.append(view)


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    # for each publication: gather the open access PDFs, etc.
    pdf_hits = 0
    misses = []

    for partition, pub_iter in iter_publications(path="step3"):
        print("working: {}".format(partition))
        pub_list = []

        for pub in pub_iter:
            gather_pdf(pub, partition, pub_list, misses)

        for pub in pub_list:
            if "pdf" in pub:
                pdf_hits += 1

        with open("step4/" + partition, "w") as f:
            json.dump(pub_list, f, indent=4, sort_keys=True)

    print("PDFs: {}".format(pdf_hits))

    # report titles for publications that failed every API lookup
    with open("misses_step4.txt", "w") as f:
        for title in misses:
            f.write("{}\n".format(title))
