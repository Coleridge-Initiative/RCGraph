#!/usr/bin/env python
# encoding: utf-8

from util import iter_publications, tally_list
import json
import re
import sys


IGNORE_JOURNALS = set([
        "ssrn electronic journal"
        ])


def extract_journals (pub):
    """
    scan results from scholarly infrastructure APIs, apply business
    logic to extract a list of candidate journals for this publication
    """
    journal_list = []

    # EuropePMC has the best PDFs
    if "EuropePMC" in pub:
        meta = pub["EuropePMC"]

        if "journal" in meta:
            journal = meta["journal"]

            if journal and isinstance(journal, str):
                journal_list.append(journal)

    # Unpaywall has mostly reliable metadata, except for PDFs
    if "Unpaywall" in pub:
        meta = pub["Unpaywall"]

        if "journal_name" in meta:
            journal = meta["journal_name"]

            if journal and isinstance(journal, str):
                journal_list.append(journal)

    # dissem.in is somewhat sparse / seems iffy
    if "dissemin" in pub and "paper" in pub["dissemin"]:
        records = pub["dissemin"]["paper"]["records"]

        if len(records) > 0:
            meta = records[0]

            if "journal" in meta:
                journal = meta["journal"]

                if journal and isinstance(journal, str):
                    journal_list.append(journal)

    # Dimensions metadata is verbose, if there
    if "Dimensions" in pub:
        meta = pub["Dimensions"]

        if "journal" in meta:
            if meta["journal"] and "title" in meta["journal"]:
                journal = meta["journal"]["title"]

                if journal and isinstance(journal, str):
                    journal_list.append(journal)

    # Semantic Scholar -- could be better
    # has good open access but doesn't share
    # also, beware their use of "arXiv" as a journal
    if "Semantic Scholar" in pub:
        meta = pub["Semantic Scholar"]

        if "venue" in meta:
            journal = meta["venue"]

            if journal and isinstance(journal, str):
                if journal.lower() != "arxiv":
                    journal_list.append(journal)

    # original metadata from data ingest
    if "original" in pub:
        meta = pub["original"]

        if "journal" in meta:
            journal = meta["journal"]

            if journal and isinstance(journal, str):
                # TODO: input metadata for `original` is not consistent
                #print("original", journal)
                #journal_list.append(journal)
                pass

    return journal_list


def reconcile_journal (pub, misses, journals):
    """
    for each publication, identify a list of its candidate journals,
    or mark as a miss
    """
    journal_list = extract_journals(pub)

    if len(journal_list) > 0:
        tally = tally_list(journal_list, ignores=IGNORE_JOURNALS)
        journals.append(tally)
    else:
        misses.append(pub["title"])


if __name__ == "__main__":
    # load the list of journals
    journals_path = "journals.json"

    with open(journals_path, "r") as f:
        journals = json.load(f)

    ids = sorted([j["id"] for j in journals])
    m = re.search("journal\-(\d+)", ids[-1])
    next_id = int(m.group(1))
    seen = {}

    for journal in journals:
        for title in journal["titles"]:
            title_key = title.strip().lower()

            if title_key in seen:
                print("DUPLICATE JOURNAL: {}".format(title))
            else:
                seen[title_key] = journal

    # for each publication: reconcile journal names
    misses = []
    journals = []

    for partition, pub_iter in iter_publications(path="step3"):
        for pub in pub_iter:
            reconcile_journal(pub, misses, journals)

    # tentative list of journals to be considered for adding
    for tally in journals:
        title, count = tally[0]
        title_key = title.strip().lower()

        if title_key not in seen:
            # add the tally for this (apparently) new journal
            next_id += 1
            entry = {
                "id": "journal-{:03d}".format(next_id),
                "titles": [j for j, c in tally]
                }

            print(json.dumps(entry, indent=2, sort_keys=True))

            for title in entry["titles"]:
                if title_key not in IGNORE_JOURNALS:
                    seen[title_key] = entry

    # report titles for publications that don't have a journal
    with open("misses_step3a.txt", "w") as f:
        for title in misses:
            f.write("{}\n".format(title))
