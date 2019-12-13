#!/usr/bin/env python
# encoding: utf-8

import glob
import hashlib
import json
import operator
import os
import re
import traceback


class RCJournals:
    IGNORE_JOURNALS = set([
            "ssrn electronic journal"
            ])

    def __init__ (self):
        self.next_id = 0
        self.known = {}


    def add_entity (self, tally):
        """
        add the tally for this (apparently) new journal
        """
        title, count = tally[0]
        title_key = title.strip().lower()

        if title_key not in self.known:
            self.next_id += 1

            entity = {
                "id": "journal-{:03d}".format(self.next_id),
                "titles": [j for j, c in tally]
                }

            for title in entity["titles"]:
                if title_key not in self.IGNORE_JOURNALS:
                    self.known[title_key] = entity

            return entity
        else:
            return None


    def load_entities (self, path="journals.json"):
        """
        load the list of journal entities
        """
        with open(path, "r") as f:
            journals = json.load(f)

        # find the next ID to use
        ids = sorted([j["id"] for j in journals])
        m = re.search("journal\-(\d+)", ids[-1])

        self.next_id = int(m.group(1))

        # scan for duplicates
        self.known = {}

        for journal in journals:
            for title in journal["titles"]:
                title_key = title.strip().lower()

                if title_key in self.known:
                    print("DUPLICATE JOURNAL: {}".format(title))
                else:
                    self.known[title_key] = journal


    def extract_journals (self, pub):
        """
        scan results from scholarly infrastructure APIs, apply
        business logic to extract a list of candidate journals for
        this publication
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


    def select_best_entity (self, journal_list):
        """
        select the best journal entity for a publication from among
        its identified list of journal names
        """
        if len(journal_list) > 0:
            tally = RCGraph.tally_list(journal_list, ignores=self.IGNORE_JOURNALS)
            title, count = tally[0]
            title_key = title.strip().lower()

            if title_key in self.known:
                return self.known[title_key]
            else:
                return self.known["unknown"]
        else:
            # no journal identified
            return self.known["unknown"]


class RCGraph:
    """
    methods for managing the Rich Context knowledge grapgh
    """
    def __init__ (self, step_name="generic"):
        self.step_name = step_name
        self.misses = []
        self.journals = RCJournals()


    @classmethod
    def get_hash (cls, strings, prefix=None, digest_size=10):
        """
        construct a unique identifier from a collection of strings
        """
        m = hashlib.blake2b(digest_size=digest_size)
    
        for elem in sorted(map(lambda x: x.encode("utf-8").lower().strip(), strings)):
            m.update(elem)

        if prefix:
            id = prefix + m.hexdigest()
        else:
            id = m.hexdigest()

        return id


    @classmethod
    def tally_list (cls, l, ignores=set([])):
        """
        sort a list in descending order of most frequent element
        """
        trans = dict([ (x.strip().lower(), x) for x in l])
        lower_l = list(map(lambda x: x.strip().lower(), l))
        keys = set(lower_l)
        enum_dict = {}

        for key in keys:
            if key in ignores:
                enum_dict[trans[key]] = 0 
            elif "html_ent glyph=" in key:
                enum_dict[trans[key]] = 0 
            elif "&amp;" in key:
                enum_dict[trans[key]] = 0 
            else:
                enum_dict[trans[key]] = lower_l.count(key)


        return sorted(enum_dict.items(), key=operator.itemgetter(1), reverse=True)


    def iter_publications (self, path, filter=None):
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


    def report_misses (self):
        """
        report the titles of publications that have metadata error
        conditions related to the current workflow step
        """
        filename = "misses_{}.txt".format(self.step_name)

        with open(filename, "w") as f:
            for title in self.misses:
                f.write("{}\n".format(title))


######################################################################
## main entry point (not used)

if __name__ == "__main__":
    g = RCGraph()
    print(g)
