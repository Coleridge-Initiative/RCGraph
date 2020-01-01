#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
import codecs
import hashlib
import json
import logging  # type: ignore
import operator
import re
import string
import sys
import traceback
import unicodedata


class RCJournals:
    PATH_JOURNALS = Path("journals.json")
    PATH_UPDATE = Path("update_journals.json") 

    # these get dumped into other journal definitions -- 
    # unless they are the only option
    IGNORE_JOURNALS = set([
            "ssrn electronic journal"
            ])

    # both "unknown" and SSRN get used as placeholders
    IGNORE_ISSNS = set([
            "0000-0000",
            "1556-5068",
            "1945-7731",
            "1945-774X"
            ])


    def __init__ (self):
        self.seen_issn = self.IGNORE_ISSNS
        self.issn_hits = 0
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


    def load_entities (self, path=PATH_JOURNALS):
        """
        load the list of journal entities
        """
        with codecs.open(path, "r", encoding="utf8") as f:
            journals = json.load(f)

        # find the next ID to use
        ids = sorted([int(j["id"].replace("journal-", "")) for j in journals])
        self.next_id = ids[-1]

        # scan for duplicates
        self.known = {}

        for journal in journals:
            if "issn" in journal:
                for issn in journal["issn"]:
                    self.seen_issn.add(issn)

            for title in journal["titles"]:
                title_key = title.strip().lower()

                if title_key in self.known:
                    print("DUPLICATE JOURNAL: {}".format(title))
                else:
                    self.known[title_key] = journal


    def extract_issn (self, pub):
        """
        extract the ISSNs from metadata about the given publication
        """
        issn_list = []

        if "Dimensions" in pub:
            meta = pub["Dimensions"]

            if ("issn" in meta) and meta["issn"]:
                issn_list.extend(meta["issn"])

        if "Unpaywall" in pub:
            meta = pub["Unpaywall"]

            if ("journal_issns" in meta) and meta["journal_issns"]:
                issn_list.extend(meta["journal_issns"].split(","))

            if ("journal_issn_l" in meta) and meta["journal_issn_l"]:
                issn_list.append(meta["journal_issn_l"])

        if len(issn_list) > 0:
            issn_tally = RCGraph.tally_list(issn_list)
            freq_issn, count = issn_tally[0]
            return 1, freq_issn, issn_tally
        else:
            return 0, None, None


    def gather_issn (self, journal):
        """
        gather the ISSN metadata from the results of API calls
        """
        if "issn" in journal:
            old_issn = journal["issn"]
        else:
            old_issn = []

        if "NCBI" in journal:
            meta = journal["NCBI"]

            ## arrange the ISSN list
            if "ISSNLinking" in meta:
                issn_link = meta["ISSNLinking"]
            else:
                issn_link = None

            if "ISSN" in meta:
                l = meta["ISSN"]
                new_issn = [ i["#text"] for i in (l if isinstance(l, list) else [l]) ]

                journal["issn"] = RCGraph.make_ordered_list(
                    old_issn,
                    new_issn,
                    issn_link
                    )

            ## add the URL
            if "IndexingSelectedURL" in meta:
                journal["url"] = meta["IndexingSelectedURL"]

            ## arrange the title list
            journal["titles"] = RCGraph.make_ordered_list(
                journal["titles"],
                [ meta["Title"], meta["MedlineTA"] ],
                meta["ISOAbbreviation"]
                )


    def add_issns (self, pub, journal, issn_tally, disputed):
        """
        add ISSNs to the given journal
        """
        new_issns = []

        for issn, count in issn_tally:
            if ("-" in issn) and (issn not in self.seen_issn):
                new_issns.append(issn)

        if len(new_issns) > 0:
            if "issn" in journal:
                old_set = set(journal["issn"])
                new_set = set(new_issns)

                # don't care conditions
                if old_set == set(["0000-0000"]):
                    pass
                
                # got dispute? check for overlapping definitions
                elif len(old_set.intersection(new_set)) < 1:
                    disputed["{} {}".format(old_set, new_set)] = journal

                # add other ISSNs to an existing entry
                else:
                    for issn in new_issns:
                        if issn not in journal["issn"]:
                            journal["issn"].append(issn)
                            self.seen_issn.add(issn)

                # invariant: already performed ISSN lookup for this journal
                return None

            else:
                # add ISSNs to a journal that had none before
                journal["issn"] = []

                for issn in new_issns:
                    journal["issn"].append(issn)
                    self.seen_issn.add(issn)

                best_issn = new_issns[0]
                return best_issn

        else:
            # there were no new ISSNs to add
            return None


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


    def suggest_updates (self):
        """
        suggest an updated journal list
        """
        j_dict = {}

        for j in self.known.values():
            j_dict[j["id"]] = j

        with codecs.open(self.PATH_UPDATE, "wb", encoding="utf8") as f:
            j_list = list(j_dict.values())
            json.dump(j_list, f, indent=4, sort_keys=True, ensure_ascii=False)


class RCPublications:
    def __init__ (self):
        self.title_hits = 0
        self.doi_hits = 0
        self.pdf_hits = 0


    def verify_doi (self, doi):
        """
        attempt to verify a DOI, and clean up the value if needed
        """
        try:
            if not doi:
                # a `None` value is valid (== DOI is unknown)
                return None
            else:
                assert isinstance(doi, str)

                if doi.startswith("DOI:"):
                    doi = doi.replace("DOI:", "")
                elif doi.startswith("doi:"):
                    doi = doi.replace("doi:", "")

                doi = doi.strip()

                if doi.startswith("http://dx.doi.org/"):
                    doi = doi.replace("http://dx.doi.org/", "")
                elif doi.startswith("https://doi.org/"):
                    doi = doi.replace("https://doi.org/", "")
                elif doi.startswith("doi.org/"):
                    doi = doi.replace("doi.org/", "")

                assert len(doi) > 0
                assert doi.startswith("10.")

                # success
                return doi
        except:
            # failure
            return None


    def extract_urls (self, pub):
        """
        scan results from scholarly infrastructure APIs, apply
        business logic to extract a list of candidate open access PDF
        links for this publication
        """
        url_list = []

        # Unpaywall has mostly reliable metadata, except for PDFs
        if "Unpaywall" in pub:
            meta = pub["Unpaywall"]

            if "is_oa" in meta:
                if meta["is_oa"]:
                    best_meta = meta["best_oa_location"]

                    url = best_meta["url_for_landing_page"]

                    if url and isinstance(url, str):
                        url_list.append(url)

        # dissem.in is somewhat sparse / seems iffy
        if "dissemin" in pub and "paper" in pub["dissemin"]:
            records = pub["dissemin"]["paper"]["records"]

            if len(records) > 0:
                meta = records[0]
        
                if "splash_url" in meta:
                    url = meta["splash_url"]

                    if url and isinstance(url, str):
                        url_list.append(url)

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

        return url_list


    def extract_pdfs (self, pub):
        """
        scan results from scholarly infrastructure APIs, apply
        business logic to extract a list of candidate open access PDF
        links for this publication
        """
        pdf_list = []

        # EuropePMC has the best PDFs
        if "EuropePMC" in pub:
            meta = pub["EuropePMC"]

            if "pdf" in meta:
                pdf = meta["pdf"]

                if pdf:
                    pdf_list.append(pdf)

        # Unpaywall has mostly reliable metadata, except for PDFs
        if "Unpaywall" in pub:
            meta = pub["Unpaywall"]

            if "is_oa" in meta:
                if meta["is_oa"]:
                    best_meta = meta["best_oa_location"]
                    pdf = best_meta["url_for_pdf"]

                    if pdf:
                        pdf_list.append(pdf)

        # Dimensions metadata is verbose, if there
        if "Dimensions" in pub:
            meta = pub["Dimensions"]

            if "linkout" in meta:
                pdf = meta["linkout"]

                if pdf:
                    pdf_list.append(pdf)

        return pdf_list


class RCGraph:
    """
    methods for managing the Rich Context knowledge grapgh
    """
    BUCKET_FINAL = Path("bucket_final")
    BUCKET_STAGE = Path("bucket_stage")

    PATH_DATASETS = Path("datasets/datasets.json")
    PATH_PROVIDERS = Path("datasets/providers.json")

    PATH_MANUAL = Path("human/manual/partitions")
    PATH_PUBLICATIONS = Path("publications/partitions")


    def __init__ (self, step_name="generic"):
        self.step_name = step_name
        self.already_reported = set([])
        self.misses = []
        self.journals = RCJournals()
        self.publications = RCPublications()


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

        return "".join(filter(lambda x: x in string.printable, id))


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


    @classmethod
    def make_ordered_list (cls, old_list, new_list, lead_elem):
        """
        preserve the order of a list, prepending a lead element if it is
        not already a member
        """
        elf_list = list(map(lambda x: x.lower().strip(), old_list))

        for elem in new_list:
            elf_elem = elem.lower().strip()

            if elf_elem not in elf_list:
                old_list.append(elem)
                elf_list.append(elf_elem)

        if lead_elem:
            lead_elf = lead_elem.lower().strip()

            if lead_elf == elf_list[0]:
                # already good
                pass
            elif lead_elf not in elf_list:
                old_list.insert(0, lead_elem)
            else:
                idx = elf_list.index(lead_elf)
                del old_list[idx]

                old_list.insert(0, lead_elem)

        return old_list


    @classmethod
    def url_validator (cls, url):
        """
        validate the format of a URL
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc, result.path])
        except:
            return False


    def report_error (self, message):
        """
        avoid reporting the same error repeatedly
        """
        if message and not message in self.already_reported:
            self.already_reported.add(message)
            print(message)


    def iter_publications (self, path, filter=None):
        """
        iterate through the publication partitions
        """
        for partition in sorted(Path(path).glob("*.json")):
            if not filter or filter == partition.name:
                with codecs.open(partition, "r", encoding="utf8") as f:
                    try:
                        yield partition.name, json.load(f)
                    except Exception:
                        traceback.print_exc()
                        print(partition)


    def load_override (self, path=PATH_MANUAL):
        """
        load the publications metadata, apply the manually curated
        override metadata, then yield an iterator
        """
        override = {}

        for partition in sorted(Path(path).glob("*.json")):
            print("override:", partition)

            with codecs.open(partition, "r", encoding="utf8") as f:
                for elem in json.load(f):
                    override[elem["title"]] = elem["manual"]

        return override


    def write_partition (self, bucket, partition, pub_list):
        """
        write one partition to a bucket
        """
        with codecs.open(Path(bucket) / partition, "wb", encoding="utf8") as f:
            json.dump(pub_list, f, indent=4, sort_keys=True, ensure_ascii=False)


    def report_misses (self, status=None, trouble=None):
        """
        report the titles of publications that have metadata error
        conditions related to the current workflow step
        """
        filename = Path("misses_{}.txt".format(self.step_name))

        with codecs.open(filename, "wb", encoding="utf8") as f:
            if status:
                f.write("{}\n".format(status))
                f.write("---\n")

            for message in self.already_reported:
                f.write("{}\n".format(message))
                f.write("---\n")

            if trouble:
                f.write("{}:\n".format(trouble))

            for title in self.misses:
                f.write("{}\n".format(title))


######################################################################
## main entry point (not used)

if __name__ == "__main__":
    g = RCGraph()
    print(g)
