#!/usr/bin/env python
# encoding: utf-8

from collections import Counter, OrderedDict, defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
import codecs
import csv
import hashlib
import json
import logging  # type: ignore
import operator
import re
import string
import sys
import traceback
import unicodedata
import unidecode


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


    def add_entity (self, tally, freq_issn):
        """
        add the tally for this (apparently) new journal
        """
        title, count = tally[0]
        title_key = title.strip().lower()

        if title_key not in self.known:
            self.next_id += 1

            entity = {
                "id": "journal-{:03d}".format(self.next_id),
                "issn": [ freq_issn ],
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
                meta["ISOAbbreviation"] if "ISOAbbreviation" in meta else None
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
        self.pub_count = 0
        self.title_hits = 0
        self.doi_hits = 0
        self.pdf_hits = 0
        self.auth_hits = 0
        self.ab_hits = 0
        self.key_hits = 0


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
                elif doi.startswith("http://doi.org/"):
                    doi = doi.replace("http://doi.org/", "")

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

        # Manually input by human reviewers
        if "original" in pub:
            meta = pub["original"]
            if "pdf" in meta:
                pdf = meta["pdf"]

                if pdf and pdf.endswith(".pdf"):
                    pdf_list.append(pdf)

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


class RCAuthors_Buckets:
    MIN_NAME_SIMILARITY = 0.80

    PID_KEYS = set([ "orcid", "semschol", "dimensions", "viaf" ])

    SOURCE_MAP = {
        "Unpaywall": 1,
        "OpenAIRE": 2,
        "Dimensions": 3,
        "dissemin": 4,
        "Semantic Scholar": 5
        }

    def __init__ (self):
        self.bucket = {}
        self.pid_map = {}
        self.uuid_map = {}
        self.decision_trace = []


    def merge (self, graph, other_buckets):
        """
        merge new authors from another bucket list
        """
        auth_ids = []

        for key, auths in other_buckets.bucket.items():
            if key not in self.bucket:
                # transfer the bucket wholesale
                self.bucket[key] = auths

                for auth in auths:
                    self.update_pid_maps(auth, auth)
                    uuid = auth["uuid"]
                    auth_ids.append(uuid)

                    if uuid not in self.uuid_map:
                        self.uuid_map[uuid] = auth
            else:
                for auth in auths:
                    features = self.get_features(auth)
                    hit_auth, feat_vec = self.pid_lookup(key, auth, features, 0)

                    if hit_auth:
                        # direct match using PIDs
                        self.update_auth(auth, hit_auth)
                        self.update_pid_maps(auth, hit_auth)
                        auth_ids.append(hit_auth["uuid"])
                    else:
                        # compare the author to known authors
                        neighbors = []

                        for certainty, hit_auth, feat_vec in self.match_auth(key, auth, features, 0):
                            neighbors.append([ certainty, hit_auth, feat_vec ])

                        if len(neighbors) > 0:
                            # select the nearest neighbor
                            certainty, hit_auth, feat_vec = sorted(neighbors, key=lambda x: x[0], reverse=True)[0]
                            self.add_decision(True, certainty, hit_auth, auth, feat_vec)

                            self.update_auth(auth, hit_auth)
                            self.update_pid_maps(auth, hit_auth)
                            auth_ids.append(hit_auth["uuid"])
                        else:
                            # this one seems new; add a new author
                            uuid = self.add_auth(graph, key, auth)
                            auth_ids.append(auth["uuid"])

                            if uuid not in self.uuid_map:
                                self.uuid_map[uuid] = auth

        self.decision_trace.extend(other_buckets.decision_trace)
        return auth_ids


    def read_authors (self, path):
        """
        restore the data structures from a file
        """
        with codecs.open(path, "r", encoding="utf8") as f:
            j = json.load(f)
            self.bucket = j["bucket"]
            self.pid_map = j["pid_map"]

        # rehydrate the UUID map
        for key, auths in self.bucket.items():
            for auth in auths:
                uuid = auth["uuid"]
                self.uuid_map[uuid] = auth


    def write_authors (self, path):
        """
        serialize the data structures out to a file
        """
        sort_func = lambda k: k["surname"] + "," + k["given"]
        bucket = {}

        for key, auths in sorted(self.bucket.items()):
            bucket[key] = list(sorted(auths, key=sort_func))

        out_view = {
            "bucket": bucket,
            "pid_map": self.pid_map
            }

        with codecs.open(path, "wb", encoding="utf8") as f:
            json.dump(out_view, f, indent=1, sort_keys=True, separators=(",", ": "), ensure_ascii=False)


    def add_decision (self, match, certainty, hit_auth, auth, feat_vec):
        """
        trace the decisions made by self-supervision
        """
        values = [ v for k, v in sorted(feat_vec.items()) ]

        decision = [
            1 if match else 0,
            certainty,
            hit_auth["uuid"],
            hit_auth["surname"],
            hit_auth["given"],
            auth["surname"],
            auth["given"],
            ]

        self.decision_trace.append(values + decision)


    def write_decisions (self, path):
        """
        serialize the decisions made by self-supervision
        """
        headers = [
            "match", "certainty", "uuid",
            "hit_sur", "hit_giv", "auth_sur", "auth_giv",
            "source", "fuzzy",
            "sur_equal", "sur_len_diff", "sur_nrm_equal", "sur_dec_equal", "sur_sim",
            "giv_equal", "giv_len_diff", "giv_nrm_equal", "giv_dec_equal", "giv_sim",
            "init_sub"
            ]

        with open(path, "w") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(headers)

            for decision in self.decision_trace:
                row = [x.encode("utf8") if isinstance(x, str) else x for x in decision]
                writer.writerow(row)


    def get_features (self, auth):
        """
        prepare features for one author
        """
        features = {}
        #print(Counter(n1))

        features["fuzzy"] = auth["fuzzy"]

        features["sur"] = auth["surname"].lower().strip()
        features["giv"] = auth["given"].replace(".", "").lower().strip()

        features["sur_nrm"] = RCAuthors.normalize_name(features["sur"])
        features["giv_nrm"] = RCAuthors.normalize_name(features["giv"]).strip(",")

        features["sur_dec"] = unidecode.unidecode(features["sur"])
        features["giv_dec"] = unidecode.unidecode(features["giv"])

        giv_init = [ s[0] for s in features["giv"].replace("  ", " ").split(" ") ]
        features["init"] = "".join([ features["sur"][0] ] + giv_init)

        return features


    def gen_auth_key (self, features):
        """
        generate a hash key for an author, assuming they have at least
        one character in both their surname and given name
        """
        initials = (features["sur_dec"][0] + features["giv_dec"][0]).lower()
        return unidecode.unidecode(RCAuthors.normalize_name(initials))


    def get_source_id (self, source):
        """
        translate from source name to a numeric representation (level)
        """
        if source not in self.SOURCE_MAP:
            source_id = 0
        else:
            source_id = self.SOURCE_MAP[source]

        return source_id


    def update_auth (self, auth, hit_auth):
        """
        update the known author's metadata
        """
        if auth["fuzzy"] > hit_auth["fuzzy"]:
            hit_auth["fuzzy"] = auth["fuzzy"]

        if len(auth["given"]) > len(hit_auth["given"]):
            hit_auth["given"] = auth["given"]


    def update_pid_maps (self, auth, hit_auth):
        """
        update the PID maps with an author UUID
        """
        uuid = hit_auth["uuid"]

        for pid in self.PID_KEYS:
            if pid in auth:
                if pid not in self.pid_map:
                    self.pid_map[pid] = {}

                if auth[pid] not in self.pid_map[pid]:
                    hit_auth[pid] = auth[pid]
                    self.uuid_map[uuid] = hit_auth

                    map = self.pid_map[pid]
                    map[hit_auth[pid]] = uuid


    def calc_name_sim (self, name_1, name_2):
        """
        a consistent means of measuring distance between two strings
        """
        return SequenceMatcher(None, name_1, name_2).ratio()


    def get_feat_vec (self, source_id, features, hit_features):
        """
        create a feature vector comparing the features of two authors
        """
        i1 = features["init"]
        i2 = hit_features["init"]

        feat_vec = {
            "source": source_id,
            "fuzzy": features["fuzzy"],
            "sur_equal": 1 if (features["sur"] == hit_features["sur"]) else 0,
            "sur_len_diff": len(features["sur"]) - len(hit_features["sur"]),
            "sur_nrm_equal": 1 if (features["sur_nrm"] == hit_features["sur_nrm"]) else 0,
            "sur_dec_equal": 1 if (features["sur_dec"] == hit_features["sur_dec"]) else 0,
            "sur_sim": self.calc_name_sim(features["sur_dec"], hit_features["sur_dec"]),
            "giv_equal": 1 if (features["giv"] == hit_features["giv"]) else 0,
            "giv_len_diff": len(features["giv"]) - len(hit_features["giv"]),
            "giv_nrm_equal": 1 if (features["giv_nrm"] == hit_features["giv_nrm"]) else 0,
            "giv_dec_equal": 1 if (features["giv_dec"] == hit_features["giv_dec"]) else 0,
            "giv_sim": self.calc_name_sim(features["giv_dec"], hit_features["giv_dec"]),
            "init_sub": 1 if ((i1 in i2) or (i2 in i1)) else 0
            }

        return feat_vec


    def name_match (self, feat_vec, features, hit_features, prefix):
        """
        use a feature vector to compare names from two authors
        """
        found = False
        match = 0.0

        if feat_vec[prefix + "_equal"] == 1:
            return True, 1.0
        else:
            if feat_vec[prefix + "_len_diff"] == 0:
                if feat_vec[prefix + "_nrm_equal"]:
                    return True, 1.0
                else:
                    sim = self.calc_name_sim(features[prefix + "_nrm"], hit_features[prefix + "_nrm"])
            else:
                if feat_vec[prefix + "_dec_equal"]:
                    return True, 1.0
                else:
                    sim = self.calc_name_sim(features[prefix + "_dec"], hit_features[prefix + "_dec"])

            found = sim >= self.MIN_NAME_SIMILARITY
            match = sim

        return found, match


    def match_auth (self, key, auth, features, source_id):
        """
        measure matches between the given author and others in their
        bucket
        """
        if key in self.bucket:
            for hit_auth in self.bucket[key]:
                hit_features = self.get_features(hit_auth)
                feat_vec = self.get_feat_vec(source_id, features, hit_features)

                found_surnm, match_surnm = self.name_match(feat_vec, features, hit_features, "sur")
                found_given, match_given = self.name_match(feat_vec, features, hit_features, "giv")

                certainty = min(match_surnm, match_given)

                if found_surnm:
                    if certainty < 1.0:
                        print(
                            "COMPARE",
                            found_surnm, match_surnm, found_given, match_given, 
                            certainty, feat_vec["init_sub"]
                            )

                    if match_given >= self.MIN_NAME_SIMILARITY:
                        yield certainty, hit_auth, feat_vec
                    elif feat_vec["init_sub"] == 1:
                        yield certainty, hit_auth, feat_vec


    def pid_lookup (self, key, auth, features, source_id):
        """
        lookup a previously known author via persistent identifier
        """
        for pid in self.PID_KEYS:
            if pid in auth:
                if pid in self.pid_map:
                    map = self.pid_map[pid]

                    if auth[pid] in map:
                        hit_auth = self.uuid_map[map[auth[pid]]]
                        hit_features = self.get_features(hit_auth)
                        feat_vec = self.get_feat_vec(source_id, features, hit_features)
                        return hit_auth, feat_vec

        # otherwise, no joy
        return None, None


    def add_auth (self, graph, key, auth):
        """
        add a new author to the data structure
        """
        # generate a UUID for the author
        id_list = [ auth["surname"], auth["given"] ]
        uuid = graph.get_hash(id_list, prefix="author-")
        auth["uuid"] = uuid

        # add author to the hashed buckets
        print("ADD", auth)

        if key not in self.bucket:
            self.bucket[key] = [ auth ]
        else:
            self.bucket[key].append(auth)

        # update the PID maps
        self.update_pid_maps(auth, auth)
        return uuid


class RCAuthors:
    PATH_AUTHORS = Path("authors.json")
    PATH_TRAINING = Path("auth_train.tsv")

    def __init__ (self):
        self.known = RCAuthors_Buckets()


    @classmethod
    def normalize_name (cls, n):
        """
        quick transforms that have structural impact on feature
        extraction
        """
        n = n.replace("ö", "oe")
        n = n.replace("’", "'")
        n = n.replace("0'", "o'")
        return n


    def gen_temp_buckets (self):
        """
        generate a temporary data structure to use for reconciling
        within a publication's set of author lists
        """
        return RCAuthors_Buckets()


    def load_entities (self, path=PATH_AUTHORS):
        """
        load the list of author entities
        """
        self.known.read_authors(path)


    def write_entities (self, auth_path=PATH_AUTHORS, train_path=PATH_TRAINING):
        """
        serialize the author entities and training set
        """
        self.known.write_authors(auth_path)
        self.known.write_decisions(train_path)


    def iter_authors (self):
        """
        iterate through all of the known authors
        """
        for key, auths in self.known.bucket.items():
            for auth in auths:
                yield auth


    def parse_auth_list (self, graph, auth_list):
        """
        use self-supervised learning to compare and merge lists of
        authors from the results of multiple APIs
        """
        list_buckets = RCAuthors_Buckets()

        for source, auths in sorted(auth_list.items()):
            print(source)

            for auth in auths:
                print("READ", auth)

                features = list_buckets.get_features(auth)
                key = list_buckets.gen_auth_key(features)
                source_id = list_buckets.get_source_id(source)

                hit_auth, feat_vec = list_buckets.pid_lookup(key, auth, features, source_id)

                if hit_auth:
                    # no-brainer, precise lookup based on persistent identifiers
                    certainty = 1.0
                    list_buckets.update_auth(auth, hit_auth)
                    list_buckets.update_pid_maps(auth, hit_auth)
                    list_buckets.add_decision(True, certainty, hit_auth, auth, feat_vec)

                else:
                    # compare the author to known authors
                    neighbors = []

                    for certainty, hit_auth, feat_vec in list_buckets.match_auth(key, auth, features, source_id):
                        neighbors.append([ certainty, hit_auth, feat_vec ])

                    if len(neighbors) > 0:
                        # select the nearest neighbor
                        certainty, hit_auth, feat_vec = sorted(neighbors, key=lambda x: x[0], reverse=True)[0]
                        list_buckets.update_auth(auth, hit_auth)
                        list_buckets.update_pid_maps(auth, hit_auth)
                        list_buckets.add_decision(True, certainty, hit_auth, auth, feat_vec)
                    else:
                        # this one seems new; add a new author
                        _ = list_buckets.add_auth(graph, key, auth)

        # then merge the best results with known authors
        auth_ids = self.known.merge(graph, list_buckets)
        return auth_ids


    def split_names (self, auth_name):
        """
        split a space-separated author name
        """
        names = auth_name.split(" ")
        bound = 1

        if (len(names) > 1) and names[-2].lower() in ["de", "di", "la", "le", "van", "ver", "von"]:
            bound = 2

        view = {
            "fuzzy": 0.8,
            "surname": " ".join(names[-bound:]),
            "given": " ".join(names[:-bound])
            }

        return view


    def append_view (self, auth_list, view):
        """
        final tests whether to consider an author's metadata valid
        """
        if len(view["given"]) > 0:
            view["surname"] = RCAuthors.normalize_name(view["surname"])
            view["given"] = RCAuthors.normalize_name(view["given"])

            auth_list.append(view)


    def find_authors (self, schol, pub):
        """
        extract author lists from this publication's metadata
        """
        results = {}

        if schol.unpaywall.name in pub:
            meta = pub[schol.unpaywall.name]
            auth_list = []

            if "z_authors" in meta and meta["z_authors"]:
                for auth in meta["z_authors"]:
                    if "family" in auth:
                        view = {
                            "fuzzy": 1.0,
                            "surname": auth["family"],
                            }

                        if "given" in auth:
                            view["given"] = auth["given"]
                        else:
                            view = self.split_names(auth["family"])

                        if "ORCID" in auth:
                            view["orcid"] = auth["ORCID"].split("/")[-1]

                        view["surname"] = view["surname"].strip(" *")
                        self.append_view(auth_list, view)

            if len(auth_list) > 0:
                results[schol.unpaywall.name] = auth_list


        if schol.dimensions.name in pub:
            meta = pub[schol.dimensions.name]
            auth_list = []

            if "authors" in meta and meta["authors"]:
                for auth in meta["authors"]:
                    view = {
                        "fuzzy": 1.0,
                        "surname": auth["last_name"],
                        "given": auth["first_name"]
                        }

                    if "orcid" in auth and len(auth["orcid"]) > 0:
                        view["orcid"] = eval(auth["orcid"])[0]

                    if "researcher_id" in auth and len(auth["researcher_id"]) > 0:
                        view["dimensions"] = auth["researcher_id"]

                    self.append_view(auth_list, view)

            if len(auth_list) > 0:
                results[schol.dimensions.name] = auth_list


        if schol.openaire.name in pub:
            meta = pub[schol.openaire.name]
            auth_list = []

            if "authors" in meta and meta["authors"]:
                for auth in meta["authors"]:
                    if ", " in auth:
                        names = auth.split(", ")
                        view = {
                            "fuzzy": 0.9,
                            "surname": names[0],
                            "given": names[1]
                            }
                        self.append_view(auth_list, view)

                    elif " " in auth:
                        view = self.split_names(auth)
                        self.append_view(auth_list, view)

            if len(auth_list) > 0:
                results[schol.openaire.name] = auth_list


        if schol.semantic.name in pub:
            meta = pub[schol.semantic.name]
            auth_list = []

            if "authors" in meta and meta["authors"]:
                for auth in meta["authors"]:
                    view = self.split_names(auth["name"])

                    if ("authorId" in auth) and auth["authorId"]:
                        # value is sometimes `null`
                        view["semschol"] = auth["authorId"]

                    self.append_view(auth_list, view)

            if len(auth_list) > 0:
                results[schol.semantic.name] = auth_list


        if schol.dissemin.name in pub:
            meta = pub[schol.dissemin.name]["paper"]
            auth_list = []

            if "authors" in meta and meta["authors"]:
                for auth in meta["authors"]:
                    view = {
                        "fuzzy": 1.0,
                        "surname": auth["name"]["last"],
                        "given": auth["name"]["first"]
                        }

                    if "orcid" in auth:
                        view["orcid"] = auth["orcid"]

                    self.append_view(auth_list, view)

            if len(auth_list) > 0:
                results[schol.dissemin.name] = auth_list

        return results


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

    PATH_STOPWORDS = Path("stop.txt")
    DET_SET = set([ "a", "an", "the", "these", "those", "this", "that" ])
    MIN_TOPIC_LEN = 3


    def __init__ (self, step_name="generic"):
        self.step_name = step_name
        self.already_reported = set([])
        self.misses = defaultdict(list)
        self.journals = RCJournals()
        self.publications = RCPublications()
        self.authors = RCAuthors()
        self.stopwords = set([])


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


    def load_stopwords (self):
        """
        load the stopwords to be removed from topic modeling
        """
        self.stopwords = set([])

        with codecs.open(self.PATH_STOPWORDS, "r", encoding="utf8") as f:
            for phrase in f.read().split("\n"):
                text = phrase.lower().strip()

                if len(text) > 0:
                    self.stopwords.add(text)


    def filter_topics (self, text):
        """
        determine whether the given text should be used as a topic
        """
        keep = True

        text = text.lower()
        text = text.replace('"', "").replace("\n", "").strip()
        text = text.replace("ﬁ", "fi").replace("ﬂ", "fl").replace("ﬀ", "ff")

        # remove determinants
        terms = text.split(" ")
        url = urlparse(text)

        if len(terms) > 1 and terms[0] in self.DET_SET:
            text = " ".join(terms[1:])

        # apply rules
        if len(text) < self.MIN_TOPIC_LEN:
            keep = False

        elif not text[0] in string.ascii_letters:
            keep = False

        elif url[0] != "":
            keep = False

        elif text in self.stopwords:
            keep = False

        return text, keep


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


    def update_misses (self, partition, pub):
        """
        keep track of missing/failed metadata
        """
        self.misses[partition].append(pub["title"])


    def report_misses (self, status=None, trouble=None):
        """
        report the titles of publications that have metadata error
        conditions related to the current workflow step
        """
        view = OrderedDict()
        view["messages"] = [ m for m in self.already_reported ]

        if status:
            view["status"] = status

        if trouble:
            view["trouble"] = trouble

        view["misses"] = OrderedDict()

        for partition, title_list in self.misses.items():
            view["misses"][partition] = title_list

        filename = Path("errors/misses_{}.json".format(self.step_name))

        with codecs.open(filename, "wb", encoding="utf8") as f:
            json.dump(view, f, indent=4, sort_keys=False, ensure_ascii=False)


######################################################################
## main entry point (not used)

if __name__ == "__main__":
    g = RCGraph()
    print(g)
