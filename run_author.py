#!/usr/bin/env python
# encoding: utf-8

from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import codecs
import csv
import json
import pprint
import sys
import traceback
import unicodedata
import unidecode

DEFAULT_PARTITION = None


class RCAuthor_Buckets:
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
        self.decision_trace.extend(other_buckets.decision_trace)

        for key, auths in other_buckets.bucket.items():
            if key not in self.bucket:
                # recreate the bucket wholesale
                self.bucket[key] = auths

                for auth in auths:
                    self.update_pid_maps(auth, auth)
                    uuid = auth["uuid"]

                    if uuid not in self.uuid_map:
                        self.uuid_map[uuid] = auth
            else:
                for auth in auths:
                    features = self.get_features(auth)
                    hit_auth, feat_vec = self.pid_lookup(key, auth, features, 0)

                    if hit_auth:
                        # direct match using PIDs
                        self.update_pid_maps(auth, hit_auth)
                    else:
                        # compare the author to known authors
                        neighbors = []

                        for certainty, hit_auth, feat_vec in self.match_auth(key, auth, features, 0):
                            neighbors.append([ certainty, hit_auth, feat_vec ])

                        if len(neighbors) > 0:
                            # select the nearest neighbor
                            certainty, hit_auth, feat_vec = sorted(neighbors, key=lambda x: x[0], reverse=True)[0]
                            self.update_pid_maps(auth, hit_auth)
                            self.add_decision(True, certainty, hit_auth, auth, feat_vec)
                        else:
                            # this one seems new; add a new author
                            uuid = self.add_auth(graph, key, auth)

                            if uuid not in self.uuid_map:
                                self.uuid_map[uuid] = auth


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
            "giv_equal", "giv_len_diff", "giv_nrm_equal", "giv_dec_equal", "giv_sim"
            ]

        with open(path, "w") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(headers)

            for decision in self.decision_trace:
                row = [x.encode("utf-8") if isinstance(x, str) else x for x in decision]
                writer.writerow(row)


    def normalize_name (self, n):
        """
        quick transforms that have structural impact on feature
        extraction
        """
        n = n.replace("ö", "oe")
        n = n.replace("’", "'")
        n = n.replace("0'", "o'")
        return n


    def get_features (self, auth):
        """
        prepare features for one author
        """
        features = {}
        #print(Counter(n1))

        features["fuzzy"] = auth["fuzzy"]

        features["sur"] = auth["surname"].lower().strip()
        features["giv"] = auth["given"].lower().strip()

        features["sur_nrm"] = self.normalize_name(features["sur"])
        features["giv_nrm"] = self.normalize_name(features["giv"]).strip(".,")

        features["sur_dec"] = unidecode.unidecode(features["sur"])
        features["giv_dec"] = unidecode.unidecode(features["giv"])

        return features


    def gen_auth_key (self, features):
        """
        generate a hash key for an author, assuming they have at least
        one character in both their surname and given name
        """
        return unidecode.unidecode(features["sur_dec"][0] + features["giv_dec"][0]).lower()


    def get_source_id (self, source):
        """
        translate from source name to a numeric representation (level)
        """
        if source not in self.SOURCE_MAP:
            source_id = 0
        else:
            source_id = self.SOURCE_MAP[source]

        return source_id


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
            "giv_sim": self.calc_name_sim(features["giv_dec"], hit_features["giv_dec"])
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

                certainty = match_surnm * match_given

                if found_surnm:
                    if certainty < 1.0:
                        print("COMPARE", found_surnm, match_surnm, found_given, match_given, certainty)

                    if match_given >= self.MIN_NAME_SIMILARITY:
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
        pass


def parse_auth_list (graph, buckets, auth_list):
    """
    self-supervised learning to compare lists of authors from the
    results of multiple APIs
    """
    auth_ids = []

    # first, use self-supervised learning to compare and merge results
    # from multiple APIs
    list_buckets = RCAuthor_Buckets()

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
                uuid = hit_auth["uuid"]
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
                    uuid = hit_auth["uuid"]
                    list_buckets.update_pid_maps(auth, hit_auth)
                    buckets.add_decision(True, certainty, hit_auth, auth, feat_vec)
                else:
                    # this one seems new; add a new author
                    uuid = list_buckets.add_auth(graph, key, auth)

            # update the ordered list of authors for this publication
            if uuid not in auth_ids:
                auth_ids.append(uuid)

    # then merge the best results with known authors
    buckets.merge(graph, list_buckets)
    return auth_ids


def split_names (auth_name):
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


def find_authors (schol, pub):
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
                        view = split_names(auth["family"])

                    if "ORCID" in auth:
                        view["orcid"] = auth["ORCID"].split("/")[-1]

                    view["surname"] = view["surname"].strip(" *")

                    if len(view["given"]) > 0:
                        auth_list.append(view)

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

                if len(view["given"]) > 0:
                    auth_list.append(view)

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

                    if len(view["given"]) > 0:
                        auth_list.append(view)

                elif " " in auth:
                    view = split_names(auth)

                    if len(view["given"]) > 0:
                        auth_list.append(view)

        if len(auth_list) > 0:
            results[schol.openaire.name] = auth_list


    if schol.semantic.name in pub:
        meta = pub[schol.semantic.name]
        auth_list = []

        if "authors" in meta and meta["authors"]:
            for auth in meta["authors"]:
                view = split_names(auth["name"])

                if ("authorId" in auth) and auth["authorId"]:
                    # value is sometimes `null`
                    view["semschol"] = auth["authorId"]

                if len(view["given"]) > 0:
                    auth_list.append(view)

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

                if len(view["given"]) > 0:
                    auth_list.append(view)

        if len(auth_list) > 0:
            results[schol.dissemin.name] = auth_list

    return results


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("author")

    buckets = RCAuthor_Buckets()
    #buckets.read_authors(RCAuthors.PATH_AUTHORS)

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        print("PARTITION", partition)
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub["title"] = unicodedata.normalize("NFKD", pub["title"]).strip()
            print("TITLE", pub["title"])

            auth_list = find_authors(schol, pub)

            if len(auth_list) > 0:
                print(json.dumps(auth_list))
                auth_ids = parse_auth_list(graph, buckets, auth_list)
                print(auth_ids)
            else:
                ## error: pub has no authors?
                pass

    ## rewrite the author file
    buckets.write_authors(RCAuthors.PATH_AUTHORS)
    buckets.write_decisions(RCAuthors.PATH_TRAINING)


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
