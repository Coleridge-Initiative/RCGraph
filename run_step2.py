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
    enum_dict = {}
    keys = set(l)

    for key in keys:
        enum_dict[key] = l.count(key)

    return sorted(enum_dict.items(), key=operator.itemgetter(1), reverse=True)


def gather_doi (pub, pub_list, misses):
    """
    use `title_search()` across scholarly infrastructure APIs to
    identify this publication's DOI, etc.
    """
    title = pub["title"]
    title_match = False
    doi_list = []

    if "doi" in pub["original"]:
        doi_list.append(pub["original"]["doi"])

    for api in [schol.openaire, schol.europepmc, schol.dimensions]:
        try:
            meta = api.title_search(title)
        except Exception:
            # debugging an edge case
            traceback.print_exc()
            print(title)
            print(api.name)
            pprint.pprint(pub)
            continue

        if meta and len(meta) > 0:
            title_match = True
            meta = dict(meta)
            #pprint.pprint(meta)

            if "doi" in meta:
                doi_list.append(meta["doi"])

            pub[api.name] = meta

    # keep track of the titles that fail all API lookups
    if not title_match:
        misses.append(title)

    # select the most frequently reported DOI -- to avoid secondary
    # DOIs, such as SSRN, dominating the metadata
    if len(doi_list) > 0:
        doi, count = tally_list(doi_list)[0]
        pub["doi"] = doi

    # send this publication along into the workflow stream
    pub_list.append(pub)


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    # for each publication: enrich metadata, gather the DOIs, etc.
    misses = []

    for partition, pub_iter in iter_publications(path="publications/partitions"):
        print("working: {}".format(partition))
        pub_list = []

        for pub in pub_iter:
            gather_doi(pub, pub_list, misses)

        with open("step2/" + partition, "w") as f:
            json.dump(pub_list, f, indent=4, sort_keys=True)

    # report titles for publications that failed every API lookup
    with open("misses.txt", "w") as f:
        for title in misses:
            f.write("{}\n".format(title))
