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


def verify_doi (doi, partition, pub, source):
    try:
        if not doi:
            # a `None` value is valid (== DOI is unknown)
            return False
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
            return True
    except:
        # bad metadata
        if False:
            print("BAD DOI: |{}|".format(doi))
            print(partition)
            print(source)
            print(type(doi))
            print(pub["title"])
            print(doi)

        return False


def gather_pdf (pub, partition, pub_list, misses):
    """
    use `publication_lookup()` across scholarly infrastructure APIs to
    identify this publication's open access PDFs, etc.
    """
    doi_list = []
    doi_match = False

    for source in ["original", "Dimensions", "EuropePMC", "OpenAIRE"]:
        if source in pub and "doi" in pub[source]:
            doi = pub[source]["doi"]

            if verify_doi(doi, partition, pub, source):
                doi_list.append(doi)
                doi_match = True

    if len(doi_list) > 0:
        tally = tally_list(doi_list)
        #print(tally)

        doi, count = tally[0]
        pub["doi"] = doi
    else:
        # there's no point in moving forward on this publication
        # without a DOI to use for the API lookups downstream
        return

    title = pub["title"]

    for api in [schol.semantic, schol.unpaywall, schol.dissemin]:
        try:
            meta = api.publication_lookup(doi)
        except Exception:
            # debugging an edge case
            traceback.print_exc()
            print(title)
            print(doi)
            print(api.name)
            #pprint.pprint(pub)
            continue

        if meta and len(meta) > 0:
            doi_match = True
            meta = dict(meta)
            #pprint.pprint(meta)

            pub[api.name] = meta

    # keep track of the titles that fail all API lookups
    if not doi_match:
        misses.append(title)

    # send this publication along into the workflow stream
    pub_list.append(pub)


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    # for each publication: enrich metadata, gather the DOIs, etc.
    misses = []
    doi_hits = 0
    pdf_hits = 0

    for partition, pub_iter in iter_publications(path="step2"):
        print("working: {}".format(partition))
        pub_list = []

        for pub in pub_iter:
            gather_pdf(pub, partition, pub_list, misses)

        for pub in pub_list:
            if "doi" in pub:
                doi_hits += 1

            if "pdf" in pub and "url" in pub:
                pdf_hits += 1

        with open("step3/" + partition, "w") as f:
            json.dump(pub_list, f, indent=4, sort_keys=True)

    print("DOIs: {} / PDFs: {}".format(doi_hits, pdf_hits))

    # report titles for publications that failed every API lookup
    with open("misses_step3.txt", "w") as f:
        for title in misses:
            f.write("{}\n".format(title))
