#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
import json
import pprint
import sys
import traceback


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
        print("BAD DOI: |{}|".format(doi))
        print(partition)
        print(source)
        print(type(doi))
        print(pub["title"])
        print(doi)
        return False


def gather_doi (pub, pub_list, partition, graph):
    """
    use `title_search()` across scholarly infrastructure APIs to
    identify this publication's DOI, etc.
    """
    title = pub["title"]
    title_match = False
    doi_list = []

    if "doi" in pub["original"]:
        doi = pub["original"]["doi"]

        if verify_doi(doi, partition, pub, "original"):
            doi_list.append(doi)

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
                doi = meta["doi"]

                if verify_doi(doi, partition, pub, api.name):
                    doi_list.append(doi)

            pub[api.name] = meta

    # keep track of the titles that fail all API lookups
    if not title_match:
        graph.misses.append(title)

    # select the most frequently reported DOI -- to avoid secondary
    # DOIs, such as SSRN, dominating the metadata
    if len(doi_list) > 0:
        tally = graph.tally_list(doi_list)
        doi, count = tally[0]
        pub["doi"] = doi

    # send this publication along into the workflow stream
    pub_list.append(pub)


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("step2")

    # for each publication: enrich metadata, gather the DOIs, etc.
    for partition, pub_iter in graph.iter_publications(path="publications/partitions"):
        print("working: {}".format(partition))
        pub_list = []

        for pub in pub_iter:
            gather_doi(pub, pub_list, partition, graph)

            if "doi" in pub:
                graph.publications.doi_hits += 1

        graph.write_partition("step2/", partition, pub_list)

    # report titles for publications that failed every API lookup
    graph.report_misses()
    print("DOIs: {}".format(graph.publications.doi_hits))
