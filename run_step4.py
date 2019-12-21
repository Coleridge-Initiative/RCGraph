#!/usr/bin/env python
# encoding: utf-8

from bs4 import BeautifulSoup
from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import json
import pprint
import requests
import sys
import traceback
import xmltodict

ALREADY_REPORTED = set([])


def find_issn (pub, graph):
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
        issn_tally = rc_graph.RCGraph.tally_list(issn_list)
        freq_issn, count = issn_tally[0]
        return 1, freq_issn, issn_tally
    else:
        return 0, None, None


def ncbi_lookup_issn (pub, journal, issn):
    """
    use the NCBI discovery service for ISSN lookup
    """
    global ALREADY_REPORTED

    try:
        url = "https://www.ncbi.nlm.nih.gov/nlmcatalog/?report=xml&format=text&term={}".format(issn)
        response = requests.get(url).text

        soup = BeautifulSoup(response,  "html.parser")
        xml = soup.find("pre").text.strip()

        if len(xml) > 0:
            ## use an XML hack to workaround common formatting errors
            ## at NCBI
            xml = "<fix>{}</fix>".format(xml)
            j = json.loads(json.dumps(xmltodict.parse(xml)))

            if "NCBICatalogRecord" in j["fix"]:
                ncbi = j["fix"]["NCBICatalogRecord"]

                if isinstance(ncbi, list):
                    if "JrXml" in ncbi[0]:
                        # ibid., XML hack
                        ncbi = ncbi[0]
                    elif len(ncbi) > 1 and "JrXml" in ncbi[1]:
                        ncbi = ncbi[1]
                    else:
                        status = "NCBI bad XML format - no JrXML: {}".format(issn)

                        if status not in ALREADY_REPORTED:
                            ALREADY_REPORTED.add(status)
                            print(status)

                        return

                meta = ncbi["JrXml"]["Serial"]
                #pprint.pprint(meta)
                journal["NCBI"] = meta
    except:
        print(traceback.format_exc())
        print("NCBI failed lookup: {} {}".format(issn, pub["title"]))


def gather_issn (journal):
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

            journal["issn"] = make_list(old_issn, new_issn, issn_link)

        ## add the URL
        if "IndexingSelectedURL" in meta:
            journal["url"] = meta["IndexingSelectedURL"]

        ## arrange the title list
        journal["titles"] = make_list(
            journal["titles"],
            [ meta["Title"], meta["MedlineTA"] ],
            meta["ISOAbbreviation"]
            )


def make_list (old_list, new_list, lead_elem):
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


if __name__ == "__main__":
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph(step_name="step4")
    graph.journals.load_entities()

    # for each publication: reconcile journal names
    proposed_journals = []
    issn_hits = 0
    disputed = {}

    for partition, pub_iter in graph.iter_publications(path="step3"):
        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            journal_list = graph.journals.extract_journals(pub)
            count, freq_issn, issn_tally = find_issn(pub, graph)
            issn_hits += count

            if len(journal_list) > 0:
                journal_tally = rc_graph.RCGraph.tally_list(journal_list, ignores=graph.journals.IGNORE_JOURNALS)
                proposed_journals.append(journal_tally)
            else:
                graph.misses.append(pub["title"])

            # attempt a lookup from the known journal entities
            journal = graph.journals.select_best_entity(journal_list)

            if freq_issn:
                new_issns = [ i for i, c in issn_tally ]
                new_set = set(new_issns)

                if "issn" in journal:
                    old_set = set(journal["issn"])

                    if len(old_set.intersection(new_set)) < 1:
                        # there's a dispute: check for overlapping
                        # journal definitions?
                        if (len(issn_tally) == 1) and (issn_tally[0][0] == "1556-5068"):
                            # ignore the singleton cases of SSRN journal attributes
                            pass
                        elif (len(journal["issn"]) == 1) and (journal["issn"][0] == "0000-0000"):
                            # ignore adding to the "unknown" caseb
                            pass
                        else:
                            disputed["{} {}".format(issn_tally, journal["issn"])] = journal
                    else:
                        # add other ISSNs to an existing entry
                        for issn in new_issns:
                            if issn not in journal["issn"]:
                                if "-" in issn:
                                    journal["issn"].append(issn)
                else:
                    journal["issn"] = []

                    for issn in new_issns:
                        if "-" in issn:
                            journal["issn"].append(issn)

            if not "NCBI" in journal:
                # DO NOT RUN IF JOUNAL ALREADY HAS AN "NCBI" ENTRY
                ncbi_lookup_issn(pub, journal, new_issns[0])
                gather_issn(journal)

    # report results
    print("ISSNs found for {} publications".format(issn_hits))
                               
    for key, journal in disputed.items():
        print("DISPUTE", key)
        print(journal)
        print("---")

    # show a tentative list of journals, considered for adding
    for tally in proposed_journals:
        new_entity = graph.journals.add_entity(tally)

        if new_entity:
            print("{},".format(json.dumps(new_entity, indent=2, sort_keys=True)))

    # suggest an updated journal list
    j_dict = {}

    for j in graph.journals.known.values():
        j_dict[j["id"]] = j

    with open("update_journals.json", "w") as f:
        j_list = list(j_dict.values())
        json.dump(j_list, f, indent=4, sort_keys=True)

    # report titles for publications that don't have a journal
    graph.report_misses()

