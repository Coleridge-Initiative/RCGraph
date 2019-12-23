#!/usr/bin/env python
# encoding: utf-8

from bs4 import BeautifulSoup
from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import json
import requests
import sys
import traceback
import xmltodict

DEFAULT_PARTITION = None


def ncbi_lookup_issn (issn):
    """
    use the NCBI discovery service for ISSN lookup
    """
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
                        # bad XML returned from the API call
                        return None, f"NCBI bad XML format: no JrXML element for ISSN {issn}"

                meta = ncbi["JrXml"]["Serial"]
                #pprint.pprint(meta)
                return meta, None

    except:
        print(traceback.format_exc())
        print(f"NCBI failed lookup: {issn}")

    return None, None


def reconcile_journal (graph, pub, disputed):
    """
    reconcile the journal entity and ISSN for the given publication
    """
    journal_list = graph.journals.extract_journals(pub)
    count, freq_issn, issn_tally = graph.journals.extract_issn(pub)
    graph.journals.issn_hits += count

    # attempt a match among the known journal entities
    message = None

    if freq_issn:
        journal = graph.journals.select_best_entity(journal_list)
        best_issn = graph.journals.add_issns(journal, issn_tally, disputed)

        if not "NCBI" in journal:
            # DO NOT RUN IF JOUNAL ALREADY HAS AN "NCBI" ENTRY
            meta, message = ncbi_lookup_issn(best_issn)

            if meta:
                # add the NCBI metadata into this journal
                journal["NCBI"] = meta
                graph.journals.gather_issn(journal)

    return journal_list, message


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph(step_name="step4")

    # for each publication: reconcile journal names
    graph.journals.load_entities()
    proposed = []
    disputed = {}

    for partition, pub_iter in graph.iter_publications(path="step3", filter=args.partition):
        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            journal_list, message = reconcile_journal(graph, pub, disputed)

            if len(journal_list) > 0:
                journal_tally = graph.tally_list(journal_list, ignores=graph.journals.IGNORE_JOURNALS)
                proposed.append(journal_tally)
            else:
                graph.misses.append(pub["title"])

            if message:
                graph.report_error(message)

    # report results
    for key, journal in disputed.items():
        print("DISPUTE", key)
        print(journal)
        print("---")

    # show a tentative list of journals, considered for adding
    for tally in proposed:
        new_entity = graph.journals.add_entity(tally)

        if new_entity:
            print("{},".format(json.dumps(new_entity, indent=2, sort_keys=True)))

    # suggest updates to the journal entities and report titles
    # for publications that don't have a journal
    graph.journals.suggest_updates()

    status = f"{graph.journals.issn_hits} publications had ISSNs found for their journals"
    graph.report_misses(status)


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="reconcile the journal and ISSN for each publication"
        )

    parser.add_argument(
        "--partition",
        type=str,
        default=DEFAULT_PARTITION,
        help="limit processing to a specified partition"
        )

    main(parser.parse_args())
