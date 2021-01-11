#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import json
import sys

DEFAULT_PARTITION = None


def reconcile_journal (schol, graph, pub, disputed):
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
        best_issn = graph.journals.add_issns(pub, journal, issn_tally, disputed)

        if best_issn and not "NCBI" in journal:
            # DO NOT RUN IF JOUNAL ALREADY HAS AN "NCBI" ENTRY
            #meta, message = ncbi_lookup_issn(best_issn)
            api = schol.pubmed

            if api.has_credentials():
                response = api.journal_lookup(best_issn)
                message = response.message

                if response.meta:
                    # add the NCBI metadata into this journal
                    journal["NCBI"] = response.meta
                    graph.journals.gather_issn(journal)

    return journal_list, message, freq_issn


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph(step_name="step4")

    # for each publication: reconcile journal names
    graph.journals.load_entities()
    proposed = {}
    disputed = {}

    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            journal_list, message, freq_issn = reconcile_journal(schol, graph, pub, disputed)
            if len(journal_list) > 0:
                journal_tally = graph.tally_list(journal_list, ignores=graph.journals.IGNORE_JOURNALS)
                proposed[freq_issn] = journal_tally
            else:
                graph.update_misses(partition, pub)

            if message:
                graph.report_error(message)

    # report results
    for key, journal in disputed.items():
        print("DISPUTE", key)
        print(journal)
        print("---")

    # show a tentative list of journals, considered for adding
    for freq_issn, tally in proposed.items():
        new_entity = graph.journals.add_entity(tally, freq_issn)
        if new_entity:
            print("{},".format(json.dumps(new_entity, indent=2, sort_keys=True)))

    # suggest updates to the journal entities and report titles
    # for publications that don't have a journal
    graph.journals.suggest_updates()

    # report errors
    status = f"{graph.journals.issn_hits} publications had ISSNs found for their journals"
    graph.report_misses(status, "publications that had no ISSN metadata")


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
