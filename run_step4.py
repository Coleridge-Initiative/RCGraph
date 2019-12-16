#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
import json
import sys


if __name__ == "__main__":
    graph = rc_graph.RCGraph(step_name="step4")
    graph.journals.load_entities()

    # for each publication: reconcile journal names
    journals = []

    for partition, pub_iter in graph.iter_publications(path="step3"):
        for pub in pub_iter:
            journal_list = graph.journals.extract_journals(pub)

            if len(journal_list) > 0:
                tally = rc_graph.RCGraph.tally_list(journal_list, ignores=graph.journals.IGNORE_JOURNALS)
                journals.append(tally)
            else:
                graph.misses.append(pub["title"])

            # attempt a lookup from the known journal entities
            journal = graph.journals.select_best_entity(journal_list)

    # show a tentative list of journals, considered for adding
    for tally in journals:
        new_entity = graph.journals.add_entity(tally)

        if new_entity:
            print("{},".format(json.dumps(new_entity, indent=2, sort_keys=True)))

    # report titles for publications that don't have a journal
    graph.report_misses()
