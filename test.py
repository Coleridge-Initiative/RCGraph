#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from richcontext import graph as rc_graph
import codecs
import json
import sys
import traceback
import unittest


class TestRCGraph (unittest.TestCase):
    def allow_arg (self):
        return None
    

    def setUp (self):
        """load the resources from submodules"""
        self.partition_map = {}
        self.publications = []
        self.datasets = {}
        self.providers = {}
        self.journals = {}
        self.authors = {}

        graph = rc_graph.RCGraph("test")

        # load the publications
        for partition in rc_graph.RCGraph.PATH_PUBLICATIONS.iterdir():
            with codecs.open(partition, "r", encoding="utf8") as f:
                try:
                    pub_list = json.load(f)
                except Exception:
                    traceback.print_exc()
                    print(partition)
                    self.fail("publication partition could not be read")

                self.publications.extend(pub_list)

                for pub in pub_list:
                    self.partition_map[pub["title"].lower()] = partition

        # load the datasets
        with codecs.open(rc_graph.RCGraph.PATH_DATASETS, "r", encoding="utf8") as f:
            for d in json.load(f):
                self.datasets[d["id"]] = d

        # load the data providers
        with codecs.open(rc_graph.RCGraph.PATH_PROVIDERS, "r", encoding="utf8") as f:
            for p in json.load(f):
                self.providers[p["id"]] = d

        # load the journals
        with codecs.open(rc_graph.RCJournals.PATH_JOURNALS, "r", encoding="utf8") as f:
            for j in json.load(f):
                self.journals[j["id"]] = j

        # load the authors
        graph.authors.load_entities()

        for a in graph.authors.iter_authors():
            self.authors[a["uuid"]] = a


    def test_resources_loaded (self):
        print("\n{} publications loaded".format(len(self.publications)))
        self.assertTrue(len(self.publications) > 0)
        print("{} unique titles".format(len(self.partition_map)))

        print("\n{} datasets loaded".format(len(self.datasets)))
        self.assertTrue(len(self.datasets) > 0)

        print("\n{} providers loaded".format(len(self.providers)))
        self.assertTrue(len(self.providers) > 0)

        print("\n{} journals loaded".format(len(self.journals)))
        self.assertTrue(len(self.journals) > 0)

        print("\n{} authors loaded".format(len(self.authors)))
        self.assertTrue(len(self.authors) > 0)


    def test_publication_dataset_links (self):
        for pub in self.publications:
            for d in pub["datasets"]:
                if d not in self.datasets.keys():
                    print("dataset `{}` not found".format(d))
                    print("```\n{}\n```".format(pub))
                    print("from partition `{}`\n".format(self.partition_map[pub["title"].lower()]))

        for pub in self.publications:
            for d in pub["datasets"]:
                self.assertTrue(d in self.datasets.keys())


if __name__ == "__main__":
    unittest.main()
