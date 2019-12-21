#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from richcontext import graph as rc_graph
from urllib.parse import urlparse
import json
import os
import sys
import traceback
import unittest


def url_validator (url):
    """validate the format of a URL"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc, result.path])
    except:
        return False


class TestRCGraph (unittest.TestCase):
    def allow_arg (self):
        return None
    

    def setUp (self):
        """load the resources from submodules"""
        self.partition_map = {}
        self.publications = []
        self.datasets = {}
        self.journals = {}

        # load the publications
        subdir = rc_graph.RCGraph.PATH_PUBLICATIONS
        partitions = [ subdir / name for name in os.listdir(subdir) ]

        for partition in partitions:
            with open(partition, "r") as f:
                try:
                    pub_list = json.load(f)
                except Exception:
                    traceback.print_exc()
                    print(partition)
                    self.fail("publication partition could not be read")

                self.publications.extend(pub_list)

                for pub in pub_list:
                    self.partition_map[pub["title"]] = partition

        # load the datasets
        with open(rc_graph.RCGraph.PATH_DATASETS, "r") as f:
            for d in json.load(f):
                self.datasets[d["id"]] = d

        # load the journals
        with open(rc_graph.RCGraph.PATH_JOURNALS, "r") as f:
            for j in json.load(f):
                self.journals[j["id"]] = j


    def test_resources_loaded (self):
        print("\n{} publications loaded".format(len(self.publications)))
        self.assertTrue(len(self.publications) > 0)

        print("\n{} datasets loaded".format(len(self.datasets)))
        self.assertTrue(len(self.datasets) > 0)

        print("\n{} journals loaded".format(len(self.journals)))
        self.assertTrue(len(self.journals) > 0)


    def test_publication_dataset_links (self):
        for pub in self.publications:
            for d in pub["datasets"]:
                if d not in self.datasets.keys():
                    print("dataset `{}` not found".format(d))
                    print("```\n{}\n```".format(pub))
                    print("from partition `{}`\n".format(self.partition_map[pub["title"]]))

        for pub in self.publications:
            for d in pub["datasets"]:
                self.assertTrue(d in self.datasets.keys())


if __name__ == "__main__":
    unittest.main()
