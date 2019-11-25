#!/usr/bin/env python
# encoding: utf-8

from urllib.parse import urlparse
import json
import os
import sys
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
        self.datasets = {}
        self.publications = []
        self.partition_map = {}

        subdir = "datasets/datasets.json"

        with open(subdir, "r") as f:
            for d in json.load(f):
                self.datasets[d["id"]] = d

        subdir = "publications/partitions"
        partitions = [ "/".join([subdir, name]) for name in os.listdir(subdir) ]

        for partition in partitions:
            with open(partition, "r") as f:
                pub_list = json.load(f)
                self.publications.extend(pub_list)

                for pub in pub_list:
                    self.partition_map[pub["title"]] = partition


    def test_resources_loaded (self):
        print("\n{} datasets loaded".format(len(self.datasets)))
        self.assertTrue(len(self.datasets) > 0)

        print("\n{} publications loaded".format(len(self.publications)))
        self.assertTrue(len(self.publications) > 0)


    def test_publication_dataset_links (self):
        for pub in self.publications:
            for d in pub["datasets"]:
                if d not in self.datasets.keys():
                    print("dataset |{}| not found: {}".format(d, pub))
                    print("from partition {}".format(self.partition_map[pub["title"]]))

                self.assertTrue(d in self.datasets.keys())


if __name__ == "__main__":
    unittest.main()
