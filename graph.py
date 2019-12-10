#!/usr/bin/env python
# encoding: utf-8

import glob
import json
import operator
import os
import traceback


class RCGraph:
    """
    methods for managing the Rich Context knowledge grapgh
    """
    IGNORE_JOURNALS = set([
            "ssrn electronic journal"
            ])


    def __init__ (self, step_name="generic"):
        self.step_name = step_name
        self.misses = []


    def iter_publications (self, path, filter=None):
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


    def tally_list (self, l, ignores=set([])):
        """
        sort a list in descending order of most frequent element
        """
        trans = dict([ (x.strip().lower(), x) for x in l])
        lower_l = list(map(lambda x: x.strip().lower(), l))
        keys = set(lower_l)
        enum_dict = {}

        for key in keys:
            enum_dict[trans[key]] = (0 if key in ignores else lower_l.count(key))

        return sorted(enum_dict.items(), key=operator.itemgetter(1), reverse=True)


    def report_misses (self):
        """
        report the titles of publications that have metadata error
        conditions related to the current workflow step
        """
        filename = "misses_{}.txt".format(self.step_name)

        with open(filename, "w") as f:
            for title in self.misses:
                f.write("{}\n".format(title))


######################################################################
## main entry point (not used)

if __name__ == "__main__":
    g = RCGraph()
    print(g)
