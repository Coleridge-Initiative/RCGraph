#!/usr/bin/env python
# encoding: utf-8

import glob
import json
import operator
import os
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


def tally_list (l, ignores=set([])):
    """
    sort a list in descending order of most frequent element
    """
    trans = dict([ (x.strip().lower(), x) for x in l])
    lower_l = list(map(lambda x: x.strip().lower(), l))
    keys = set(lower_l)
    enum_dict = {}

    for key in keys:
        if key in ignores:
            count = 0
        else:
            count = lower_l.count(key)

        enum_dict[trans[key]] = count

    return sorted(enum_dict.items(), key=operator.itemgetter(1), reverse=True)
