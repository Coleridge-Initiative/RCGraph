#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from tqdm import tqdm
import argparse


DEFAULT_PARTITION = None


def lookup_abstract (schol, graph, partition, pub):
    """
    move the abstract into the top level metadata for that publication 
    if the abstract is found in sematic scholar
    """
    source = schol.semantic.name
    abstract_match = False

    if (source in pub) and ("abstract" in pub[source]):
        # avoid redundant opreations if the abstract is already in place
        if "abstract" not in pub:
            meta = pub[source]
            abstract = meta["abstract"]
            pub["abstract"] = abstract

        abstract_match = True

    return abstract_match


def main (args):
    # initialize the federated API access
    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)
    graph = rc_graph.RCGraph("abstract")

    # for each publication: enrich metadata, gather the abstracts
    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        pub_list = []

        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            pub_list.append(pub)
            abstract_match = lookup_abstract(schol, graph, partition, pub)

            if abstract_match:
                graph.publications.ab_hits += 1
            else:
                graph.misses.append(pub["title"])

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)

    # report errors
    status = "{} successful abstract lookups".format(graph.publications.ab_hits)
    graph.report_misses(status, "publications that failed every abstract lookup")


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="specific publication lookup with abstracts."
    )

    parser.add_argument(
        "--partition",
        type=str,
        default=DEFAULT_PARTITION,
        help="limit processing to a specified partition"
    )
    
    main(parser.parse_args())
