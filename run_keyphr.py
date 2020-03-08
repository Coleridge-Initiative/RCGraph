#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from pathlib import Path
from richcontext import graph as rc_graph
from tqdm import tqdm
import argparse
import codecs
import json
import pytextrank
import spacy


DEFAULT_PARTITION = None


def extract_phrases (graph, nlp, partition, pub, pub_list, limit_keyphrase=15):
    """
    for each publication, find the abstract and extract key phrases
    """
    success = False
    phrases = {}

    if ("abstract" in pub) and pub["abstract"]:
        abstract = pub["abstract"]
        doc = nlp(abstract)  # abstract text
                
        for phrase in doc._.phrases[:limit_keyphrase]:
            phrase_text = phrase.text.lower()
            phrases[phrase_text] = {"count": phrase.count, "rank_score": phrase.rank}
                
        if len(phrases) > 0:
            pub["keyphrases"] = phrases
            pub_list.append(pub)
            graph.publications.key_hits += 1
            success = True

    if not success:
        graph.update_misses(partition, pub)


def main (args):
    """
    For each publication, search for the abstract and extract key phrases 
    if abstract exists and is not null. Report if the abstract is missing.
    """
    graph = rc_graph.RCGraph("keyphr")
    
    # add PyTextRank into the spaCy pipeline
    nlp = spacy.load("en_core_web_sm")
    tr = pytextrank.TextRank(logger=None)
    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)

    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        pub_list = []
        
        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            extract_phrases(graph, nlp, partition, pub, pub_list)

        graph.write_partition(graph.BUCKET_STAGE, partition, pub_list)
            
    # report errors
    status = "{} publications parsed keyphrases from abstracts".format(graph.publications.key_hits)
    trouble = "publications which could not parse keyphrases"
    graph.report_misses(status, trouble)


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
