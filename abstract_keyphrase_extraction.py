#!/usr/bin/env python
# encoding: utf-8

from richcontext import graph as rc_graph
from tqdm import tqdm
import argparse
from pathlib import Path
import json
import pytextrank
import spacy
import codecs


DEFAULT_PARTITION = None


def main (args, base_path="."):
    '''
    For each publication, search for the abstract and extract key phrases 
    if abstract exists and is not null. Report if the abstract is missing.
    '''
    graph = rc_graph.RCGraph("abstract")
    resource_path = Path(base_path) / "abstract_keyphrases"
    
    # for each publication: search the abstract and extract the key 
    missing_list = {}
    for partition, pub_iter in graph.iter_publications(graph.BUCKET_STAGE, filter=args.partition):
        
        output_path = resource_path / partition
        results = []
        temp_list = []
        
        for pub in tqdm(pub_iter, ascii=True, desc=partition[:30]):
            
            if ("abstract" in pub) and pub["abstract"]:
                abstract = pub["abstract"]
                doc = nlp(abstract)  # abstract text

                phrases = {}
                final = {}
                limit_keyphrase = 15
                
                for phrase in doc._.phrases[:limit_keyphrase]:
                    phrases[phrase.text] = {"count": phrase.count, "rank_score": phrase.rank}
                
                final["pub_title"] = pub["title"] # partition title
                final["abstract_text_rank"] = phrases
                results.append(final)
                
            else:  
                temp_list.append(pub["title"])
        
        missing_list[partition] = temp_list
        
        with codecs.open(output_path, "wb", encoding="utf8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
            
            
    # report publications without abstract
    report_path = 'report_missing_abstracts.json'
    with codecs.open(report_path, "wb", encoding="utf8") as f:
        json.dump(missing_list, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    # set up and add PyTextRank into the spaCy pipeline
    nlp = spacy.load("en_core_web_sm")
    tr = pytextrank.TextRank(logger=None)
    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)
    
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