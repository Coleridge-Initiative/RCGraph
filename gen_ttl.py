#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from rdflib.serializer import Serializer
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import json
import pprint
import rdflib
import sys

CORPUS_TTL_FILENAME = "tmp.ttl"
CORPUS_JSONLD_FILENAME = "tmp.jsonld"


PREAMBLE = """
@base <https://github.com/Coleridge-Initiative/adrf-onto/wiki/Vocabulary> .

@prefix cito:	<http://purl.org/spar/cito/> .
@prefix dct:	<http://purl.org/dc/terms/> .
@prefix foaf:	<http://xmlns.com/foaf/0.1/> .
@prefix rdf:	<http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:	<http://www.w3.org/2001/XMLSchema#> .
"""

TEMPLATE_JOURNAL = """
:{}
  rdf:type :Journal ;
  dct:title "{}" ;
"""

TEMPLATE_DATASET = """
:{}
  rdf:type :Dataset ;
  foaf:page "{}"^^xsd:anyURI ;
  dct:publisher "{}" ;
  dct:title "{}" ;
"""

TEMPLATE_PUBLICATION = """
:{}
  rdf:type :ResearchPublication ;
  foaf:page "{}"^^xsd:anyURI ;
  dct:publisher :{} ;
  dct:title "{}" ;
  dct:identifier "{}" ;
  :openAccess "{}"^^xsd:anyURI ;
"""


def load_journals (graph, out_buf):
    """
    load the journals
    """
    journals = {}

    for j in graph.journals.known.values():
        id_list = [ j["titles"][0] ]
        j["hash"] = rc_graph.RCGraph.get_hash(id_list, prefix="journal-")
        journals[j["id"]] = j

    for id, j in journals.items():
        out_buf.append(
            TEMPLATE_JOURNAL.format(
                j["hash"],
                j["titles"][0],
                ).strip()
            )

        if len(j["titles"]) > 1:
            for title in j["titles"][1:]:
                out_buf.append("  dct:alternative \"{}\" ;".format(title))

        if "issn" in j:
            # select the first element as the Linking ISSN
            issn_l = j["issn"][0]
            out_buf.append("  dct:identifier \"{}\" ;".format(issn_l))

        if "url" in j:
            out_buf.append("  foaf:page \"{}\"^^xsd:anyURI ;".format(j["url"]))

        out_buf.append(".\n")

    return journals


def load_datasets (out_buf):
    """
    load the datasets
    """
    known_datasets = {}

    with open(rc_graph.RCGraph.PATH_DATASETS, "r") as f:
        for d in json.load(f):
            dat_id = d["id"]
            id_list = [d["provider"], d["title"]]
            known_datasets[dat_id] = rc_graph.RCGraph.get_hash(id_list, prefix="dataset-")

            if "url" in d:
                url = d["url"]
            else:
                url = "http://example.com"

            out_buf.append(
                TEMPLATE_DATASET.format(
                    known_datasets[dat_id],
                    url,
                    d["provider"],
                    d["title"]
                    ).strip()
                )

            if "alt_title" in d:
                for alt_title in d["alt_title"]:
                    out_buf.append("  dct:alternative \"{}\" ;".format(alt_title))

            out_buf.append(".\n")

    return known_datasets


def load_publications (graph, out_buf, known_datasets, known_journals):
    """
    load publications, link to datasets, reshape metadata
    """
    seen = set([])

    for partition, pub_iter in graph.iter_publications(path=graph.BUCKET_FINAL):
        for pub in pub_iter:
            link_map = pub["datasets"]

            if "pdf" in pub and pub["pdf"] and (len(link_map) > 0):
                # generate UUID
                try:
                    pub["title"] = pub["title"].replace('"', "'")
                    url = pub["url"]

                    id_list = [pub["journal"], pub["title"]]
                    pub_id = rc_graph.RCGraph.get_hash(id_list, prefix="publication-")

                    # ensure uniqueness
                    if pub_id in seen:
                        continue
                    else:
                        seen.add(pub_id)
                except:
                    print("MISSING JOURNAL or URL")
                    pprint.pprint(pub)

                # reshape the metadata for corpus output
                journal_hash = known_journals[pub["journal"]]["hash"]

                out_buf.append(
                    TEMPLATE_PUBLICATION.format(
                        pub_id,
                        pub["url"],
                        journal_hash,
                        pub["title"],
                        pub["doi"],
                        pub["pdf"]
                        ).strip()
                    )

                # link to datasets
                dat_list = [ ":{}".format(known_datasets[dat_id]) for dat_id in link_map ]
                out_buf.append("  cito:citesAsDataSource {} ;".format(", ".join(dat_list)))
                out_buf.append(".\n")

    # return the unique count
    return len(seen)


def write_corpus (out_buf, vocab_file="vocab.json"):
    """
    output the corpus in TTL and JSON-LD
    """
    ## write the TTL output
    with open(CORPUS_TTL_FILENAME, "w") as f:
        for text in tqdm(out_buf, ascii=True, desc="write corpus"):
            f.write(text)
            f.write("\n")

    ## load the TTL output as a graph
    print("load and parse the corpus file")
    g = rdflib.Graph()
    g.parse(CORPUS_TTL_FILENAME, format="n3")

    ## transform graph into JSON-LD
    print("transform graph into JSON-LD")

    with open(vocab_file, "r") as f:
        context = json.load(f)

    with open(CORPUS_JSONLD_FILENAME, "wb") as f:
        f.write(g.serialize(format="json-ld", context=context, indent=2))

    ## read back, to confirm formatting
    print("confirm formatting")
    g = rdflib.Graph()
    g.parse(CORPUS_JSONLD_FILENAME, format="json-ld")


def main ():
    ## 1. load the metadata for journals, datasets, publications
    ## 2. validate the linked data
    ## 3. format output for the corpus as both TTL and JSON-LD

    out_buf = [ PREAMBLE.lstrip() ]

    graph = rc_graph.RCGraph("corpus")
    graph.journals.load_entities()

    known_journals = load_journals(graph, out_buf)
    known_datasets = load_datasets(out_buf)
    num_pubs = load_publications(graph, out_buf, known_datasets, known_journals)

    print("{} journals".format(len(known_journals)))
    print("{} datasets".format(len(known_datasets)))
    print("{} publications".format(num_pubs))

    write_corpus(out_buf)


if __name__ == "__main__":
    main()
