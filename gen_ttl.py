#!/usr/bin/env python
# encoding: utf-8

from graph import RCGraph
from rdflib.serializer import Serializer
import glob
import json
import pprint
import rdflib
import sys

PREAMBLE = """
@base <https://github.com/Coleridge-Initiative/adrf-onto/wiki/Vocabulary> .

@prefix cito:	<http://purl.org/spar/cito/> .
@prefix dct:	<http://purl.org/dc/terms/> .
@prefix foaf:	<http://xmlns.com/foaf/0.1/> .
@prefix rdf:	<http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:	<http://www.w3.org/2001/XMLSchema#> .
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
  dct:publisher "{}" ;
  dct:title "{}" ;
  dct:identifier "{}" ;
  :openAccess "{}"^^xsd:anyURI ;
"""

OVERRIDE = {}


def load_datasets (out_buf):
    """
    load the datasets
    """
    known_datasets = {}
    dataset_path = "datasets/datasets.json"

    with open(dataset_path, "r") as f:
        for elem in json.load(f):
            dat_id = elem["id"]
            id_list = [elem["provider"], elem["title"]]
            known_datasets[dat_id] = RCGraph.get_hash(id_list, prefix="dataset-")

            if "url" in elem:
                url = elem["url"]
            else:
                url = "http://example.com"

            out_buf.append(
                TEMPLATE_DATASET.format(
                    known_datasets[dat_id],
                    url,
                    elem["provider"],
                    elem["title"]
                    ).strip()
                )

            if "alt_title" in elem:
                for alt_title in elem["alt_title"]:
                    out_buf.append("  dct:alternative \"{}\" ;".format(alt_title))

            out_buf.append(".\n")

    return known_datasets


def iter_publications (override_path="human/manual/partitions/*.json"):
    """
    load the publications metadata, apply the manually curated
    override metadata, then yield an iterator
    """
    # load the manual override metadata
    for filename in glob.glob(override_path):
        with open(filename) as f:
            for elem in json.load(f):
                # one small fix...
                if "publisher" in elem["manual"]:
                    elem["manual"]["journal"] = elem["manual"]["publisher"]

                OVERRIDE[elem["title"]] = elem["manual"]

    # load the metadata stream
    for partition, pub_iter in graph.iter_publications(path="step5"):
        for elem in pub_iter:
            title = elem["title"]

            if title in OVERRIDE:
                OVERRIDE[title]["used"] = True

                if "omit-corpus" in OVERRIDE[title] and OVERRIDE[title]["omit-corpus"]:
                    # omit this publication from the corpus
                    continue

                for key in ["doi", "pdf", "journal", "url"]:
                    if key in OVERRIDE[title]:
                        elem[key] = OVERRIDE[title][key]

                if "datasets" not in elem:
                    elem["datasets"] = []

                if "datasets" in OVERRIDE[title]:
                    for dataset in OVERRIDE[title]["datasets"]:
                        if not dataset in elem["datasets"]:
                            elem["datasets"].append(dataset)

            # yield corrected metadata for one publication
            yield elem

    # did we miss any of the manual entries?
    for title, elem in OVERRIDE.items():
        if "used" not in elem:
            if "omit-corpus" in elem and elem["omit-corpus"]:
                continue
            else:
                elem["title"] = title
                yield elem


def load_publications (graph, out_buf, known_datasets):
    """
    load publications, link to datasets, reshape metadata
    """
    for elem in iter_publications():
        link_map = elem["datasets"]

        if "pdf" in elem and elem["pdf"] and (len(link_map) > 0):
            # generate UUID
            try:
                elem["title"] = elem["title"].replace('"', "'")
                url = elem["url"]

                id_list = [elem["journal"], elem["title"]]
                pub_id = RCGraph.get_hash(id_list, prefix="publication-")
            except:
                print("MISSING JOURNAL or URL")
                pprint.pprint(elem)
                print(len(out_buf))
                sys.exit(0)

            # reshape the metadata for corpus output
            out_buf.append(
                TEMPLATE_PUBLICATION.format(
                    pub_id,
                    elem["url"],
                    elem["journal"],
                    elem["title"],
                    elem["doi"],
                    elem["pdf"]
                    ).strip()
                )

            # link to datasets
            dat_list = [ ":{}".format(known_datasets[dat_id]) for dat_id in link_map ]
            out_buf.append("  cito:citesAsDataSource {} ;".format(", ".join(dat_list)))
            out_buf.append(".\n")


def write_corpus (out_buf, vocab_file="vocab.json"):
    """
    output the corpus in TTL and JSON-LD
    """
    corpus_ttl_filename = "tmp.ttl"
    corpus_jsonld_filename = "tmp.jsonld"

    ## write the TTL output
    with open(corpus_ttl_filename, "w") as f:
        for text in out_buf:
            f.write(text)
            f.write("\n")

    ## load the TTL output as a graph
    g = rdflib.Graph()
    g.parse(corpus_ttl_filename, format="n3")

    ## transform graph into JSON-LD
    with open(vocab_file, "r") as f:
        context = json.load(f)

    with open(corpus_jsonld_filename, "wb") as f:
        f.write(g.serialize(format="json-ld", context=context, indent=2))

    ## read back, to confirm formatting
    g = rdflib.Graph()
    g.parse(corpus_jsonld_filename, format="json-ld")


if __name__ == "__main__":

    ## 1. load the metadata for datasets and publications
    ## 2. apply manually curated metadata as override per publication, if any
    ## 3. validate the linked data
    ## 4. format output for the corpus as both TTL and JSON-LD

    out_buf = [ PREAMBLE.lstrip() ]
    known_datasets = load_datasets(out_buf)

    graph = RCGraph("step5")
    graph.journals.load_entities()

    load_publications(graph, out_buf, known_datasets)
    write_corpus(out_buf)
