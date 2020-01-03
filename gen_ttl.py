#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from rdflib.serializer import Serializer
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
import argparse
import codecs
import json
import pprint
import rdflib
import skosify
import sys
import traceback

DEFAULT_FULL_GRAPH = False

PATH_CORPUS_TTL = Path("tmp.ttl")
PATH_VOCAB_JSONLD = Path("vocab.json")
PATH_SKOSIFY_CFG = Path("adrf-onto/skosify.cfg")
PATH_ADRF_TTL = Path("adrf-onto/adrf.ttl")
PATH_VOC_TTL = Path("voc.ttl")


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
  dct:publisher "{}" ;
  dct:title "{}" ;
"""

TEMPLATE_PROVIDER = """
:{}
  rdf:type :Provider ;
  dct:title "{}" ;
"""

TEMPLATE_PUBLICATION = """
:{}
  rdf:type :ResearchPublication ;
  dct:publisher :{} ;
  dct:title "{}" ;
"""


def load_providers (graph, frags):
    """
    load the providers
    """
    known_providers = {}

    with codecs.open(graph.PATH_PROVIDERS, "r", encoding="utf8") as f:
        for p in json.load(f):
            buf = []
            prov_id = p["id"]

            # use persistent identifiers where possible
            if "ror" in p:
                id_list = [p["ror"]]
            else:
                id_list = [p["title"]]

            p["uuid"] = graph.get_hash(id_list, prefix="provider-")
            known_providers[prov_id] = p

            buf.append(
                TEMPLATE_PROVIDER.format(
                    p["uuid"],
                    p["title"]
                    ).strip()
                )

            if "ror" in p:
                buf.append("  dct:identifier \"https://ror.org/{}\"^^xsd:anyURI ;".format(p["ror"]))

            if "url" in p:
                buf.append("  foaf:page \"{}\"^^xsd:anyURI ;".format(p["url"]))

            buf.append(".\n")
            frags[p["uuid"]] = buf

    return known_providers


def load_datasets (graph, frags, used, known_providers):
    """
    load the datasets
    """
    known_datasets = {}

    with codecs.open(graph.PATH_DATASETS, "r", encoding="utf8") as f:
        for d in json.load(f):
            buf = []
            dat_id = d["id"]

            prov_id = d["provider"]
            used.add(known_providers[prov_id]["uuid"])

            # use persistent identifiers where possible
            # for datasets, the ADRF ontology is the ID
            id_list = [prov_id, d["title"]]
            d["uuid"] = graph.get_hash(id_list, prefix="dataset-")
            known_datasets[dat_id] = d

            buf.append(
                TEMPLATE_DATASET.format(
                    d["uuid"],
                    known_providers[prov_id]["uuid"],
                    d["title"]
                    ).strip()
                )

            if "alt_title" in d:
                for alt_title in d["alt_title"]:
                    buf.append("  dct:alternative \"{}\" ;".format(alt_title))

            if "url" in d:
                buf.append("  foaf:page \"{}\"^^xsd:anyURI ;".format(d["url"]))

            buf.append(".\n")
            frags[d["uuid"]] = buf

    return known_datasets


def load_journals (graph, frags):
    """
    load the journals
    """
    known_journals = {}

    for j in graph.journals.known.values():
        # use persistent identifiers where possible
        if "issn" in j:
            id_list = [ j["issn"][0] ]
        else:
            id_list = [ j["titles"][0] ]

        j["uuid"] = graph.get_hash(id_list, prefix="journal-")
        known_journals[j["id"]] = j

    for id, j in known_journals.items():
        buf = []

        buf.append(
            TEMPLATE_JOURNAL.format(
                j["uuid"],
                j["titles"][0],
                ).strip()
            )

        if len(j["titles"]) > 1:
            for title in j["titles"][1:]:
                buf.append("  dct:alternative \"{}\" ;".format(title))

        if "issn" in j:
            # select the first element as the Linking ISSN
            issn_l = j["issn"][0]
            buf.append("  dct:identifier \"https://portal.issn.org/resource/ISSN/{}\"^^xsd:anyURI ;".format(issn_l))

        if "url" in j:
            buf.append("  foaf:page \"{}\"^^xsd:anyURI ;".format(j["url"]))

        buf.append(".\n")
        frags[j["uuid"]] = buf

    return known_journals


def format_pub (out_buf, pub, pub_id, used, known_journals, known_datasets, link_map, full_graph):
    """
    format one publication, serialized as TTL
    """
    # test for open access PDF
    if "pdf" in pub and pub["pdf"]:
        # has an open access PDF
        pdf = pub["pdf"]
    elif full_graph:
        # publish the full graph anyway
        pdf = None
    else:
        # do not output this publication
        return False

    # reshape the metadata for corpus output
    journal_id = known_journals[pub["journal"]]["uuid"]
    used.add(journal_id)

    out_buf.append(
        TEMPLATE_PUBLICATION.format(
            pub_id,
            journal_id,
            pub["title"]
            ).strip()
        )

    if "doi" in pub:
        out_buf.append("  dct:identifier \"https://doi.org/{}\"^^xsd:anyURI ;".format(pub["doi"]))

    if "url" in pub:
        out_buf.append("  foaf:page \"{}\"^^xsd:anyURI ;".format(pub["url"]))

    if pdf:
        out_buf.append("  :openAccess \"{}\"^^xsd:anyURI ;".format(pdf))

    # link to datasets
    dat_list = []

    for dat_id in link_map:
        dat_list.append(":{}".format(known_datasets[dat_id]["uuid"]))
        used.add(known_datasets[dat_id]["uuid"])

    out_buf.append("  cito:citesAsDataSource {} ;".format(", ".join(dat_list)))
    out_buf.append(".\n")

    return True


def load_publications (graph, used, out_buf, known_datasets, known_journals, full_graph):
    """
    load publications, link to datasets, reshape metadata
    """
    seen = set([])

    for partition, pub_iter in graph.iter_publications(path=graph.BUCKET_FINAL):
        for pub in pub_iter:
            link_map = pub["datasets"]

            if len(link_map) > 0:
                # prep titles for generating TTL
                pub["title"] = pub["title"].replace('"', "'").replace("\\", "-")

                # generate UUID
                try:
                    # use persistent identifiers where possible
                    if "doi" in pub:
                        id_list = pub["doi"]
                    else:
                        title = pub["title"].replace(".", "").replace(" ", "")
                        id_list = [pub["journal"], title]

                    pub_id = graph.get_hash(id_list, prefix="publication-")

                    # ensure uniqueness
                    if pub_id not in seen:
                        if format_pub(out_buf, pub, pub_id, used, known_journals, known_datasets, link_map, full_graph):
                            seen.add(pub_id)
                except:
                    traceback.print_exc()
                    print("MISSING JOURNAL or URL")
                    pprint.pprint(pub)

    # return the unique count
    return len(seen)


def write_corpus (frags, used, out_buf, path):
    """
    output the corpus in TTL and JSON-LD
    """
    ## add the used fragments
    for id, buf in sorted(frags.items()):
        if id in used:
            out_buf.extend(buf)

    ## write the TTL output
    with codecs.open(path, "wb", encoding="utf8") as f:
        for text in tqdm(out_buf, ascii=True, desc="write corpus"):
            f.write(text)
            f.write("\n")

    ## load the TTL output as a graph
    print("load and parse the corpus file")

    g = rdflib.Graph()
    g.parse(str(path), format="n3")

    ## transform graph into JSON-LD
    print("transform graph into JSON-LD")

    path_jsonld = Path(path.stem + ".jsonld")

    with codecs.open(PATH_VOCAB_JSONLD, "r", encoding="utf8") as f:
        context = json.load(f)

    with codecs.open(path_jsonld, "wb", encoding="utf8") as f:
        buf = g.serialize(format="json-ld", context=context, indent=2).decode("utf8")
        f.write(buf)

    ## read back, to confirm formatting
    print("confirm formatting")

    g = rdflib.Graph()
    g.parse(str(path_jsonld), format="json-ld")


def test_corpus (path):
    """
    see docs:
    https://semantic-web.com/2017/08/21/standard-build-knowledge-graphs-12-facts-skos/
    https://skosify.readthedocs.io/en/latest/
    """
    # load the graph
    g = rdflib.Graph()

    for ttl_path in [ str(PATH_ADRF_TTL), str(path) ]:
        print("loading TTL file: {}".format(ttl_path))
        g.parse(ttl_path, format="n3")

    # an example lookup
    # https://github.com/Coleridge-Initiative/adrf-onto/wiki/Vocabulary#Catalog
    print("lookup `NHANES` ...")
    print(list(g[::rdflib.Literal("NHANES")]))

    # convert, extend, and check the SKOS vocabulary used
    config = skosify.config(PATH_SKOSIFY_CFG)
    voc = skosify.skosify(g, **config)
    voc.serialize(destination=str(PATH_VOC_TTL), format="n3")

    # validate the inference rules
    skosify.infer.skos_related(g)
    skosify.infer.skos_topConcept(g)
    skosify.infer.skos_hierarchical(g, narrower=True)
    skosify.infer.skos_transitive(g, narrower=True)

    skosify.infer.rdfs_classes(g)
    skosify.infer.rdfs_properties(g)

    # for the humans watching, print a note that all steps completed
    print("OK")


def main (args):
    ## 1. load the metadata for providers, datasets, journals, publications
    ## 2. validate the linked data
    ## 3. format output for the corpus as both TTL and JSON-LD

    graph = rc_graph.RCGraph("corpus")
    graph.journals.load_entities()

    frags = {}
    used = set([])

    known_providers = load_providers(graph, frags)
    known_datasets = load_datasets(graph, frags, used, known_providers)
    known_journals = load_journals(graph, frags)

    out_buf = [ PREAMBLE.lstrip() ]
    num_pubs = load_publications(graph, used, out_buf, known_datasets, known_journals, args.full_graph)
    write_corpus(frags, used, out_buf, PATH_CORPUS_TTL)

    num_prov = len(used.intersection(set([p["uuid"] for p in known_providers.values()])))
    num_data = len(used.intersection(set([d["uuid"] for d in known_datasets.values()])))
    num_jour = len(used.intersection(set([j["uuid"] for j in known_journals.values()])))

    print(f"{num_prov} providers written")
    print(f"{num_data} datasets written")
    print(f"{num_jour} journals written")
    print(f"{num_pubs} publications written")

    test_corpus(PATH_CORPUS_TTL)


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="generate a corpus update in TTL and JSON-LD"
        )

    parser.add_argument(
        "--full_graph",
        type=bool,
        default=DEFAULT_FULL_GRAPH,
        help="generate the full graph, not just the open source subset"
        )

    main(parser.parse_args())
