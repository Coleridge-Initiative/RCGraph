import codecs
import sys
import json
import pandas as pd
from pathlib import Path

from richcontext import graph as rc_graph


def load_publications (graph):#, used, out_buf, known_datasets, known_journals, known_authors, full_graph):
    """
    load publications
    """
    publications = []

    for partition, pub_iter in graph.iter_publications(path=graph.BUCKET_FINAL):
        for pub in pub_iter:
            if "doi" in pub:
                doi = pub["doi"]
            else:
                 doi = "TODO handle null DOI........."
            title = pub["title"]
            aPublication = dict()
            aPublication["doi"]=doi
            aPublication["title"]=title

            publications.append(aPublication)
    return publications

def main(terms,limit):
    print("terms",terms)
    print("limit",limit)

    graph = rc_graph.RCGraph("corpus")

    pubs = load_publications(graph)
    print(len(pubs))
    return
    # dataPath = '/bucket_final'
    # df = pd.read_json(Path('bucket_final/20190610_usda_iri_publications.json'))
    # df.head()

    with codecs.open(Path('bucket_final/20190610_usda_iri_publications.json'), "r", encoding="utf8") as f:
        publications = json.load(f)

    extractedData = []
    for publication in publications:
        print("doi"+publication["doi"]+"\n")
        extractedData.append(publication)
    # print(publications)



if __name__ == '__main__':

    #Enforcing only 2 parameters.
    if(len(sys.argv[1:]) != 2):
        print("Only 2 parameters allowed: 'search terms' and 'limit'")
        exit(1)
    terms = sys.argv[1]
    limit = sys.argv[2]

    #TODO check limit is an integer.
    main(terms, limit)
