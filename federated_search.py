#!/usr/bin/env python
# encoding: utf-8

from collections import OrderedDict, defaultdict
from pathlib import Path
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
import codecs
import json
import re
import sys
import traceback
import urllib.parse
import pandas as pd


# TODO This is a modified copy of title_match from scholapi.py class _ScholInfra
def title_match (title0, title1):
    """
    within reason, do the two titles match?
    """
    try:
        if not title0 or not title1:
            return False
        else:
            return re.sub("\W+","",title0).lower() == re.sub("\W+","",title1).lower()
    except Exception:
        # debug this as an edge case
        print('exception calling title_match')
        traceback.print_exc()
        return False


def get_xml_node_value (root, name):
    """
    return the named value from an XML node, if it exists
    """
    node = root.find(name)

    if not node:
        return None
    elif len(node.text) < 1:
        return None
    else:
        return node.text.strip()


def parse_oa (results):
    meta_list = []

    for result in results:
        if result.find("instancetype")["classname"] in ["Other literature type", "Article"]: ## TODO review this decision
            meta = OrderedDict()
            result_title = get_xml_node_value(result, "title")
            meta["title"] = result_title

            if get_xml_node_value(result, "journal"):
                meta["journal"] = get_xml_node_value(result, "journal")

            meta["url"] = get_xml_node_value(result, "url")
            meta["doi"] = get_xml_node_value(result, "pid") ##TODO not all information in pid field is a DOI reference, see https://www.openaire.eu/schema/1.0/doc/oaf-result-1_0_xsd.html#result_pid
            meta["open"] = len(result.find_all("bestaccessright",  {"classid": "OPEN"})) > 0
            meta["api"] = "openaire"
            meta_list.append(meta)
        else:
            pass

    if len(meta_list) > 0:
        return meta_list
    elif meta_list == []:
        return None


def parse_dimensions (results):
    meta_list = []

    for result in results:
        if result["type"] in ["article","preprint"]:
            meta = OrderedDict()
            meta["title"] = result["title"]
            meta["api"] = "dimensions"

            try:
                meta["journal"] = result["journal"]["title"]
            except:
                meta["journal"] = None

            try:
                meta["doi"] = result["doi"]
            except:
                meta["doi"] = None

            meta["url"] = "https://app.dimensions.ai/discover/publication?search_type=kws&search_field=full_search&search_text={}".format(urllib.parse.quote(meta["title"]))

            meta_list.append(meta)
        else:
            pass
    if len(meta_list) > 0:
        return meta_list
    elif meta_list == []:
        return None

def parse_pubmed_item (result):
    article_meta = result["MedlineCitation"]["Article"]

    meta = OrderedDict()
    meta['doi'] = None  # to enforce having a 'doi' key

    pmid = result["MedlineCitation"]["PMID"]["#text"]
    meta["url"] = f"https://www.ncbi.nlm.nih.gov/pubmed/{pmid}"

    try:
        # sometimes the ArticleTitle is a dict.
        if type(article_meta["ArticleTitle"]) is str:
            meta["title"] = article_meta["ArticleTitle"]
        else:
            meta["title"] = article_meta["ArticleTitle"]['#text']
    except Exception:
        # debug this as an edge case
        print('exception handling parse_pubmed ArticleTitle field')
        print('***** type(article_meta["ArticleTitle"]):', type(article_meta["ArticleTitle"]))
        print('*****', article_meta["ArticleTitle"])
        traceback.print_exc()
        meta["title"] = None
        return None

    meta["journal"] = article_meta["Journal"]["Title"]
    meta["api"] = "pubmed"

    try:
        pid_list = article_meta["ELocationID"]

        if isinstance(pid_list, list):
            doi_test = [d["#text"] for d in pid_list if d["@EIdType"] == "doi"]

            if len(doi_test) > 0:
                meta["doi"] = doi_test[0]

        if isinstance(pid_list, dict):
            if pid_list["@EIdType"] == "doi":
                meta["doi"] = pid_list["#text"]
    except:
        meta["doi"] = None

    return meta

def parse_pubmed (results):
    meta_list = []

    if isinstance(results,list):
        for result in results:
            if isinstance(result, dict) and "MedlineCitation" in result:
                meta = parse_pubmed_item(result)
                if meta:
                    meta_list.append(meta)

    # When pubmed retunrs only one article is not a list of one dict, but one dict.
    elif isinstance(results,dict) and "MedlineCitation" in results:
        meta = parse_pubmed_item(results)
        if meta:
            meta_list.append(meta)

    if len(meta_list) > 0:
        return meta_list
    elif meta_list == []:
        return None


def load_publications (graph):
    """
    Load publications from knowledge graph. Only DOI and Title fields
    """
    publications = []

    for partition, pub_iter in graph.iter_publications(path=graph.BUCKET_FINAL):
        for pub in pub_iter:
            aPublication = dict()

            if "doi" in pub:
                aPublication["doi"] = pub["doi"]
            else:
                aPublication["doi"] = None

            aPublication["title"] = pub["title"]
            publications.append(aPublication)

    return publications


def api_implements_full_text_search (api):
    """
    Returns True if api has a method named "full_text_search".
    Returns False if not.
    """
    implements = False

    try: #__getattribute__ raises an exception when "full_text_search" is missing
        #checks if api.full_text_search is defined and is a method.
        if callable(api.__getattribute__("full_text_search")):
            # print(api.name, "implements full_text_search")
            implements = True
    except Exception:
        # print(api.name, "does NOT implement full_text_search")
        implements = False

    return implements


def parse_results (apiName, results):
    # TODO handle all APIs
    if apiName == "OpenAIRE":
        search_hits = parse_oa(results)
    elif apiName == "Dimensions":
        search_hits = parse_dimensions(results)
    elif apiName == "PubMed":
        search_hits = parse_pubmed(results)
    else:
        search_hits = None
        print(apiName, "without a parser...skipping results")

    return search_hits


def get_api_list_with_full_text_search (schol):

    api_list = []

    #get all atributes of schol object
    dict = schol.__dict__

    for key, value in dict.items():
        if api_implements_full_text_search(value):
            api_list.append(value)

    return api_list


def create_datadrop (view, search_terms, file_path='federated.csv'):
    dfKnown = pd.DataFrame(view["known"])
    dfKnown["category"] = "known hits in KG"

    dfOverlap = pd.DataFrame(view["overlap"])
    dfOverlap["category"] = "overlap between APIs"

    dfUnique = pd.DataFrame(view["unique"])
    dfUnique["category"] = "unique hits"

    dfAll = pd.DataFrame()
    dfAll = dfAll.append(dfKnown)
    dfAll = dfAll.append(dfOverlap)
    dfAll = dfAll.append(dfUnique)

    if len(dfAll) > 0:

        dfAll = dfAll[["category","api","doi","title","url"]]
        dfAll["search_term"]= search_terms

        dfAll.to_csv(file_path, index=False, encoding="utf-8-sig")



def main (search_terms, limit):
    print("terms", search_terms)
    print("limit",limit)

    graph = rc_graph.RCGraph("corpus")

    # get known publications from knowledge graph
    pubs = load_publications(graph)
    print(len(pubs),"publications")

    # known DOIs and Titles already present in the Knoledge Graph
    known_dois = set([p["doi"] for p in pubs])
    if None in known_dois:
        known_dois.remove(None)
    if "" in known_dois:
        known_dois.remove("")

    known_titles = set([p["title"] for p in pubs])

    print(len(known_dois),"known DOIs")
    print(len(known_titles),"known titles")

    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    known_hits = list()
    new_overlapped_hits = list()
    new_unique_hits = list()

    search_hits = defaultdict(list)

    # call full_text_search on all APIs that implement it
    for api in get_api_list_with_full_text_search(schol):
        if api.has_credentials():
            try:
                meta, timing, message = api.full_text_search(search_term=search_terms, limit=limit)

                # if not empty, parse the result and get a list of elements returned by the API
                if meta:
                    print(api.name, "returned",len(meta),"hits")
                    results = parse_results(api.name, meta)

                    if results:
                        for item in results:
                            article = dict()

                            #make sure DOI field is a valid DOI
                            item["doi"] = graph.publications.verify_doi(item['doi'])

                            #assuming that all articles have a Title but not all articles have a DOI
                            if item["doi"] != None:
                                article['doi']=item['doi']

                            article['title']=item['title']
                            article['api']=item['api']
                            article["url"] = item["url"]

                            search_hits[item['doi']].append(article)

            except Exception:
                # debug this as an edge case
                print(api.name, 'exception calling full_text_search')

                if message:
                    print(message)

                traceback.print_exc()
                continue

    # after getting all the federated results, try to match search
    # hits with unknown DOI comparing by title to search hits with
    # known DOI.

    # iterate through articles grouped on missing DOI (doi == None)
    for non_doi_article in search_hits[None].copy(): # using copy since I will remove items from the list, and that would make the iterator to miss elements
        # iterate through each DOI in the search hits
        for doi, aggregated_hits in search_hits.items():
            if not doi:
                #TODO: case where two hits have same title and no DOI
                continue

            # here I use only the first item on the aggregated_hits since all titles in that list should match.
            if title_match(non_doi_article["title"],aggregated_hits[0]["title"]):
                #move the article from the None doi list to the matched doi list
                search_hits[doi].append(non_doi_article)
                search_hits[None].remove(non_doi_article)
                #once found, no need to continue the search
                break

    #now, explore the aggregated search hits and group in 3 kinds:
    #Already known in Knowledge Graph, Overlap results among APIs, and
    #unique hits.

    for doi,aggregated_hits in search_hits.items():

        if doi == None:
            #when DOI is None, I assume that all articles are different.
            #for each article title, look up in the known_titles
            for i in aggregated_hits:
                found = False

                for known_title in known_titles:
                    if title_match(known_title, i["title"]):
                        known_hits.append(i)
                        found = True
                        break

                if not found:
                    new_unique_hits.append(i)
                    # TODO: case where two hits have same title and no DOI, now are cataloged as unique hits

        #first, create a list of search hits that are already in the KG
        elif graph.publications.verify_doi(doi) in known_dois:
            #create one entry per doi-title-api
            for hit in aggregated_hits:
                known_hits.append(hit)
        else:
            # second, create a list of search hits returned by more than one API
            if len(aggregated_hits) > 1:
                #transform the list of dict into a set of tuples to
                #remove duplicated entries and back into a list of
                #dictionaries
                de_duplicated_hits = [dict(t) for t in {tuple(d.items()) for d in aggregated_hits}]

                for hit in de_duplicated_hits:
                    new_overlapped_hits.append(hit)

            else:
                #finally, create a list of search hits returned only by one API
                new_unique_hits.append(aggregated_hits[0])

    # order results by title to help the user spot duplicated hits
    known_hits = sorted(known_hits, key=lambda k: k['title'].lower())
    new_unique_hits = sorted(new_unique_hits, key=lambda k: k['title'].lower())

    #new_overlapped_hits = sorted(new_overlapped_hits, key=lambda k: k['title']) ##not sorting this results to keep it ordered by DOI

    # report about the results
    print("#known_hits", len(known_hits))
    print("#new_overlapped_hits", len(new_overlapped_hits))
    print("#new_unique_hits", len(new_unique_hits))

    # write the output file
    view = {
        "known": known_hits,
        "overlap": new_overlapped_hits,
        "unique": new_unique_hits
        }

    out_path = "federated.json"

    # write json file
    with codecs.open(Path(out_path), "wb", encoding="utf8") as f:
        json.dump(view, f, indent=4, sort_keys=True, ensure_ascii=False)

    # write a cvs file
    create_datadrop(view,search_terms,'federated.csv')


if __name__ == '__main__':
    #Enforcing only 2 parameters.
    if (len(sys.argv[1:]) != 2):
        print("Only 2 parameters allowed: 'search terms' and 'limit'")
        sys.exit(1)

    terms = sys.argv[1]
    limit = sys.argv[2]

    #TODO check limit is an integer.
    main(terms, limit)
