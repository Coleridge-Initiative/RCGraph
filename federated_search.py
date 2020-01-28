
import sys
import traceback
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
from collections import OrderedDict


def get_xml_node_value(root, name):
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
        if result.find("instancetype")["classname"] in ["Other literature type", "Article"]: ## TODO review this IF
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

def parse_dimensions(results):
    meta_list = []
    for result in results:
        if result["type"] in ["article","preprint"]:
            meta = OrderedDict()
            meta["title"] = result["title"]
            meta["api"] = "dimensions"
            try:
                meta["journal"] = result["journal"]["title"]
            except:
                pass
            try:
                meta["doi"] = result["doi"]
            except:
                pass
            meta_list.append(meta)
        else:
            pass
    if len(meta_list) > 0:
        return meta_list
    elif meta_list == []:
        return None

def parse_pubmed(result):
    article_meta = result["MedlineCitation"]["Article"]
    meta = OrderedDict()
    meta["title"] = article_meta["ArticleTitle"]
    meta["journal"] = article_meta["Journal"]["Title"]
    meta["api"] = "pubmed"
    try:
        pid_list = article_meta["ELocationID"]
        if isinstance(pid_list,list):
                doi_test = [d["#text"] for d in pid_list if d["@EIdType"] == "doi"]
                if len(doi_test) > 0:
                    meta["doi"] = doi_test[0]
        if isinstance(pid_list,dict):
            if pid_list["@EIdType"] == "doi":
                meta["doi"] = pid_list["#text"]
    except:
        pass
    return meta

def load_publications (graph):
    """
    load publications. Only DOI and Title
    """
    publications = []

    for partition, pub_iter in graph.iter_publications(path=graph.BUCKET_FINAL):
        for pub in pub_iter:
            if "doi" in pub:
                doi = pub["doi"]
            title = pub["title"]
            aPublication = dict()
            aPublication["doi"]=doi
            aPublication["title"]=title

            publications.append(aPublication)
    return publications

def api_implements_full_text_search(api):
    """
    Returns True if api has a method named "full_text_search".
    Returns False if not.
    """
    implements = False
    try: #__getattribute__ raises an exception when "full_text_search" is missing

        #checks if api.full_text_search is defined and is a method.
        if callable(api.__getattribute__("full_text_search")):
            print(api.name, "implements full_text_search")
            implements = True
    except Exception:
        print(api.name, "does NOT implement full_text_search")
        implements = False

    return implements

def parse_results(apiName,results):
    # TODO handle all APIs

    if apiName == "OpenAIRE":
        search_hits = parse_oa(results)
    elif apiName == "Dimensions":
        search_hits = parse_dimensions(results)
    elif apiName == "PubMed":
        search_hits = parse_pubmed(results)
    else:
        search_hits = results
    return search_hits

def main(search_terms, limit):
    print("terms", search_terms)
    print("limit",limit)

    graph = rc_graph.RCGraph("corpus")

    pubs = load_publications(graph)
    print(len(pubs),"publications")

    # known DOIs and Titles already present in the Knoledge Graph
    known_dois = set([p["doi"] for p in pubs])
    known_titles = set([p["title"] for p in pubs])

    print(len(known_dois),"known DOIs")
    print(len(known_titles),"known titles")

    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    # will use these to aggregate results from the federated search
    hit_titles = set()
    hit_dois = set()

    for api in [schol.openaire, schol.europepmc, schol.dimensions,schol.pubmed, schol.repec]: #TODO get a list of all APIs implemented
        if api_implements_full_text_search(api):
            try:
                meta, timing, message = api.full_text_search(search_term=search_terms, limit=limit)

                # parse the result and get a list of elements returned by the API
                search_hits = parse_results(api.name,meta)
                for item in search_hits:
                    if item["title"] is not None:
                        hit_titles.add(item["title"])
                    if item["doi"] is not None:
                        hit_dois.add( graph.publications.verify_doi(item["doi"]) ) ## TODO: remove None value from hit_dois set since verify_doi sometimes returns None

                print('#hits',len(search_hits),'titles',len(hit_titles),'DOIs',len(hit_dois))

            except Exception:
                # debug this as an edge case
                traceback.print_exc()
                #print(search_terms)
                print(api.name,'exception calling full_text_search')
                continue

    #show articles that already exists in the Knowledge Graph
    repeated_dois = known_dois.intersection(hit_dois)
    reapeated_titles = known_titles.intersection(hit_titles)

    print("repeated DOIs:\n",(repeated_dois))
    print("repeated titles\n",(reapeated_titles))
    # TODO: aggregate search hits which have the same DOI or similar title.

    return



if __name__ == '__main__':

    #Enforcing only 2 parameters.
    if(len(sys.argv[1:]) != 2):
        print("Only 2 parameters allowed: 'search terms' and 'limit'")
        exit(1)
    terms = sys.argv[1]
    limit = sys.argv[2]

    #TODO check limit is an integer.
    main(terms, limit)
