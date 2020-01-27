
import sys
import traceback
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi


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
    try: #__getattribute__ raises an exception when "full_text_search" is missing

        #checks if api.full_text_search is defined and is a method.
        if callable(api.__getattribute__("full_text_search")):
            print(api.name, "implements full_text_search")
            return True
    except Exception:
        pass
    finally:
        print(api.name, "does NOT implement full_text_search")
        return False

def main(search_terms, limit):
    print("terms", search_terms)
    print("limit",limit)

    graph = rc_graph.RCGraph("corpus")

    pubs = load_publications(graph)
    print(len(pubs),"publications")

    known_doi = set([p["doi"] for p in pubs])
    known_title = set([p["title"] for p in pubs])

    print(len(known_doi),"known DOIs")
    print(len(known_title),"known titles")

    schol = rc_scholapi.ScholInfraAPI(config_file="rc.cfg", logger=None)

    for api in [schol.openaire, schol.europepmc, schol.dimensions,schol.pubmed, schol.repec]: #TODO get a list of all APIs implemented
        if api_implements_full_text_search(api):
            try:
                meta = api.full_text_search(search_term=search_terms, limit=limit)
                print(type(meta), len(meta))
            except Exception:
                # debug this as an edge case
                traceback.print_exc()
                #print(search_terms)
                print(api.name,'exception calling full_text_search')
                continue

            #TODO: aggregate search hits which have the same DOI or similar title.

            # if meta and len(meta) > 0:
            #     title_match = True
            #     meta = dict(meta)
            #     #pub[api.name] = meta
            #     print(meta)
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
