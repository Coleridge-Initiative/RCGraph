import re
import sys
import codecs
import json
import time
from difflib import SequenceMatcher
from pprint import pprint
from fuzzywuzzy import fuzz
from datasketch import MinHashLSHEnsemble, MinHash
from sklearn import metrics
from pathlib import Path
import pandas as pd

KNOWN = 1
UNKNOWN = 0
DEBUG = False

CALIBRATE_LSH = False
LSH_THRESHOLD = 0.79 #Required when CALIBRATE_LSH == False

CALIBRATE_SEQUENCEMATCHER = False
SEQUENCEMATCHER_THRESHOLD = 0.55 #Required when CALIBRATE_SEQUENCEMATCHER == False

CALIBRATE_FUZZYWUZZY = False
FUZZYWUZZY_THRESHOLD = 0.54 #Required when CALIBRATE_SEQUENCEMATCHER == False

ADRF_PROVIDERS_JSON_PATH = "../adrf_data/2020_03_25/dataset_providers_03_25_2020.json"
ADRF_DATASET_JSON_PATH = "../adrf_data/2020_03_25/datasets_03_25_2020.json"

RC_PROVIDERS_JSON_PATH = "../../datasets/providers.json"
RC_DATASET_JSON_PATH = "../../datasets/datasets.json"


def get_set_of_words(text):
    return re.sub("[\W+]", " ", text).lower().split()




def load_classified_vector(vector_path,adrf_dataset_list):
    vector = list()
    classified = list()

    vectorDF = pd.read_csv(vector_path)

    for adrf_dataset in adrf_dataset_list:

        #search the adrf_dataset in the classified vector
        for index, row in vectorDF.iterrows():
            if adrf_dataset["fields"]["dataset_id"] == row["adrf_id"]:
                if row['link'] != "FALSE":
                    vector.append(KNOWN)
                    classified.append(adrf_dataset["fields"]["dataset_id"])
                    break
                else:
                    vector.append(UNKNOWN)
                    classified.append(adrf_dataset["fields"]["dataset_id"])
                    break

    return vector,classified




def export_linkages_to_csv(resultDF, filename):

    # load rich context providers information
    rc_providers_DF = pd.read_json(RC_PROVIDERS_JSON_PATH)

    # load ADRF providers information
    with codecs.open(ADRF_PROVIDERS_JSON_PATH, "r", encoding="utf8") as f:
        adrf_providers_DF = pd.json_normalize(json.load(f))

    # fields used to merge dataframes needs to be of the same type
    adrf_providers_DF["pk"] = adrf_providers_DF["pk"].apply(str)
    resultDF["ADRF_provider_id"] = resultDF["ADRF_provider_id"].apply(str)

    # add provider's names from RC and ADRF
    resultDF = resultDF.merge(rc_providers_DF, how="inner", left_on="RC_provider_id", right_on="id")
    resultDF = resultDF.merge(adrf_providers_DF, how="inner", left_on="ADRF_provider_id", right_on="pk")

    # remove useless columns
    resultDF.drop(["id", "ror", "model","pk"], axis=1,inplace=True)

    # rename fields containing provider name
    resultDF = resultDF.rename(columns={"title": "RC_Provider", "fields.name": "ADRF_Provider"})

    # reorder columns
    resultDF = resultDF[['RC_id', 'ADRF_id', 'RC_title', 'ADRF_title', 'RC_Provider', 'ADRF_Provider', \
                         'RC_description', 'ADRF_description', 'RC_provider_id', 'ADRF_provider_id']]


    # write csv file
    resultDF.to_csv(filename, index=False, encoding="utf-8-sig")


def link_records():

    linksDF = pd.read_csv("manual_crosslink_for_maybies.csv")

    with codecs.open(ADRF_DATASET_JSON_PATH, "r", encoding="utf8") as f:
        adrf_dataset_DF = pd.json_normalize(json.load(f))

    rc_dataset_DF = pd.read_json(RC_DATASET_JSON_PATH)

    adrf_dataset_DF = adrf_dataset_DF[['fields.title','fields.description','fields.dataset_id','fields.data_provider']]
    rc_dataset_DF = rc_dataset_DF[['id','title', 'description','provider']]

    adrf_dataset_DF = adrf_dataset_DF.rename(columns={"fields.title": "ADRF_title",
                                                      "fields.description": "ADRF_description",
                                                      "fields.dataset_id": "ADRF_id",
                                                      "fields.data_provider": "ADRF_provider_id",
                                                      })
    rc_dataset_DF = rc_dataset_DF.rename(columns={"id": "RC_id",
                                                      "title": "RC_title",
                                                      "description": "RC_description",
                                                      "provider": "RC_provider_id",
                                                      })

    resultDF = linksDF.merge(adrf_dataset_DF,how="inner",on="ADRF_id")
    resultDF = resultDF.merge(rc_dataset_DF, how="inner",on="RC_id")

    # write csv file

    export_linkages_to_csv(resultDF, "manual_crosslink_all_fields_for_maybies.csv")


def main(corpus_path, search_for_matches_path, classified_vector_path):
    pass
    # #Load all dataset adrf_ids and titles from ADRF dump
    # with codecs.open(search_for_matches_path, "r", encoding="utf8") as f:
    #     adrf_dataset_list = json.load(f)
    #
    # print("loaded ADRF dataset corpus...", type(adrf_dataset_list), len(adrf_dataset_list))
    #
    # # Load all dataset ids and titles from dataset.json
    # with codecs.open(corpus_path, "r", encoding="utf8") as f:
    #     rc_dataset_list = json.load(f)
    #
    # print("loaded RC dataset corpus...",type(rc_dataset_list),len(rc_dataset_list))
    #
    # test_vector,classified_ids = load_classified_vector(classified_vector_path, adrf_dataset_list)
    # print("loaded clasiffied data from",classified_vector_path,"|",len(test_vector),"data points")
    #
    # print("creating MinHash for each RC dataset...")
    #
    # # create a MinHash for each dataset title and a structure to access title and its set of unique words
    # rc_corpus = dict()
    # for dataset in rc_dataset_list:
    #     d = dict()
    #     d["title"] = dataset["title"]
    #     d["words"] = get_set_of_words(dataset["title"])
    #     if "url" in dataset:
    #         d["url"] = dataset["url"]
    #     if "description" in dataset:
    #         d["description"] = dataset["description"]
    #     d["provider_id"] = dataset["provider"]
    #


def create_ADRF_csv(filename):

    # Load all dataset adrf_ids and titles from ADRF dump
    with codecs.open(ADRF_DATASET_JSON_PATH, "r", encoding="utf8") as f:
        adrf_dataset_list = json.load(f)


    # load ADRF providers information
    with codecs.open(ADRF_PROVIDERS_JSON_PATH, "r", encoding="utf8") as f:
        adrf_providers_DF = pd.json_normalize(json.load(f))

    # load ADRF datasets information
    with codecs.open(ADRF_DATASET_JSON_PATH, "r", encoding="utf8") as f:
        adrf_datasets_DF = pd.json_normalize(json.load(f))

    # keep only the fields needed
    adrf_datasets_DF = adrf_datasets_DF[["fields.dataset_id","fields.title","fields.description","fields.data_provider"]]

    # rename fields
    adrf_datasets_DF = adrf_datasets_DF.rename(columns={"fields.dataset_id": "ADRF_id",
                                                        "fields.title": "ADRF_title",
                                                        "fields.description": "ADRF_description",
                                                        "fields.data_provider": "ADRF_provider_id",
                                                        })

    # fields used to merge dataframes needs to be of the same type
    adrf_providers_DF["pk"] = adrf_providers_DF["pk"].apply(str)
    adrf_datasets_DF["ADRF_provider_id"] = adrf_datasets_DF["ADRF_provider_id"].apply(str)

    # add provider's names from ADRF
    adrf_datasets_DF = adrf_datasets_DF.merge(adrf_providers_DF, how="inner", left_on="ADRF_provider_id", right_on="pk")

    # remove useless columns
    adrf_datasets_DF.drop([ "model","pk"], axis=1,inplace=True)

    # rename fields containing provider name
    adrf_datasets_DF = adrf_datasets_DF.rename(columns={"fields.name": "ADRF_provider"})

    # reorder columns
    adrf_datasets_DF = adrf_datasets_DF[['ADRF_id', 'ADRF_title', 'ADRF_provider', \
                          'ADRF_description', 'ADRF_provider_id']]


    # write csv file
    adrf_datasets_DF.to_csv(filename, index=False, encoding="utf-8-sig")




if __name__ == '__main__':

    #Enforcing only 2 parameters.
    # if(len(sys.argv[1:]) != 2):
    #     print("Only 2 parameters allowed")
    #     exit(1)
    # corpus_path = sys.argv[1]
    # search_for_matches_path = sys.argv[2]

    # TODO using a temporal copy of datsets.json instead the most updated version˚
    corpus_path= "../datasets/datasets.json"
    search_for_matches_path = "adrf_data/datasets-02-11-2020.json"

    # TODO: classified vector is probably biased. It does not cover any edge case.
    classified_vector_path = "training_vector_1.01.csv"

    #main(corpus_path,search_for_matches_path,classified_vector_path)

    #create_ADRF_csv("2020_03_25_complete_adrf.csv")

    link_records()