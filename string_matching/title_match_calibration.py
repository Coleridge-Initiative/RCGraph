import re
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
from richcontext import graph as rc_graph

CALIBRATE_LSH = True
LSH_THRESHOLD = 0.79  # Required when CALIBRATE_LSH == False

CALIBRATE_SEQUENCEMATCHER = True
SEQUENCEMATCHER_THRESHOLD = 0.55  # Required when CALIBRATE_SEQUENCEMATCHER == False

CALIBRATE_FUZZYWUZZY = True
FUZZYWUZZY_THRESHOLD = 54  # Required when CALIBRATE_SEQUENCEMATCHER == False

DEBUG = False

ADRF_PROVIDERS_JSON_PATH = "adrf_data/2020_03_25/dataset_providers_03_25_2020.json"
RC_PROVIDERS_JSON_PATH = "../datasets/providers.json"

ADRF_DATASET_JSON_PATH = "adrf_data/2020_03_25/datasets_03_25_2020.json"
RC_DATASET_JSON_PATH = "../datasets/datasets.json"

class RCTitleMatcher:

    UNKNOWN = 0
    KNOWN = 1

    def get_set_of_words (self, text):
        return re.sub("[\W+]", " ", text).lower().split()


    def get_confusion_matrix_scores (self, test_vector, result):
        tn, fp, fn, tp = metrics.confusion_matrix(test_vector, result).ravel()

        scores = dict()
        confusion_matrix = dict()
        confusion_matrix["TN"] = tn
        confusion_matrix["FP"] = fp
        confusion_matrix["FN"] = fn
        confusion_matrix["TP"] = tp
        scores["confusion_matrix"] = confusion_matrix
        scores["accuracy_score"] = metrics.accuracy_score(test_vector, result)
        scores["recall_score"] = metrics.recall_score(test_vector, result)
        scores["precision_score"] = metrics.precision_score(test_vector, result)
        scores["f1_score"] = metrics.f1_score(test_vector, result)  # harmonic mean of Precision and Recall
        scores["specificity_score"] = tn / (tn + fp)
        scores["False Positive Rate or Type I Error"] = fp / (fp + tn)
        scores["False Negative Rate or Type II Error"] = fn / (fn + tp)
        return scores


    def generate_minhash (self, entities_list, target_text_field, id_field, other_fields = {}):

        entities_list_with_minhash = dict()

        for entity in entities_list:

            # id_field is required. If not present, the entity is skipped from the corpus list
            if id_field not in entity:
                print("entity does not have field",id_field)
                continue

            d = dict()
            d[target_text_field] = entity[target_text_field]
            d["words"] = self.get_set_of_words(entity[target_text_field])

            for field_original_name, field_display_name in other_fields.items():
                if field_original_name in entity:
                    d[field_display_name] = entity[field_original_name]

            mh = MinHash(num_perm=128)
            for term in d["words"]:
                mh.update(term.encode("utf8"))
            d["min_hash"] = mh
            entities_list_with_minhash[entity[id_field]] = d

        return entities_list_with_minhash


    def create_lsh_ensemble (self, lsh_threshold, rc_corpus):
        print("creating MinHashLSHEnsemble with threshold=%s, num_perm=128, num_part=16..." % lsh_threshold)
        # Create an LSH Ensemble index with threshold and number of partition settings.
        lshensemble = MinHashLSHEnsemble(threshold=lsh_threshold, num_perm=128, num_part=16)
        print("indexing all RC dataset's MinHash...")
        # Index takes an iterable of (key, minhash, size)
        lshensemble.index([(key, values["min_hash"], len(values["words"])) for key, values in rc_corpus.items()])
        return lshensemble


    def test_lsh_threshold (self, classified_minhash, rc_corpus, lsh_threshold):

        lshensemble = self.create_lsh_ensemble(lsh_threshold, rc_corpus)

        # test by querying the LSH Ensemble with each classified entity title to explore potential matches
        results = list()
        for id, values in classified_minhash.items():
            m1 = values["min_hash"]
            set1 = values["words"]
            # print("\nquery for '%s' yields datasets" % adrf_dataset["fields"]["title"])
            matches = False
            for key in lshensemble.query(m1, len(set1)):
                # print(key, rc_corpus[key]["title"])
                matches = True
                break
            if matches:
                results.append(self.KNOWN)
            else:
                results.append(self.UNKNOWN)
                # print("no matches")

        return results


    def calibrate_lsh_threshold (self, classified_minhash, rc_corpus, test_vector):
        calibration_metrics = dict()
        max_f1_score = 0
        selected_lsh_threshold = 0
        for step in range(60, 100, 1):

            lsh_threshold = step / 100
            print(lsh_threshold)

            result = self.test_lsh_threshold(classified_minhash, rc_corpus, lsh_threshold)

            scores = self.get_confusion_matrix_scores(test_vector, result)

            print('confusion matrix for ' + str(lsh_threshold))
            # print("\tTP: " + str(tp) + "\tFP: " + str(fp))
            # print("\tFN: " + str(fn) + "\tTN: " + str(tn))
            pprint(scores["confusion_matrix"])

            calibration_metrics[lsh_threshold] = scores

            if scores["f1_score"] > max_f1_score:
                selected_lsh_threshold = lsh_threshold
                max_f1_score = scores["f1_score"]
        if DEBUG:
            print("\nshowing all metrics...")
            pprint(calibration_metrics)
        print("Selected threshold:", selected_lsh_threshold)
        pprint(calibration_metrics[selected_lsh_threshold])

        return selected_lsh_threshold


class RCDatasetTitleMatcher(RCTitleMatcher):

    def load_classified_vector (self, vector_path, adrf_dataset_list):
        vector = list()
        classified = list()

        vectorDF = pd.read_csv(vector_path)

        for adrf_dataset in adrf_dataset_list:

            # search the adrf_dataset in the classified vector
            for index, row in vectorDF.iterrows():
                if adrf_dataset["fields"]["dataset_id"] == row["adrf_id"]:
                    if row['link'] != "FALSE":
                        vector.append(self.KNOWN)
                        classified.append(adrf_dataset["fields"]["dataset_id"])
                        break
                    else:
                        vector.append(self.UNKNOWN)
                        classified.append(adrf_dataset["fields"]["dataset_id"])
                        break

        return vector, classified


    def load_corpus (self, corpus_path):

        # Load all dataset from dataset.json
        with codecs.open(corpus_path, "r", encoding="utf8") as f:
            rc_dataset_list = json.load(f)

        print("loaded RC dataset corpus...", type(rc_dataset_list), len(rc_dataset_list))

        # fields I need in the rc_corpus list
        fields = {"url": "url",
             "description": "description",
             "provider": "provider_id"}

        # create a MinHash for each dataset title and a structure to access title and its set of unique words
        rc_corpus = self.generate_minhash(entities_list=rc_dataset_list, target_text_field="title",
                                          id_field="id", other_fields=fields)
        return rc_corpus


    def export_linkages_to_csv (self, resultDF, filename):

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
        resultDF.drop(["id", "ror", "model", "pk"], axis=1, inplace=True)

        # rename fields containing provider name
        resultDF = resultDF.rename(columns={"title": "RC_Provider", "fields.name": "ADRF_Provider"})

        # reorder columns
        resultDF = resultDF[['RC_id', 'ADRF_id', 'RC_title', 'ADRF_title', 'RC_Provider', 'ADRF_Provider', \
                             'RC_description', 'ADRF_description', 'RC_provider_id', 'ADRF_provider_id']]

        # write csv file
        resultDF.to_csv(filename, index=False, encoding="utf-8-sig")


    def test_sm_threshold (self, adrf_classified_minhash, lsh_ensemble, rc_corpus, sequenceMatcher_threshold):
        print("******** SequenceMatcher threshold", sequenceMatcher_threshold, "*******")
        # iterate the adrf_dataset_list, but only test the text matcher with the cases present on the test_vector
        results = list()
        for key, values in adrf_classified_minhash.items():

            m1 = values["min_hash"]
            set1 = values["words"]
            matches = False
            # this forces that any match will have at least the SM_threshold
            max_score = sequenceMatcher_threshold

            # search the adrf dataset title in the LSH index and for potential hits
            for rc_dataset_id in lsh_ensemble.query(m1, len(set1)):
                # print(rc_dataset_id, rc_corpus[rc_dataset_id]["title"])
                s = SequenceMatcher(None, rc_corpus[rc_dataset_id]["title"], values["title"])
                # select the best match
                if (s.ratio() >= max_score):
                    best_match = rc_dataset_id
                    max_score = s.ratio()
                    matches = True

            if matches:
                if DEBUG:
                    print("Searching for", values["title"])
                    print("matches with", best_match, rc_corpus[best_match]["title"])
                    print("with a SequenceMatcher ratio", max_score)
                results.append(self.KNOWN)
            else:
                results.append(self.UNKNOWN)
                # print("no matches")
        return results


    ## TODO: the main logic in this method is the same as test_sm_threshold. Try to generalize it and deduplicate code.
    def test_fuzzy_threshold (self, adrf_classified_minhash, lsh_ensemble, rc_corpus, fuzzy_threshold):
        print("******** Fuzzy matcher threshold", fuzzy_threshold, "*******")
        # iterate the adrf_dataset_list, but only test the text matcher with the cases present on the test_vector
        results = list()
        for key, values in adrf_classified_minhash.items():

            m1 = values["min_hash"]
            set1 = values["words"]
            matches = False
            # this forces that any match will have at least the SM_threshold
            max_score = fuzzy_threshold

            # search the adrf dataset title in the LSH index and for potential hits
            for rc_dataset_id in lsh_ensemble.query(m1, len(set1)):
                # print(rc_dataset_id, rc_corpus[rc_dataset_id]["title"])
                ratio = fuzz.token_sort_ratio(rc_corpus[rc_dataset_id]["title"], values["title"])
                # select the best match
                if ratio >= max_score:
                    best_match = rc_dataset_id
                    max_score = ratio
                    matches = True

            if matches:
                if DEBUG:
                    print("Searching for", values["title"])
                    print("matches with", best_match, rc_corpus[best_match]["title"])
                    print("with a Fuzzy matcher ratio", max_score)
                results.append(self.KNOWN)
            else:
                results.append(self.UNKNOWN)
                # print("no matches")
        return results


    def calibrate_SequenceMatcher (self, lsh_ensemble, adrf_classified_minhash, rc_corpus, test_vector):

        max_precision_score = 0
        calibration_metrics = dict()
        selected_sm_threshold = 0

        for step in range(50, 100, 1):

            sequenceMatcher_threshold = step / 100

            results = self.test_sm_threshold(adrf_classified_minhash, lsh_ensemble, rc_corpus,
                                             sequenceMatcher_threshold)

            scores = self.get_confusion_matrix_scores(test_vector, results)

            print('confusion matrix for ' + str(sequenceMatcher_threshold))
            # print("\tTP: " + str(tp) + "\tFP: " + str(fp))
            # print("\tFN: " + str(fn) + "\tTN: " + str(tn))
            pprint(scores["confusion_matrix"])

            calibration_metrics[sequenceMatcher_threshold] = scores

            if scores["precision_score"] > max_precision_score:
                selected_sm_threshold = sequenceMatcher_threshold
                max_precision_score = scores["precision_score"]

        if DEBUG:
            print("\nshowing all metrics...")
            pprint(calibration_metrics)

        print("Selected threshold:", selected_sm_threshold)
        pprint(calibration_metrics[selected_sm_threshold])

        return selected_sm_threshold


    ## TODO: the logic in this method is the same as calibrate_SequenceMatcher. Try to generalize it and deduplicate code.
    def calibrate_FuzzyWuzzy (self, lsh_ensemble, adrf_classified_minhash, rc_corpus, test_vector):

        max_precision_score = 0
        calibration_metrics = dict()
        selected_fuzzy_threshold = 0

        for step in range(50, 80, 1):

            fuzzy_threshold = step  # / 100 #fuzzy ratio is 1 to 100

            results = self.test_fuzzy_threshold(adrf_classified_minhash, lsh_ensemble, rc_corpus, fuzzy_threshold)

            scores = self.get_confusion_matrix_scores(test_vector, results)

            print('confusion matrix for ' + str(fuzzy_threshold))

            pprint(scores["confusion_matrix"])

            calibration_metrics[fuzzy_threshold] = scores

            if scores["precision_score"] > max_precision_score:
                selected_fuzzy_threshold = fuzzy_threshold
                max_precision_score = scores["precision_score"]

        if DEBUG:
            print("\nshowing all metrics...")
            pprint(calibration_metrics)

        print("Selected threshold:", selected_fuzzy_threshold)
        pprint(calibration_metrics[selected_fuzzy_threshold])

        return selected_fuzzy_threshold


    def record_linking_sm (self, adrf_dataset_list, rc_corpus, lsh_ensemble, sm_min_score):
        # this is for measuring the time this method takes to do the record linkage
        t0 = time.time()

        # create a MinHash for each adrf dataset title
        result_list = list()

        # dataframe to export results to a CSV
        resultDF = pd.DataFrame(
            columns=['RC_id', 'RC_title', 'ADRF_id', 'ADRF_title', 'RC_description', 'ADRF_description',
                     "RC_provider_id", "ADRF_provider_id"])

        for adrf_dataset in adrf_dataset_list:

            matches = False

            adrf_id = adrf_dataset["fields"]["dataset_id"]
            title = adrf_dataset["fields"]["title"]
            words = self.get_set_of_words(adrf_dataset["fields"]["title"])

            mh = MinHash(num_perm=128)
            for term in words:
                mh.update(term.encode("utf8"))

            max_score = sm_min_score
            for rc_dataset_id in lsh_ensemble.query(mh, len(words)):
                # print(rc_dataset_id, rc_corpus[rc_dataset_id]["title"])
                s = SequenceMatcher(None, rc_corpus[rc_dataset_id]["title"], title)
                # select the best match
                if (s.ratio() >= max_score):
                    best_match = rc_dataset_id
                    max_score = s.ratio()
                    matches = True

            if matches:
                # if DEBUG:
                #     print("Searching for", values["title"])
                #     print("matches with", best_match, rc_corpus[best_match]["title"])
                #     print("with a SequenceMatcher ratio", max_score)
                adrf_match = dict()
                adrf_match["adrf_id"] = adrf_id
                adrf_match["title"] = title
                adrf_match["url"] = adrf_dataset["fields"]["source_url"]
                adrf_match["description"] = adrf_dataset["fields"]["description"]
                adrf_match["adrf_provider_id"] = adrf_dataset["fields"]["data_provider"]

                rc_match = dict()
                rc_match["dataset_id"] = best_match
                rc_match["title"] = rc_corpus[best_match]["title"]

                if "url" in rc_corpus[best_match]:
                    rc_match["url"] = rc_corpus[best_match]["url"]

                if "description" in rc_corpus[best_match]:
                    rc_match["description"] = rc_corpus[best_match]["description"]

                rc_match["rc_provider_id"] = rc_corpus[best_match]["provider_id"]

                result_list.append(adrf_match)
                result_list.append(rc_match)

                resultDF = resultDF.append(
                    {
                        'RC_id': rc_match["dataset_id"],
                        'RC_title': rc_match["title"],
                        'ADRF_id': adrf_match["adrf_id"],
                        'ADRF_title': adrf_match["title"],
                        'RC_description': rc_match.get("description"),
                        'ADRF_description': adrf_match.get("description"),
                        'RC_provider_id': rc_match.get("rc_provider_id"),
                        'ADRF_provider_id': adrf_match.get("adrf_provider_id")
                    }, ignore_index=True)

        timing = (time.time() - t0) * 1000.0
        # I left the export to files out of the time measurement on purpose

        # write csv file
        self.export_linkages_to_csv(resultDF, "matched_datasets_SequenceMatcher.csv")

        # write json file
        out_path = "matched_datasets_SequenceMatcher.json"
        with codecs.open(Path(out_path), "wb", encoding="utf8") as f:
            json.dump(result_list, f, indent=4, sort_keys=True, ensure_ascii=False)

        print(len(result_list) / 2, "matched datasets")

        return timing


    def record_linking_fuzzy (self, adrf_dataset_list, rc_corpus, lsh_ensemble, fuzzy_min_score):
        # this is for measuring the time this method takes to do the record linkage
        t0 = time.time()

        # create a MinHash for each adrf dataset title
        result_list = list()

        # dataframe to export results to a CSV
        resultDF = pd.DataFrame(
            columns=['RC_id', 'RC_title', 'ADRF_id', 'ADRF_title', 'RC_description', 'ADRF_description',
                     "RC_provider_id", "ADRF_provider_id"])

        for adrf_dataset in adrf_dataset_list:

            matches = False

            adrf_id = adrf_dataset["fields"]["dataset_id"]
            title = adrf_dataset["fields"]["title"]
            words = self.get_set_of_words(adrf_dataset["fields"]["title"])

            mh = MinHash(num_perm=128)
            for term in words:
                mh.update(term.encode("utf8"))

            max_score = fuzzy_min_score
            for rc_dataset_id in lsh_ensemble.query(mh, len(words)):
                # print(rc_dataset_id, rc_corpus[rc_dataset_id]["title"])
                ratio = fuzz.token_sort_ratio(rc_corpus[rc_dataset_id]["title"], title)
                # select the best match
                if ratio >= max_score:
                    best_match = rc_dataset_id
                    max_score = ratio
                    matches = True

            if matches:

                adrf_match = dict()
                adrf_match["adrf_id"] = adrf_id
                adrf_match["title"] = title
                adrf_match["url"] = adrf_dataset["fields"]["source_url"]
                adrf_match["description"] = adrf_dataset["fields"]["description"]
                adrf_match["adrf_provider_id"] = adrf_dataset["fields"]["data_provider"]

                rc_match = dict()
                rc_match["dataset_id"] = best_match
                rc_match["title"] = rc_corpus[best_match]["title"]

                if "url" in rc_corpus[best_match]:
                    rc_match["url"] = rc_corpus[best_match]["url"]

                if "description" in rc_corpus[best_match]:
                    rc_match["description"] = rc_corpus[best_match]["description"]

                rc_match["rc_provider_id"] = rc_corpus[best_match]["provider_id"]

                result_list.append(adrf_match)
                result_list.append(rc_match)

                resultDF = resultDF.append(
                    {
                        'RC_id': rc_match["dataset_id"],
                        'RC_title': rc_match["title"],
                        'ADRF_id': adrf_match["adrf_id"],
                        'ADRF_title': adrf_match["title"],
                        'RC_description': rc_match.get("description"),
                        'ADRF_description': adrf_match.get("description"),
                        'RC_provider_id': rc_match.get("rc_provider_id"),
                        'ADRF_provider_id': adrf_match.get("adrf_provider_id")
                    }, ignore_index=True)

        timing = (time.time() - t0) * 1000.0
        # I left the export to files out of the time measurement on purpose

        self.export_linkages_to_csv(resultDF, "matched_datasets_fuzzy.csv")

        # write json file
        out_path = "matched_datasets_fuzzy.json"
        with codecs.open(Path(out_path), "wb", encoding="utf8") as f:
            json.dump(result_list, f, indent=4, sort_keys=True, ensure_ascii=False)

        print(len(result_list) / 2, "matched datasets")

        return timing


## ***************************************************************************

class RCPublicationTitleMatcher(RCTitleMatcher):

    def load_classified_vector (self, classified_vector_path, rc_corpus):

        vector = list()
        classified = list()

        vectorDF = pd.read_csv(classified_vector_path, encoding="utf8", sep=";")

        # search in the RC corpus the publication in the classified vector
        for index, row in vectorDF.iterrows():
            if row["doi"] in rc_corpus.keys():
                print("found actual", row["actual_title"])
                print("found wrong ", row["wrong_title"])
                print("corpus",rc_corpus[row["doi"]]["title"])
                vector.append(self.KNOWN)
            else:
                print("not found", row["actual_title"])
                print("not found", row["wrong_title"])
                vector.append(self.UNKNOWN)

            classified.append(row)

        return vector, classified


    def load_corpus (self):
        """
       Load publications from knowledge graph. Only DOI and Title fields
       """
        publication_list = list()

        graph = rc_graph.RCGraph("corpus")

        # Load all publications from RCGraph
        pubs_path = Path("../",graph.BUCKET_FINAL)
        for partition, pub_iter in graph.iter_publications(path=pubs_path):
            publication_list.extend(pub_iter)

        print("loaded RC publication corpus...", type(publication_list), len(publication_list))

        # fields I need in the rc_corpus list
        #fields = {} #right now I don't use other fields than title and doi

        # create a MinHash for each dataset title and a structure to access title and its set of unique words
        rc_corpus = self.generate_minhash(entities_list=publication_list, target_text_field="title",
                                          id_field="doi")

        return rc_corpus


## ***************************************************************************
def main_dataset (corpus_path, search_for_matches_path, classified_vector_path):

    textMatcher = RCDatasetTitleMatcher()

    # Load all dataset adrf_ids and titles from ADRF dump
    with codecs.open(search_for_matches_path, "r", encoding="utf8") as f:
        adrf_dataset_list = json.load(f)

    print("loaded ADRF dataset corpus...", type(adrf_dataset_list), len(adrf_dataset_list))

    test_vector, classified_ids = textMatcher.load_classified_vector(classified_vector_path, adrf_dataset_list)
    print("loaded clasiffied data from", classified_vector_path, "|", len(test_vector), "data points")

    print("creating MinHash for each RC dataset...")

    # load RC dataset corpus and create a MinHash for each dataset title
    rc_corpus = textMatcher.load_corpus(corpus_path)

    # create a MinHash for each classified adrf dataset title
    # adrf_classified_minhash = textMatcher.generate_minhash_search_list(classified_ids, adrf_dataset_list)

    adrf_classified_datasets = list()
    for adrf_dataset in adrf_dataset_list:
        adrf_id = adrf_dataset["fields"]["dataset_id"]
        # skip the datasets not used in the test_vector
        if adrf_id in classified_ids:
            d = dict()
            d["adrf_id"] = adrf_id
            d["title"] = adrf_dataset["fields"]["title"]
            adrf_classified_datasets.append(d)

    adrf_classified_minhash = textMatcher.generate_minhash(entities_list=adrf_classified_datasets, target_text_field="title",
                                                           id_field="adrf_id")

    if CALIBRATE_LSH:
        print("***starting LSH Ensemble threshold calibration***")
        lsh_threshoild = textMatcher.calibrate_lsh_threshold(adrf_classified_minhash, rc_corpus, test_vector)
    else:
        lsh_threshoild = LSH_THRESHOLD

    lsh_ensemble = textMatcher.create_lsh_ensemble(lsh_threshoild, rc_corpus)

    if CALIBRATE_SEQUENCEMATCHER:
        print("***starting SequenceMatcher threshold calibration***")
        sm_min_score = textMatcher.calibrate_SequenceMatcher(lsh_ensemble, adrf_classified_minhash, rc_corpus, test_vector)
    else:
        sm_min_score = SEQUENCEMATCHER_THRESHOLD

    print("selected threshold for SequenceMatcher:", sm_min_score)

    timing_sm = textMatcher.record_linking_sm(adrf_dataset_list, rc_corpus, lsh_ensemble, sm_min_score)
    #
    if CALIBRATE_FUZZYWUZZY:
        print("***starting FuzzyWuzzy threshold calibration***")
        fuzzy_min_score = textMatcher.calibrate_FuzzyWuzzy(lsh_ensemble, adrf_classified_minhash, rc_corpus, test_vector)
    else:
        fuzzy_min_score = FUZZYWUZZY_THRESHOLD
    #
    print("selected threshold for FuzzyWuzzy:", fuzzy_min_score)

    timing_fw = textMatcher.record_linking_fuzzy(adrf_dataset_list, rc_corpus, lsh_ensemble, fuzzy_min_score)

    print('SequenceMatcher timing:', timing_sm)
    print('FuzzyWuzzy timing:', timing_fw)


def main_publications(classified_vector_path):

    ## TODO
    ##
        # create a small classified vector manually. Later on make a search
        # Open a classified vector
        #   Calibrate LSH
        # Calibrate SequenceMatcher
        # Calibrate FuzzyWuzzy
        # Use it with new publications

    textMatcher = RCPublicationTitleMatcher()

    rc_corpus = textMatcher.load_corpus()

    # Load training vector
    test_vector, classified = textMatcher.load_classified_vector(classified_vector_path,rc_corpus)

    # generate a minhash for each publication title to search
    pub_classified_minhash = textMatcher.generate_minhash(classified,"actual_title","doi")

    if CALIBRATE_LSH:
        print("***starting LSH Ensemble threshold calibration***")
        lsh_threshoild = textMatcher.calibrate_lsh_threshold(pub_classified_minhash, rc_corpus, test_vector)
    else:
        lsh_threshoild = LSH_THRESHOLD

    print(lsh_threshoild)

    return

if __name__ == '__main__':
    # Enforcing only 2 parameters.
    # if(len(sys.argv[1:]) != 2):
    #     print("Only 2 parameters allowed")
    #     exit(1)
    # corpus_path = sys.argv[1]
    # search_for_matches_path = sys.argv[2]

    # TODO using a temporal copy of datsets.json instead the most updated version˚
    corpus_path = RC_DATASET_JSON_PATH
    search_for_matches_path = "adrf_data/datasets-02-11-2020.json"

    # TODO: classified vector is probably biased. It does not cover any edge case.
    classified_vector_path = "training_vector_1.01.csv"

    #main_dataset(corpus_path, search_for_matches_path, classified_vector_path)

    classified_vector_path = Path("publications_data/training_vector_1.0.csv")

    main_publications(classified_vector_path)