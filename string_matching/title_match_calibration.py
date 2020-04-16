import re
import codecs
import json
import time
import traceback
from difflib import SequenceMatcher
from pprint import pprint
import numpy
from fuzzywuzzy import fuzz
from datasketch import MinHashLSHEnsemble, MinHash
from sklearn import metrics
from pathlib import Path
import pandas as pd
from richcontext import graph as rc_graph
from richcontext import scholapi as rc_scholapi
import pickle

CALIBRATE_LSH = True
LSH_THRESHOLD = 0.79  # Required when CALIBRATE_LSH == False
#LSH_THRESHOLD = 0.85  # Required when CALIBRATE_LSH == False

CALIBRATE_SEQUENCEMATCHER = True
SEQUENCEMATCHER_THRESHOLD = 0.55  # Required when CALIBRATE_SEQUENCEMATCHER == False

#SEQUENCEMATCHER_THRESHOLD = 0.5  # Required when CALIBRATE_SEQUENCEMATCHER == False

CALIBRATE_FUZZYWUZZY = True
FUZZYWUZZY_THRESHOLD = 54  # Required when CALIBRATE_SEQUENCEMATCHER == False

DEBUG = False

ADRF_PROVIDERS_JSON_PATH = "adrf_data/2020_03_25/dataset_providers_03_25_2020.json"
RC_PROVIDERS_JSON_PATH = "../datasets/providers.json"

ADRF_DATASET_JSON_PATH = "adrf_data/2020_03_25/datasets_03_25_2020.json"
RC_DATASET_JSON_PATH = "../datasets/datasets.json"

class RCTitleMatcher:

# TODO: include a method to clean strings before doing the match evaluations


    def __init__(self,sm_threshold = None,fuzzy_threshold = None,lsh_threshold = None, lsh_ensemble = None, rc_corpus = None):
        self.sm_threshold = sm_threshold
        self.fuzzy_threshold = fuzzy_threshold
        self.lsh_threshold = lsh_threshold
        self.lsh_ensemble = lsh_ensemble
        self.rc_corpus = rc_corpus
        self.UNKNOWN = 0
        self.KNOWN = 1


    # use pickle to persist the calibrated thresholds and other object attributes
    def save_model(self, filename="RCTitleMatcher.pkl"):
        outfile = open(filename, 'wb')
        pickle.dump(self, outfile)
        outfile.close()

    # retunrs an object
    def load_model(self = None, filename="RCTitleMatcher.pkl"):
        infile = open(filename, 'rb')
        tm = pickle.load(infile)
        infile.close()
        return tm


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


    def generate_minhash (self, entities_list, target_text_field, id_field, other_fields = {}, is_corpus = False):

    # TODO: not sure if this is the best design, but I need a dict() for the corpus that is going to be used for the LSH and I need a list() for the rest
        if is_corpus:
            entities_list_with_minhash = dict()
        else:
            entities_list_with_minhash = list()

        for entity in entities_list:

            # id_field is required. If not present, the entity is skipped from the corpus list
            if is_corpus and id_field not in entity:
                print("entity does not have field",id_field)
                continue

            d = dict()
            # d["id"] = entity[id_field] (never used for now)
            d["text_to_match"] = entity[target_text_field]
            d["words"] = self.get_set_of_words(entity[target_text_field])

            for field_original_name, field_display_name in other_fields.items():
                if field_original_name in entity:
                    d[field_display_name] = entity[field_original_name]

            mh = MinHash(num_perm=128)
            for term in d["words"]:
                mh.update(term.encode("utf8"))
            d["min_hash"] = mh

            if is_corpus:
                entities_list_with_minhash[entity[id_field]] = d
            else:
                entities_list_with_minhash.append(d)

        return entities_list_with_minhash


    def create_lsh_ensemble (self, lsh_threshold):
        print("creating MinHashLSHEnsemble with threshold=%s, num_perm=128, num_part=16..." % lsh_threshold)
        # Create an LSH Ensemble index with threshold and number of partition settings.
        lshensemble = MinHashLSHEnsemble(threshold=lsh_threshold, num_perm=128, num_part=16)
        print("indexing all RC dataset's MinHash...")
        # Index takes an iterable of (key, minhash, size)
        lshensemble.index([(key, values["min_hash"], len(values["words"])) for key, values in self.rc_corpus.items()])

        self.lsh_ensemble = lshensemble


    def test_lsh_threshold (self, classified_minhash, lsh_threshold):

        self.create_lsh_ensemble(lsh_threshold)

        # test by querying the LSH Ensemble with each classified entity title to explore potential matches
        results = list()
        for values in classified_minhash:
            m1 = values["min_hash"]
            set1 = values["words"]
            # print("\nquery for '%s' yields datasets" % adrf_dataset["fields"]["title"])
            matches = False
            for key in self.lsh_ensemble.query(m1, len(set1)):
                # print(key, rc_corpus[key]["title"])
                matches = True
                break
            if matches:
                results.append(self.KNOWN)
            else:
                results.append(self.UNKNOWN)
                # print("no matches")

        return results


    def calibrate_lsh_threshold (self, classified_minhash, test_vector):

        # steps for the grid search of the LSH threshold
        steps = numpy.arange(60, 100, 5, int)

        # doing this instead of generating a range using float to prevent having steps like 0.990000000001
        steps = steps / 100

        selected_lsh_threshold = self.calibrate_generic(classified_minhash, test_vector,"f1_score",
                               self.test_lsh_threshold, steps)

        self.lsh_threshold = selected_lsh_threshold

        return selected_lsh_threshold


    def test_sm_threshold (self, classified_minhash, sequenceMatcher_threshold):
        print("******** SequenceMatcher threshold", sequenceMatcher_threshold, "*******")

        results = list()
        for values in classified_minhash:

            m1 = values["min_hash"]
            set1 = values["words"]

            best_match, matches, max_score = self.find_text_in_corpus_sm(m1, set1, values["text_to_match"],
                                                                         sequenceMatcher_threshold)

            if matches:
                if DEBUG:
                    print("Searching for", values["text_to_match"])
                    print("matches with", best_match, self.rc_corpus[best_match]["text_to_match"])
                    print("with a SequenceMatcher ratio", max_score)
                results.append(self.KNOWN)
            else:
                results.append(self.UNKNOWN)
                # print("no matches")
        return results


    def find_text_in_corpus_sm (self, m1, set1, text_to_match, sequenceMatcher_threshold):

        matches = False
        best_match = None
        # this forces that any match will have at least the SM_threshold
        max_score = sequenceMatcher_threshold
        # search the adrf dataset title in the LSH index and for potential hits
        for entity_id in self.lsh_ensemble.query(m1, len(set1)):
            # print(entity_id, rc_corpus[entity_id]["title"])
            # TODO: "text_to_match" is "title" but hardcoding "title" was be a bug. See if makes sense to have both "text_to_match" and "title"
            s = SequenceMatcher(None, self.rc_corpus[entity_id]["text_to_match"], text_to_match )
            # select the best match
            if (s.ratio() >= max_score):
                best_match = entity_id
                max_score = s.ratio()
                matches = True

        return best_match, matches, max_score

    # TODO: make some kind of overloading for this
    # def find_text_in_corpus_sm (self, search_for, text_to_match, sequenceMatcher_threshold, lsh_ensemble, rc_corpus):
    #
    #     words = self.get_set_of_words(search_for)
    #
    #     mh = MinHash(num_perm=128)
    #     for term in words:
    #         mh.update(term.encode("utf8"))
    #
    #     best_match, matches, max_score =  self.find_text_in_corpus_sm(mh, words, text_to_match, sequenceMatcher_threshold, lsh_ensemble, rc_corpus)
    #
    #     return best_match, matches, max_score


    def find_text_in_corpus_fuzzy (self, mh, words, text_to_match, fuzzy_min_score):

        matches = False
        best_match = None
        max_score = fuzzy_min_score
        for entity_id in self.lsh_ensemble.query(mh, len(words)):
            # print(entity_id, rc_corpus[entity_id]["title"])
            corpus_potential_match_text = self.rc_corpus[entity_id]["text_to_match"]
            ratio = fuzz.token_sort_ratio(corpus_potential_match_text, text_to_match)
            # select the best match
            if ratio >= max_score:
                best_match = entity_id
                max_score = ratio
                matches = True

        return best_match, matches, max_score

    def calibrate_generic(self, classified_minhash, test_vector,confusion_matrix_target_score,
                          test_tool_threshold,steps):

        implemented_confusion_matrix_scores = ["TN", "FP", "FN", "TP", "confusion_matrix", "accuracy_score",
                                               "recall_score",
                                               "precision_score", "f1_score", "specificity_score",
                                               "False Positive Rate or Type I Error",
                                               "False Negative Rate or Type II Error"]
        if confusion_matrix_target_score not in implemented_confusion_matrix_scores:
            raise ValueError("confusion_matrix_target_score: received", confusion_matrix_target_score,
                             "but expected one of the folowing:", implemented_confusion_matrix_scores)

        max_cm_score = 0
        calibration_metrics = dict()
        selected_threshold = 0

        for step in steps:

            threshold = step

            results = test_tool_threshold(classified_minhash,
                                             threshold)

            scores = self.get_confusion_matrix_scores(test_vector, results)

            print('confusion matrix for ' + str(threshold))

            pprint(scores["confusion_matrix"])

            calibration_metrics[threshold] = scores

            if scores[confusion_matrix_target_score] > max_cm_score:
                selected_threshold = threshold
                max_cm_score = scores[confusion_matrix_target_score]

        if DEBUG:
            print("\nshowing all metrics...")
            pprint(calibration_metrics)

        pprint(calibration_metrics)

        print("Selected threshold:", selected_threshold)
        pprint(calibration_metrics[selected_threshold])

        return selected_threshold

    def calibrate_SequenceMatcher (self, classified_minhash, test_vector,confusion_matrix_target_score):

        # steps for the grid search of the SequenceMatcher threshold
        steps = numpy.arange(50,100,1, int)

        # doing this instead of generating a range using float to prevent having steps like 0.990000000001
        steps = steps / 100

        # calibrate_generic will iterate through each step in steps and select the threshold that maximizes the
        # confusion_matrix_target_score calling test_sm_threshold method -specific to SequenceMatcher- for each step
        selected_sm_threshold = self.calibrate_generic(classified_minhash, test_vector,
                                                       confusion_matrix_target_score,self.test_sm_threshold, steps)

        # set the internal SM threshold attribute
        self.sm_threshold = selected_sm_threshold

        return selected_sm_threshold


    def set_sm_threshold (self, sm_threshold):
        # set the internal SM threshold attribute
        self.sm_threshold = sm_threshold

    def set_fuzzy_threshold (self, fuzzy_threshold):
        # set the internal Fuzzy threshold attribute
        self.fuzzy_threshold = fuzzy_threshold


## TODO: the main logic in this method is the same as test_sm_threshold. Try to generalize it and deduplicate code.
    def test_fuzzy_threshold (self, classified_minhash, fuzzy_threshold):
        print("******** Fuzzy matcher threshold", fuzzy_threshold, "*******")

        results = list()
        for values in classified_minhash:

            m1 = values["min_hash"]
            set1 = values["words"]

            best_match, matches, max_score = self.find_text_in_corpus_fuzzy(m1, set1, values["text_to_match"],
                                                                            fuzzy_threshold)

            if matches:
                if DEBUG:
                    print("Searching for", values["text_to_match"])
                    print("matches with", best_match, self.rc_corpus[best_match]["text_to_match"])
                    print("with a Fuzzy matcher ratio", max_score)
                results.append(self.KNOWN)
            else:
                results.append(self.UNKNOWN)
                # print("no matches")
        return results


    def calibrate_FuzzyWuzzy (self, classified_minhash, test_vector,confusion_matrix_target_score):

        steps = numpy.arange(50, 100, 1)

        # calibrate_generic will iterate through each step in steps and select the threshold that maximizes the
        # confusion_matrix_target_score calling self.test_fuzzy_threshold
        # method -specific to FuzzyWuzzy matcher- for each step
        selected_fuzzy_threshold = self.calibrate_generic( classified_minhash, test_vector,
                                                       confusion_matrix_target_score,
                                                       self.test_fuzzy_threshold, steps)

        # set the internal Fuzzy threshold attribute
        self.fuzzy_threshold = selected_fuzzy_threshold

        return selected_fuzzy_threshold


    def sm_text_match (self, text1, text2):
        s = SequenceMatcher(None, text1, text2)

        if (s.ratio() >= self.sm_threshold):
            return True, s.ratio()
        else:
            return False, None


    def fuzzy_text_match (self, text1, text2):

        ratio = fuzz.token_sort_ratio(text1, text2)

        if ratio >= self.fuzzy_threshold:
            return True, ratio
        else:
            return False, None


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
                                          id_field="id", other_fields=fields,is_corpus=True)
        self.rc_corpus = rc_corpus


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


    def record_linking_sm (self, adrf_dataset_list, sm_min_score):
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

            best_match, matches, max_score = self.find_text_in_corpus_sm(mh, words, title, sm_min_score)

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
                rc_match["title"] = self.rc_corpus[best_match]["text_to_match"]

                if "url" in self.rc_corpus[best_match]:
                    rc_match["url"] = self.rc_corpus[best_match]["url"]

                if "description" in self.rc_corpus[best_match]:
                    rc_match["description"] = self.rc_corpus[best_match]["description"]

                rc_match["rc_provider_id"] = self.rc_corpus[best_match]["provider_id"]

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


    def record_linking_fuzzy (self, adrf_dataset_list, fuzzy_min_score):
        # this is for measuring the time this method takes to do the record linkage
        t0 = time.time()

        # create a MinHash for each adrf dataset title
        result_list = list()

        # dataframe to export results to a CSV
        resultDF = pd.DataFrame(
            columns=['RC_id', 'RC_title', 'ADRF_id', 'ADRF_title', 'RC_description', 'ADRF_description',
                     "RC_provider_id", "ADRF_provider_id"])

        for adrf_dataset in adrf_dataset_list:

            adrf_id = adrf_dataset["fields"]["dataset_id"]
            title = adrf_dataset["fields"]["title"]
            words = self.get_set_of_words(adrf_dataset["fields"]["title"])

            mh = MinHash(num_perm=128)
            for term in words:
                mh.update(term.encode("utf8"))

            best_match, matches,max_score = self.find_text_in_corpus_fuzzy(mh, words, title,fuzzy_min_score)

            if matches:

                adrf_match = dict()
                adrf_match["adrf_id"] = adrf_id
                adrf_match["title"] = title
                adrf_match["url"] = adrf_dataset["fields"]["source_url"]
                adrf_match["description"] = adrf_dataset["fields"]["description"]
                adrf_match["adrf_provider_id"] = adrf_dataset["fields"]["data_provider"]

                rc_match = dict()
                rc_match["dataset_id"] = best_match
                rc_match["title"] = self.rc_corpus[best_match]["text_to_match"]

                if "url" in self.rc_corpus[best_match]:
                    rc_match["url"] = self.rc_corpus[best_match]["url"]

                if "description" in self.rc_corpus[best_match]:
                    rc_match["description"] = self.rc_corpus[best_match]["description"]

                rc_match["rc_provider_id"] = self.rc_corpus[best_match]["provider_id"]

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

    def load_classified_vector (self, classified_vector_path):

        vector = list()
        classified = list()

        vectorDF = pd.read_csv(classified_vector_path, encoding="utf8")

        # search in the RC corpus the publication in the classified vector
        for index, row in vectorDF.iterrows():
            if row["doi"] in self.rc_corpus.keys():
                # print("found actual", row["actual_title"])
                # print("found wrong ", row["wrong_title"])
                # print("corpus",rc_corpus[row["doi"]]["text_to_match"])
                vector.append(self.KNOWN)
            else:
                # print("not found", row["actual_title"])
                # print("not found", row["wrong_title"])
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
                                          id_field="doi", is_corpus=True)

        self.rc_corpus = rc_corpus


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
    textMatcher.load_corpus(corpus_path)

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
        lsh_threshold = textMatcher.calibrate_lsh_threshold(adrf_classified_minhash, test_vector)
    else:
        lsh_threshold = LSH_THRESHOLD

    textMatcher.create_lsh_ensemble(lsh_threshold)

    if CALIBRATE_SEQUENCEMATCHER:
        print("***starting SequenceMatcher threshold calibration***")
        sm_min_score = textMatcher.calibrate_SequenceMatcher(adrf_classified_minhash, test_vector,
                                                             "precision_score")
    else:
        sm_min_score = SEQUENCEMATCHER_THRESHOLD

    print("selected threshold for SequenceMatcher:", sm_min_score)

    #
    if CALIBRATE_FUZZYWUZZY:
        print("***starting FuzzyWuzzy threshold calibration***")
        fuzzy_min_score = textMatcher.calibrate_FuzzyWuzzy( adrf_classified_minhash, test_vector,
                                                           "precision_score")
    else:
        fuzzy_min_score = FUZZYWUZZY_THRESHOLD
    #
    print("selected threshold for FuzzyWuzzy:", fuzzy_min_score)

    textMatcher.save_model()

    loadedTextMatcher = RCDatasetTitleMatcher.load_model()

    timing_sm = loadedTextMatcher.record_linking_sm(adrf_dataset_list, sm_min_score)
    timing_fw = loadedTextMatcher.record_linking_fuzzy(adrf_dataset_list, fuzzy_min_score)

    print('SequenceMatcher timing:', timing_sm)
    print('FuzzyWuzzy timing:', timing_fw)


def main_publications_calibrate(classified_vector_path):

    ## TODO
    ##
        # create a small classified vector manually. Later on make a search. DONE
        # Open a classified vector. DONE
        #   Calibrate LSH. DONE
        # Calibrate SequenceMatcher. DONE
        # Calibrate FuzzyWuzzy. DONE
        # Use it with new publications

    textMatcher = RCPublicationTitleMatcher()

    textMatcher.load_corpus()

    # Load training vector
    test_vector, classified = textMatcher.load_classified_vector(classified_vector_path)

    # generate a minhash for each publication title to search
    pub_classified_minhash = textMatcher.generate_minhash(classified,"title","doi")

    if CALIBRATE_LSH:
        print("***starting LSH Ensemble threshold calibration***")
        lsh_threshold = textMatcher.calibrate_lsh_threshold(pub_classified_minhash, test_vector)
    else:
        lsh_threshold = LSH_THRESHOLD

    print(lsh_threshold)

    textMatcher.create_lsh_ensemble(lsh_threshold)

    if CALIBRATE_SEQUENCEMATCHER:
        print("***starting SequenceMatcher threshold calibration***")
        sm_min_score = textMatcher.calibrate_SequenceMatcher(pub_classified_minhash, test_vector,
                                                             "f1_score")
    else:
        sm_min_score = SEQUENCEMATCHER_THRESHOLD

    print("selected threshold for SequenceMatcher:", sm_min_score)

    if CALIBRATE_FUZZYWUZZY:
        print("***starting FuzzyWuzzy threshold calibration***")
        fuzzy_min_score = textMatcher.calibrate_FuzzyWuzzy( pub_classified_minhash, test_vector,
                                                            "f1_score")
    else:
        fuzzy_min_score = FUZZYWUZZY_THRESHOLD
    #
    print("selected threshold for FuzzyWuzzy:", fuzzy_min_score)

    return sm_min_score, fuzzy_min_score

    print("-----------------------------------------------------")
    # Test the matcher with the training vector ##TODO test better
    vectorDF = pd.read_csv(classified_vector_path, encoding="utf8")
    for index, row in vectorDF.iterrows():

        search_for = row["title"]

        words = textMatcher.get_set_of_words(search_for)

        mh = MinHash(num_perm=128)
        for term in words:
            mh.update(term.encode("utf8"))

        best_match, matches, max_score = textMatcher.find_text_in_corpus_fuzzy(mh,words,search_for,
                                                                            sm_min_score)
        if matches:
            print("Searching for", search_for)
            print("matches with", best_match, self.rc_corpus[best_match]["text_to_match"])
            print("with a SequenceMatcher ratio", max_score)
        else:
            print("Searching for", search_for)
            print("NOT FOUND")

    return sm_min_score, fuzzy_min_score


def main_publications_test_search(sm_min_score, fuzzy_min_score):

    text1 = "Profitability of organic and conventional soybean production under ‘green payments’ in carbon offset programs"
    text2 = "Profitability of organic and conventional soybean production under 'green payments' in carbon offset programs"

    ## Use the text matcher with known calibrated threshold
    calibratedTextMatcher = RCPublicationTitleMatcher()

    calibratedTextMatcher.set_sm_threshold(sm_min_score)
    calibratedTextMatcher.set_fuzzy_threshold(fuzzy_min_score)

    sm_result,sm_score = calibratedTextMatcher.sm_text_match(text1,text2)
    fuzzy_result,fuzzy_score = calibratedTextMatcher.fuzzy_text_match(text1,text2)

    print("SM match",sm_result,sm_score)
    print("Fuzzy match",fuzzy_result, fuzzy_score)


    return


def search_publication_titles ():

    schol = rc_scholapi.ScholInfraAPI(config_file="../rc.cfg", logger=None)

    #get all APIs with publication_lookup
    api_list = []
    # get all atributes of schol object
    schol_dict = schol.__dict__
    for key, api in schol_dict.items():
        try:  # __getattribute__ raises an exception when "full_text_search" is missing
            # checks if api.full_text_search is defined and is a method.
            if callable(api.__getattribute__("publication_lookup")):
                #test if its actually implemented
                try:
                    response = api.publication_lookup("10.1016/j.envsoft.2010.05.009")
                    if response is not None:
                        test1 = response.title()
                        api_list.append(api)
                except NotImplementedError:
                    pass
        except Exception:
            # print(api.name, "does NOT implement full_text_search")
            continue

    for api in api_list:
        print(api.name)

    # Load all publications from RCGraph that have a DOI
    graph = rc_graph.RCGraph("corpus")
    pubs_path = Path("../", graph.BUCKET_FINAL)
    publications = dict()
    for partition, pub_iter in graph.iter_publications(path=pubs_path):
        for pub in pub_iter:
            # skip publications with unknown DOI
            if "doi" not in pub:
                continue
            publications[pub["doi"]] = pub["title"]

    print("loaded", len(publications), "known publications")

    retrieved_publications = list()
    count=0
    # I iterate first through the KG picking one DOI and making a search for that DOI in each API so I don't overload the API
    for doi, title in publications.items():
        print("lookup for",doi,title,"...")
        known_titles = list()
        known_titles.append(title.lower())
        for api in api_list:
            if api.name == "CORE" or api.name == "NSF PAR":
                continue
            try:
                print(api.name,"publication_lookup...")
                response = api.publication_lookup(doi)
                if response is not None and response.title() is not None and response.title().lower() not in known_titles:
                    d = dict()
                    d["api"] = api.name
                    d["doi"] = doi
                    d["title"] = response.title()
                    retrieved_publications.append(d)
                    known_titles.append(d["title"].lower())
                    count += 1
            except Exception as e:
                print("exception using publication_lookup of",api.name)
                traceback.print_exc()
        # if count > 40:
        #     break
        # wait a little bit before using again the APIs
        print("wait 1 sec, count", count)
        time.sleep(0.5)

    # save publications retrieved into a CSV file
    auxDF = pd.DataFrame(data=retrieved_publications)
    auxDF.to_csv("publications_with_title_mismatch.csv", index=False, encoding="utf-8-sig")


def load_rejected_publications():

    # get publications from the verified not-links "shadow" partitions to balance the training set
    from pandas.io.json import json_normalize

    publication_list = list()
    graph = rc_graph.RCGraph("corpus")
    # Load all publications from RCGraph
    pubs_path = Path("../", "not-links_partitions")
    for partition, pub_iter in graph.iter_publications(path=pubs_path):
        publication_list.extend(pub_iter)

    publication_list = json_normalize(publication_list)

    # save publications retrieved into a CSV file
    auxDF = pd.DataFrame(data=publication_list)
    print(auxDF.columns)
    auxDF.to_csv("not-links_publications.csv", index=False, encoding="utf-8-sig",columns=["original.doi","title"])


if __name__ == '__main__':
    #

    # TODO using a temporal copy of datsets.json instead the most updated version˚
    corpus_path = RC_DATASET_JSON_PATH
    search_for_matches_path = "adrf_data/datasets-02-11-2020.json"

    # TODO: classified vector is probably biased. It does not cover any edge case.
    classified_vector_path = "training_vector_1.01.csv"

    main_dataset(corpus_path, search_for_matches_path, classified_vector_path)

    # this uses scholapi to search publications by DOI using all possible APIs, the result was used to create the training vector 3.0
    #search_publication_titles()

    # this recover all publications produced by recover_verified_not_links.py and those were used to create the training vector 3.0
    # load_rejected_publications()

    classified_vector_path = Path("publications_data/training_vector_3.0.csv")

    #sm_min_score, fuzzy_min_score = main_publications_calibrate(classified_vector_path)

    # TODO: this just compares 2 publication titles. Maybe it makes sense to search in the entire corpus.
    #main_publications_test_search(sm_min_score, fuzzy_min_score)
