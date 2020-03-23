
from richcontext import graph as rc_graph
import re
from pathlib import Path
import os
import pandas as pd
import json
import numpy

PATH_DATADROPS = Path("richcontextmetadata/metadata")
PATH_DATASETS = Path("datasets")
DEBUG = False

DATADROP_SIZE_UPPER_LIMIT = 75 #Datadrops bigger than this are unlikely to be fully processed
RATIO_LOWER_LIMIT = 0.15 # ratio defined as partition size / datadrop size. If is too low, it is probably that the reviewer did not process the whole datadrop


def normalize_fields(datadropDF):
    # unify different column name for valid links
    replacements = {
        "valid": ["valid", "valid?", "Validation", "validation", "dataset_used?","Used dataset?"],
        "dataset": ["dataset", "datasets", "dataset_id"]
    }
    datadropDF.columns = datadropDF.columns.str.strip()
    datadropDF.rename(columns={el: k for k, v in replacements.items() for el in v}, inplace=True)
    #datadropDF["dataset"] = datadropDF["dataset"].apply(str)

    return datadropDF


#copied this function from publications_export_template.py and edited it
def create_pub_dict(linkages_dataframe):

    # Cleaning the Dataframe
    linkages_dataframe = linkages_dataframe.loc[pd.notnull(linkages_dataframe.dataset)].drop_duplicates() #prevent null values for datasets in some datadrop files.
    #linkages_dataframe = linkages_dataframe.loc[pd.notnull(linkages_dataframe.title)].drop_duplicates()


    # Get Datasets Information
    datasets_file_path = PATH_DATASETS / "datasets.json"
    with open(datasets_file_path, encoding="utf-8") as json_file:
        datasets = json.load(json_file)

    pub_metadata_fields = ["title"]
    original_metadata_cols = list(
        set(linkages_dataframe.columns.values.tolist()) - set(pub_metadata_fields) - set(["dataset"]))

    pub_dict_list = []
    for i, r in linkages_dataframe.iterrows():
        # r["title"] = scrub_unicode(r["title"])
        ds_id_list = [f for f in [d.strip() for d in r["dataset"].split(",")] if f not in ["", " "]]
        for ds in ds_id_list:
            check_ds = [b for b in datasets if b["id"] == ds]
            if len(check_ds) == 0:
                print("dataset {} isnt listed in datasets.json. Please add to file".format(ds))
        required_metadata = r[pub_metadata_fields].to_dict()
        required_metadata.update({"datasets": ds_id_list})
        pub_dict = required_metadata
        if len(original_metadata_cols) > 0:
            original_metadata_raw = r[original_metadata_cols].to_dict()
            # original_metadata_raw.update({"date_added": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            original_metadata = {k: v for k, v in original_metadata_raw.items() if not pd.isnull(v)}
            pub_dict.update({"original": original_metadata})
        pub_dict_list.append(pub_dict)
    return pub_dict_list

def recover_verified_not_links(filename, publications):

    pubs_classified_as_not_valid = False

    try:
        datadropDF = pd.read_csv(filename, encoding="utf-8")
    except Exception as e:
        # debug this as an edge case
        print("exception while trying to open",filename,str(e))
        return

    #print(datadropDF.head()) #TODO: describe column valid if exists

    if "valid" in datadropDF.columns:
        print("valid unique values:",datadropDF.valid.nunique())
    else:
        print("valid column not present")

    ratio = len(publications) / len(datadropDF)
    print("partition size", len(publications),"| datadrop size", len(datadropDF),\
          "valid/total ratio", str(ratio), "process:", ratio *100 > 12.5 and ratio *100 != 100)

    #filter out datadrops

    if len(datadropDF) > DATADROP_SIZE_UPPER_LIMIT: # Datadrops too big
        return None
    elif len(publications) == len(datadropDF): # cvs file most probably contains only valid links
        return None
    elif ratio < RATIO_LOWER_LIMIT:
        return None
    else:
        return create_pub_dict(  normalize_fields(datadropDF))



def select_datadrop_file(datadrop_directory):
    ## criteria for selecting best CSV file:
        #1. column "valid" with the most unique values (to make sure not valids are identified)
        #2. longest file (to skip the files that only have valid links)
        #3. file with the shortest filename (usually -but not always- the original datadrop)

    ##TODO: change criteria, it is prioritizing final vs original, related to how datadropDF.valid.nunique() is evaluated
        ##20200302_federated2_USDA_AgriculturalResourceManagementSurvey_part5_final.csv

    if DEBUG: #debug
        print("***********select datadrop file***********")
        shortest_filename = None
        datadrop_with_most_classes = None
        max_classes = 0
        longest_datadrop = None
        datadrop_max_row_count = 0

    auxDF = pd.DataFrame(columns=["filename","filename_lenght","valid_nunique","cant_rows"])

    for filename in os.listdir(datadrop_directory):

        if not filename.endswith(".csv"):
            continue
        print("analyzing file",filename)

        try:
            datadropDF = pd.read_csv(datadrop_directory / filename, encoding="utf-8")
            #print(datadropDF.head())

            datadropDF = normalize_fields(datadropDF)

            if "dataset" not in datadropDF.columns:
                print("dataset field not included in CSV file:",datadropDF.columns)
                continue
            elif datadropDF["dataset"].dtypes != numpy.object:
                print("dataset field value types incorrect:", datadropDF["dataset"].dtypes)
                continue


            if DEBUG: # deguggin
                if "valid" not in datadropDF.columns:
                    print("after column rename:", datadropDF.columns)

                if shortest_filename == None or len(filename) < len(shortest_filename):
                    shortest_filename = filename

                if len(datadropDF) > datadrop_max_row_count:
                    datadrop_max_row_count = len(datadropDF)
                    longest_datadrop = filename

            if "valid" in datadropDF.columns:
                nunique_valid_values = datadropDF.valid.nunique()

                if DEBUG:
                    if datadropDF.valid.nunique() > max_classes :
                        max_classes = datadropDF.valid.nunique()
                        datadrop_with_most_classes = filename
            else:
                nunique_valid_values = 1

            auxDF = auxDF.append(
                {
                    "filename":filename,
                    "filename_lenght":len(filename),
                    "valid_nunique": nunique_valid_values,
                    "cant_rows": len(datadropDF)
                }, ignore_index=True)

        except Exception as e:
            # debug this as an edge case
            print("exception while trying to open", filename, str(e))
            continue


    #print("shortest filename:", shortest_filename, "| Filename with more information:", datadrop_with_most_classes)

    if DEBUG: #debug
        print("most 'valid' classes:",datadrop_with_most_classes)
        print("longest datadrop:",longest_datadrop)
        print("shortest filename:",shortest_filename)

    if len(auxDF) > 0:

        auxDF = auxDF.sort_values(["valid_nunique", "cant_rows","filename_lenght"], ascending=(False, False, True))
        return auxDF.iloc[0]["filename"]
    else:
        return None


def main():
    ##TODO list:
        #process datadrop if there is a partition (since not all datadrops where reviewed).-DONE
        #open the original datadrop (and not the final or intermediate csv).- NEW SELECTION CRITERIA DONE
        #validate that the original datadrop has more publications than the partition
        #filter out datadrops too big to be sure that where fully processed
        #validate that the partition publications exists in the original datadrop
        #be careful with the USDA ARMS datadrops




    ## for each partition, check if there is a metadata folder with a matching name

    graph = rc_graph.RCGraph()
    cant_partitions =0
    cant_dirs = 0
    cant_files = 0
    cant_shadow_partition =0

    for partition_name, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS):
        cant_partitions += 1


        datadrop_directory = re.sub(".json$", "", partition_name)

        if datadrop_directory.endswith("_publications"):
            datadrop_directory = re.sub("_publications$", "", datadrop_directory)

        else:
            print("not all partitions follows the name convention:",partition_name)

        print("partition:",datadrop_directory)

        # if datadrop_directory != "20191119_LongitudinalEmployer-HouseholdDynamicsOrigin-DestinationEmploymentStatistics":
        #     continue

        #check if the partition filename matches with a datadrop directory
        if (os.path.isdir(PATH_DATADROPS / datadrop_directory )):
            cant_dirs += 1

            datadrop_filename = select_datadrop_file(PATH_DATADROPS / datadrop_directory)
            print("selected file: "+ str(datadrop_filename))


            if datadrop_filename:
                shadow_partition = recover_verified_not_links(PATH_DATADROPS / datadrop_directory / datadrop_filename, pub_iter)
                cant_files += 1
                if shadow_partition:
                    cant_shadow_partition += 1

        else:
            print(PATH_DATADROPS / datadrop_directory, "does not exists")


    print("existing partitions",cant_partitions)
    print("matching directories by name",cant_dirs)
    print("matching files by name",cant_files)
    print("shadow partition",cant_shadow_partition)




if __name__ == "__main__":

    main()
