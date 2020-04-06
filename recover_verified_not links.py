
from richcontext import graph as rc_graph
import re
from pathlib import Path
import os
import pandas as pd
import json
import numpy

DEBUG = False

PATH_DATADROPS = Path("richcontextmetadata/metadata")
PATH_DATASETS = Path("datasets")

PATH_SHADOW_PARTITIONS = Path("not-links_partitions")

DATADROP_SIZE_UPPER_LIMIT = 75 #Datadrops bigger than this are unlikely to be fully processed
RATIO_LOWER_LIMIT = 0.15 # ratio defined as partition size / datadrop size. If is too low, it is probably that the reviewer did not process the whole datadrop


def normalize_fields(datadropDF):
    # unify different column name for valid links
    replacements = {
        "valid": ["valid", "valid?", "Validation", "validation", "dataset_used?","Used dataset?"],
        "dataset": ["dataset", "datasets", "dataset_id", "dataset id"]
    }
    datadropDF.columns = datadropDF.columns.str.strip()
    datadropDF.rename(columns={el: k for k, v in replacements.items() for el in v}, inplace=True)
    #datadropDF["dataset"] = datadropDF["dataset"].apply(str)

    return datadropDF

def normalize_valid_field_values(datadropDF):

    print("before replace:",datadropDF.valid.unique())

    # convert all values of "valid" column into strings and lowercase
    datadropDF["valid"] = datadropDF.valid.astype(str).str.lower()

    datadropDF.replace({'valid': {
                            "0": "no" , "n": "no" ,
                            "1": "yes", "y": "yes",
                            "2": "maybe", "review" : "maybe",
                            "dupe": "duplicated", "duped" : "duplicated",
                            "unavailable" : "no access",
                            "nan": "", " - ": "", " -": "",
    }},inplace=True)

    print("after replace:",datadropDF.valid.unique())

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

def recover_verified_not_links(filename, valid_links):

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
        normalize_valid_field_values(datadropDF)
    else:
        print("valid column not present")

    ratio = len(valid_links) / len(datadropDF)
    print("partition size", len(valid_links), "| datadrop size", len(datadropDF),\
          "valid/total ratio", str(ratio), "process:", ratio * 100 > 12.5 and ratio * 100 != 100)

    #filter out datadrops

    if len(datadropDF) > DATADROP_SIZE_UPPER_LIMIT: # Datadrops too big
        print("Datadrop too big",len(datadropDF))
        return None
    elif len(valid_links) == len(datadropDF): # cvs file most probably contains only valid links
        return None
    elif ratio < RATIO_LOWER_LIMIT:
        return None
    else:

        datadrop_links = create_pub_dict(normalize_fields(datadropDF))

        shadow_partition = list()

        for link in datadrop_links:
            link_exists_in_partition = False
            for valid_link in valid_links:
                if "doi" in link["original"] and "doi" in valid_link["original"]:
                    if link["original"]["doi"] == valid_link["original"]["doi"]:
                        link_exists_in_partition = True
                        break
                elif "title" in link and "title" in valid_link:
                    if link["title"] == valid_link["title"]:
                        link_exists_in_partition = True
                        break

            if link_exists_in_partition == False:
                shadow_partition.append(link)

        return shadow_partition



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

            # some CSV files raised an UnicoDecodeError and those can be opened using engine="python" but not the rest
            # that is why I use this nested try
            try:
                datadropDF = pd.read_csv(datadrop_directory / filename, encoding="utf-8")
            except UnicodeDecodeError as e:
                # when this exception is thrown I try one other thing that would normally fail to open the files
                print("exception",str(e))
                datadropDF = pd.read_csv(datadrop_directory / filename, engine="python")

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

            #testing case
            if "valid" not in datadropDF.columns:
                continue

            if "valid" in datadropDF.columns:
                nunique_valid_values = datadropDF.valid.nunique()

                # testing case
                if nunique_valid_values < 2:
                    continue

                # if "valid" column has 1 value typically is "yes", so I change it to -1 to penalize it compared to an empty "valid" column which typically means that is the original datadrop
                if nunique_valid_values == 1:
                    nunique_valid_values = -1
                print("valid nunique values:", nunique_valid_values)

                if DEBUG:
                    if datadropDF.valid.nunique() > max_classes :
                        max_classes = datadropDF.valid.nunique()
                        datadrop_with_most_classes = filename
            else:
                nunique_valid_values = 0

            auxDF = auxDF.append(
                {
                    "filename":filename,
                    "filename_lenght":len(filename),
                    "valid_nunique": nunique_valid_values,
                    "cant_rows": len(datadropDF)
                }, ignore_index=True)

        except Exception as e:
            # debug this as an edge case
            print("exception while selecting the datadrop file", filename, str(e))
            continue


    #print("shortest filename:", shortest_filename, "| Filename with more information:", datadrop_with_most_classes)

    if DEBUG: #debug
        print("most 'valid' classes:",datadrop_with_most_classes)
        print("longest datadrop:",longest_datadrop)
        print("shortest filename:",shortest_filename)

    if len(auxDF) > 0:

        auxDF = auxDF.sort_values(["valid_nunique", "cant_rows","filename_lenght"], ascending=(False, False, True))
        print(auxDF[["valid_nunique", "cant_rows","filename_lenght"]].head())
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




    graph = rc_graph.RCGraph()
    cant_partitions =0
    cant_dirs = 0
    cant_files = 0
    cant_shadow_partition =0
    cant_not_links = 0

    ## for each partition, check if there is a metadata folder with a matching name
    for partition_name, valid_links in graph.iter_publications(graph.PATH_PUBLICATIONS):
        cant_partitions += 1

        # infer datadrop original directory from partition filename
        datadrop_directory = re.sub(".json$", "", partition_name)

        if datadrop_directory.endswith("_publications"):
            datadrop_directory = re.sub("_publications$", "", datadrop_directory)
        else:
            print("this partition or the datadrop directory don't follow the name convention:",partition_name)

        print("partition:",datadrop_directory)

        # if datadrop_directory != "20191119_LongitudinalEmployer-HouseholdDynamicsOrigin-DestinationEmploymentStatistics":
        #     continue

        #check if the partition filename matches with a datadrop directory
        if (os.path.isdir(PATH_DATADROPS / datadrop_directory )):
            cant_dirs += 1

            # select the best candidate from all CSV files in the datadrop directory
            datadrop_filename = select_datadrop_file(PATH_DATADROPS / datadrop_directory)
            print("selected file: "+ str(datadrop_filename))

            if datadrop_filename:
                cant_files += 1
                # create a shadow partition with the verified not-links
                shadow_partition = recover_verified_not_links(PATH_DATADROPS / datadrop_directory / datadrop_filename, valid_links)
                if shadow_partition:
                    cant_shadow_partition += 1
                    cant_not_links += len(shadow_partition)

                    # save the shadow partition preserving the original partition name.
                        #also order links by title and internally order all metadata keys consistently in order to get an easier to read diff when tweaking the script
                    shadow_partition = sorted(shadow_partition, key=lambda x: x["title"])
                    with open(PATH_SHADOW_PARTITIONS / partition_name, 'w', encoding="utf-8") as outfile:
                        json.dump(shadow_partition, outfile, indent=2, ensure_ascii=False, sort_keys=True)

        else:
            print(PATH_DATADROPS / datadrop_directory, "does not exists")


    print("existing partitions",cant_partitions)
    print("matching directories by name",cant_dirs)
    print("matching files by name",cant_files)
    print("shadow partition",cant_shadow_partition)
    print("not-links",cant_not_links)




if __name__ == "__main__":

    main()
