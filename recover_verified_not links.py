
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

DATADROP_SIZE_UPPER_LIMIT = 75 # When datadrops are too big, it's unlikely to be fully processed. TODO: 75 is arbitrary
RATIO_LOWER_LIMIT = 0.15 # ratio defined as partition size / datadrop size. If is too low, it is probably that the reviewer did not process the whole datadrop

SKIP_MAYBES = False

def normalize_fields(datadropDF):
    # unify different column name for valid links
    replacements = {
        "valid": ["valid", "valid?", "Validation", "validation", "dataset_used?","Used dataset?"],
        "dataset": ["dataset", "datasets", "dataset_id", "dataset id"]
    }
    datadropDF.columns = datadropDF.columns.str.strip()
    datadropDF.rename(columns={el: k for k, v in replacements.items() for el in v}, inplace=True)

    return datadropDF

def normalize_valid_field_values(datadropDF):
    # there is no standard nomenclature for "valid" and "not valid" links.
    if DEBUG:
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

    if DEBUG:
        print("after replace:",datadropDF.valid.unique())

#copied this function from publications_export_template.py and edited it
def create_pub_dict(linkages_dataframe):

    # Cleaning the Dataframe
    linkages_dataframe = linkages_dataframe.loc[pd.notnull(linkages_dataframe.dataset)].drop_duplicates() #prevent null values for datasets in some datadrop files.

    # Get Datasets Information
    datasets_file_path = PATH_DATASETS / "datasets.json"
    with open(datasets_file_path, encoding="utf-8") as json_file:
        datasets = json.load(json_file)

    pub_metadata_fields = ["title"]
    original_metadata_cols = list(
        set(linkages_dataframe.columns.values.tolist()) - set(pub_metadata_fields) - set(["dataset"]))

    pub_dict_list = []
    for i, r in linkages_dataframe.iterrows():

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
            original_metadata = {k: v for k, v in original_metadata_raw.items() if not pd.isnull(v)}
            pub_dict.update({"original": original_metadata})
        pub_dict_list.append(pub_dict)
    return pub_dict_list

def recover_verified_not_links(datadropDF, valid_links):

    if "valid" in datadropDF.columns:
        normalize_valid_field_values(datadropDF)
        print("valid unique values:",datadropDF.valid.nunique())
    else:
        print("valid column not present")

    ratio = len(valid_links) / len(datadropDF)
    print("partition size", len(valid_links), "| datadrop size", len(datadropDF),\
          "valid/total ratio", str(ratio), "process:", ratio  > RATIO_LOWER_LIMIT and ratio != 1)

    # filter out datadrops
    if len(datadropDF) > DATADROP_SIZE_UPPER_LIMIT: # Datadrops too big
        print("Datadrop too big",len(datadropDF))
        return None
    elif len(valid_links) == len(datadropDF): # cvs file contains only valid links or is incomplete
        return None
    elif ratio < RATIO_LOWER_LIMIT: # some big datadrops when having a low valid ratio are not fully evaluated
        return None
    else:

        datadrop_links = create_pub_dict(normalize_fields(datadropDF))

        shadow_partition = list()

        # iterate through each link in the datadrop
        for link in datadrop_links:
            link_exists_in_partition = False

            if "valid" in link["original"]:
                # forcing not to include links marked as valid in the datadrop file analyzed
                if link["original"]["valid"] == "yes":
                    continue
                # optionally forcing to include only links marked as not valid in the datadrop
                elif SKIP_MAYBES and link["original"]["valid"] != "no":
                    continue

            # iterate through each link in the partition (valid="yes" by definition)
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
        #2. file with most rows (to skip the files that only have valid links)
        #3. file with the shortest filename (usually -but not always- the original datadrop)

    if DEBUG: #debug
        print("***********select datadrop file***********")
        shortest_filename = None
        datadrop_with_most_classes = None
        max_classes = 0
        longest_datadrop = None
        datadrop_max_row_count = 0

    auxDF = pd.DataFrame(columns=["filename","filename_lenght","valid_nunique","cant_rows"])
    datadrops = dict()

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
                print("handled UnicodeDecodeError exception file",filename,str(e))
                datadropDF = pd.read_csv(datadrop_directory / filename, engine="python")

            datadropDF = normalize_fields(datadropDF)

            if "dataset" not in datadropDF.columns:
                print("dataset field not included in CSV file:",datadropDF.columns)
                continue
            elif datadropDF["dataset"].dtypes != numpy.object: #some files have numbers in the dataset field.
                print("dataset field value types incorrect:", datadropDF["dataset"].dtypes)
                continue

            if DEBUG:
                if "valid" not in datadropDF.columns:
                    print("after column rename:", datadropDF.columns)

                if shortest_filename == None or len(filename) < len(shortest_filename):
                    shortest_filename = filename

                if len(datadropDF) > datadrop_max_row_count:
                    datadrop_max_row_count = len(datadropDF)
                    longest_datadrop = filename

            if "valid" in datadropDF.columns:
                nunique_valid_values = datadropDF.valid.nunique()

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
            datadrops[filename]= datadropDF

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
        # get the most relevant datadrop filename as first element
        auxDF = auxDF.sort_values(["valid_nunique", "cant_rows","filename_lenght"], ascending=(False, False, True))
        print(auxDF[["valid_nunique", "cant_rows","filename_lenght"]].head())

        print("selected file: " + str(auxDF.iloc[0]["filename"]))

        # return the most relevant datadrop as a dataframe
        return datadrops[ auxDF.iloc[0]["filename"] ]
    else:
        return None


def main():
    ##TODO #Note: USDA ARMS datadrops where processed using only the abstract, here there might be several false negatives

    graph = rc_graph.RCGraph()
    count_partitions =0
    count_dirs = 0
    count_files = 0
    count_shadow_partition =0
    count_not_links = 0

    ## for each partition, check if there is a metadata folder with a matching name
    for partition_name, valid_links in graph.iter_publications(graph.PATH_PUBLICATIONS):
        count_partitions += 1

        # infer datadrop original directory from partition filename
        datadrop_directory = re.sub(".json$", "", partition_name)

        if datadrop_directory.endswith("_publications"):
            datadrop_directory = re.sub("_publications$", "", datadrop_directory)
        else:
            print("this partition or the datadrop directory don't follow the name convention:",partition_name)

        print("partition:",datadrop_directory)

        #check if the partition filename matches with a datadrop directory
        if (os.path.isdir(PATH_DATADROPS / datadrop_directory )):
            count_dirs += 1

            # select the best candidate from all CSV files in the datadrop directory
            datadropDF = select_datadrop_file(PATH_DATADROPS / datadrop_directory)

            if datadropDF is not None:
                count_files += 1
                # create a shadow partition with the verified not-links
                shadow_partition = recover_verified_not_links(datadropDF, valid_links)
                if shadow_partition:
                    count_shadow_partition += 1
                    count_not_links += len(shadow_partition)

                    # save the shadow partition preserving the original partition name.
                        #also order links by title and internally order all metadata keys consistently in order to get an easier to read diff when tweaking the script
                    shadow_partition = sorted(shadow_partition, key=lambda x: x["title"])
                    with open(PATH_SHADOW_PARTITIONS / partition_name, 'w', encoding="utf-8") as outfile:
                        json.dump(shadow_partition, outfile, indent=2, ensure_ascii=False, sort_keys=True)

        else:
            print(PATH_DATADROPS / datadrop_directory, "does not exist")

    print("existing partitions",count_partitions)
    print("matching directories by name",count_dirs)
    print("matching files by name",count_files)
    print("shadow partition",count_shadow_partition)
    print("not-links",count_not_links)


if __name__ == "__main__":

    main()
