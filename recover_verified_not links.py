
from richcontext import graph as rc_graph
import re
from pathlib import Path
import os
import pandas as pd

PATH_DATADROPS = Path("richcontextmetadata/metadata")
DEBUG = False


def recover_verified_not_links(filename, publications):

    pubs_classified_as_not_valid = False

    try:
        datadropDF = pd.read_csv(filename, encoding='utf-8')
    except Exception as e:
        # debug this as an edge case
        print('exception while trying to open',filename,str(e))
        return

    #print(datadropDF.head()) #TODO: describe column valid if exists

    if 'valid' in datadropDF.columns:
        print("valid unique values:",datadropDF.valid.nunique())
    else:
        print("valid column not present")

    ratio = len(publications) / len(datadropDF)
    print("partition size", len(publications),"| datadrop size", len(datadropDF),\
          "valid/total ratio", str(ratio), "process:", ratio *100 > 5 )
    return


def select_datadrop_file(datadrop_directory):
    ## criteria for selecting best CSV file:
        #1. column "valid" with the most unique values (to make sure not valids are identified)
        #2. longest file (to skip the files that only have valid links)
        #3. file with the shortest filename (usually -but not always- the original datadrop)

    if DEBUG: #debug
        print("***********select datadrop file***********")

    shortest_filename = None
    datadrop_with_most_classes = None
    max_classes = 0
    longest_datadrop = None
    datadrop_max_row_count = 0

    auxDF = pd.DataFrame(columns=['filename','filename_lenght','valid_nunique','cant_rows'])

    for filename in os.listdir(datadrop_directory):

        if not filename.endswith(".csv"):
            continue

        try:
            datadropDF = pd.read_csv(datadrop_directory / filename, encoding='utf-8')

            # unify different column name for valid links
            replacements = {
                'valid': ['valid', 'valid?', 'Validation', 'validation']
            }
            datadropDF.columns = datadropDF.columns.str.strip()
            datadropDF.rename(columns={el: k for k, v in replacements.items() for el in v}, inplace=True)

            if DEBUG: # deguggin
                if "valid" not in datadropDF.columns:
                    print("after column rename:", datadropDF.columns)

                if shortest_filename == None or len(filename) < len(shortest_filename):
                    shortest_filename = filename

                if len(datadropDF) > datadrop_max_row_count:
                    datadrop_max_row_count = len(datadropDF)
                    longest_datadrop = filename

            if 'valid' in datadropDF.columns:
                nunique_valid_values = datadropDF.valid.nunique()

                if DEBUG:
                    if datadropDF.valid.nunique() > max_classes :
                        max_classes = datadropDF.valid.nunique()
                        datadrop_with_most_classes = filename
            else:
                nunique_valid_values = -1

            auxDF = auxDF.append(
                {
                    'filename':filename,
                    'filename_lenght':len(filename),
                    'valid_nunique': nunique_valid_values,
                    'cant_rows': len(datadropDF)
                }, ignore_index=True)

        except Exception as e:
            # debug this as an edge case
            print('exception while trying to open', filename, str(e))
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

    for partition_name, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS):
        cant_partitions += 1


        datadrop_directory = re.sub('.json$', '', partition_name)

        if datadrop_directory.endswith('_publications'):
            datadrop_directory = re.sub('_publications$', '', datadrop_directory)

        else:
            print("not all partitions follows the name convention:",partition_name)

        print("partition:",datadrop_directory)

        #check if the partition filename matches with a datadrop directory
        if (os.path.isdir(PATH_DATADROPS / datadrop_directory )):
            cant_dirs += 1

            datadrop_filename = select_datadrop_file(PATH_DATADROPS / datadrop_directory)
            print("selected file: "+ str(datadrop_filename))


            if datadrop_filename:
                recover_verified_not_links(PATH_DATADROPS / datadrop_directory / datadrop_filename, pub_iter)
                cant_files += 1

            # for filename in os.listdir(PATH_DATADROPS / datadrop_directory):
            #     #process only original files ##TODO: validate that this if captures only the original datadrop
            #     if filename == datadrop_directory+".csv" or filename == datadrop_directory.split('_')[-1]+".csv" :
            #         cant_files += 1
            #         datadrop_filename = os.path.join(datadrop_directory, filename)
            #         print(datadrop_filename)
            #
            #         continue
            #     else:
            #         continue
        else:
            print(PATH_DATADROPS / datadrop_directory, "does not exists")


    print('existing partitions',cant_partitions)
    print('matching directories by name',cant_dirs)
    print('matching files by name',cant_files)




if __name__ == '__main__':

    main()
