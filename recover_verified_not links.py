import codecs
import json

from richcontext import graph as rc_graph
import re
from pathlib import Path
import os
import pandas as pd

PATH_DATADROPS = Path("richcontextmetadata/metadata")


def recover_verified_not_links(filename, publications):


    datadropDF = pd.read_csv(PATH_DATADROPS / filename, encoding='utf-8')


    print(datadropDF.head()) #TODO: describe column valid if exists

    ratio = len(publications) / len(datadropDF)
    print("partition size", len(publications),"| datadrop size", len(datadropDF),\
          "valid/total ratio", str(ratio), "process:", ratio *100 > 5 )
    return



def main():
    ##TODO list:
        #process datadrop if there is a partition (since not all datadrops where reviewed)
        #open the original datadrop (and not the final or intermediate csv)
        #validate that the original datadrop has more publications than the partition
        #filter out datadrops too big to be sure that where fully processed
        #validate that the partition publications exists in the original datadrop
        #be careful with the USDA ARMS datadrops




    ## for each partition, check if there is a metadata folder with a matching name

    graph = rc_graph.RCGraph()
    cant_partitions =0
    cant_dirs = 0
    cant_files = 0

    for partition, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS):
        cant_partitions += 1


        datadrop_directory = re.sub('.json$', '', partition)

        if datadrop_directory.endswith('_publications'):
            datadrop_directory = re.sub('_publications$', '', datadrop_directory)

        else:
            print("not all partitions follows the name convention:",partition)

        print(datadrop_directory)

        #check if the partition filename matches with a datadrop directory
        if (os.path.isdir(PATH_DATADROPS / datadrop_directory )):
            cant_dirs += 1
            for filename in os.listdir(PATH_DATADROPS / datadrop_directory):
                #process only original files ##TODO: validate that this if captures only the original datadrop
                if filename == datadrop_directory+".csv" or filename == datadrop_directory.split('_')[-1]+".csv" :
                    cant_files += 1
                    datadrop_filename = os.path.join(datadrop_directory, filename)
                    print(datadrop_filename)
                    recover_verified_not_links(datadrop_filename,pub_iter)
                    continue
                else:
                    continue
        else:
            print(PATH_DATADROPS / datadrop_directory, "does not exists")


    print('existing partitions',cant_partitions)
    print('matching directories by name',cant_dirs)
    print('matching files by name',cant_files)




if __name__ == '__main__':

    main()
