from richcontext import graph as rc_graph
import re
from pathlib import Path
import os

PATH_DATADROPS = Path("richcontextmetadata/metadata")

def main():
    ##TODO list:
        #process datadrop if there is a partition (since not all datadrops where reviewed)
        #open the original datadrop (and not the final or intermediate csv)
        #validate that the original datadrop has more publications than the partition
        #validate that the partition publications exists in the original datadrop




    ## for each partition, check if there is a metadata folder with a matching name

    graph = rc_graph.RCGraph()
    cant_partitions =0
    cant_dirs = 0
    cant_files = 0

    for partition, pub_iter in graph.iter_publications(graph.PATH_PUBLICATIONS):
        cant_partitions += 1

        partition = re.sub('.json$', '', partition)

        if partition.endswith('_publications'):
            partition = re.sub('_publications$', '', partition)

        else:
            print("not all partitions follows the name convention:",partition)

        print(partition)

        #check if the partition filename matches with a datadrop directory
        if (os.path.isdir(PATH_DATADROPS / partition )):
            cant_dirs += 1
            for filename in os.listdir(PATH_DATADROPS / partition):
                #process only original files ##TODO: validate that this if captures only the original datadrop
                if filename == partition+".csv" or filename == partition.split('_')[-1]+".csv" :
                    cant_files += 1
                    print(os.path.join(partition, filename))
                    continue
                else:
                    continue
        else:
            print(PATH_DATADROPS / partition, "does not exists")


    print('existing partitions',cant_partitions)
    print('matching directories by name',cant_dirs)
    print('matching files by name',cant_files)




if __name__ == '__main__':

    main()
