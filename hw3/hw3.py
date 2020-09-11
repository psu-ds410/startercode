# I collaborated with:
#
# 1)
# 2) 
# ...
#


from dask import delayed
from dask.distributed import Client
from typing import List, Dict, Tuple, Any
import re

def tokenize(line: str) -> List[str]:
    """ Splits a line into words """
    trimmed = line.strip()
    return re.split("\W+", trimmed) if trimmed else []


def sortfile(f: str) -> List[str]:
    """ Returns an array consisting of the sorted words in f"""
    with open(f, "r") as infile:
        words = [word for line in infile.readlines() for word in tokenize(line)]
        words.sort()
    return words


def mergesort(file_list: List[str]) ->  List[str]:
    """ Performas a parallelized merge sort with branching factor 2 over the files in file_list 
    
    Input: 
       file_list: list of file names

    Output:
       the list of the sorted words

    Note:
       you must use the futures api and merge files as results become available
    """
    pass
