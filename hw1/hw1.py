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
    return re.split("\W+", line.strip())


def count_them(word_list: List[str], file_list: List[str]) -> Dict[str, int]:
    """ Returns a dictionary of {word: count}
    
    Input:
       word_list: a python list of words we are interested in
       file_list: a list of file names

    Output:
       a python dictionary where the key is a word (from word_list) and the value
       is the number of times that word appears in all of the files.

    """
    pass


def sortfile(f: str) -> List[str]:
    """ Returns an array consisting of the sorted words in f"""
    with open(f, "r") as infile:
        words = [word for line in infile.readlines() for word in tokenize(line)]
        words.sort()
    return words


def mergesort(file_list: List[str]) -> Tuple[Any, List[str]]:
    """ Performas a parallelized merge sort with branching factor 2 over the files in file_list 
    
    Input: 
       file_list: list of file names

    Output:
       a tuple. The first part of the tuple is the delayed object for the computation, the second part is a list
       of the sorted words
    """
    pass
