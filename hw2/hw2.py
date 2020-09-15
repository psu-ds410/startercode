# Author: Anikate Ganju
#Date due: 9/15/2020

import dask
from dask import delayed
from dask.distributed import Client
import typing
import re

#@delayed
def tokenize(line: str) -> typing.List[str]:
    """ Splits a line into words """
    trimmed = line.strip()
    return re.split("\W+", trimmed) if trimmed else []


def count_them(word_list: typing.List[str], file_list: typing.List[str]) -> typing.Dict[str, int]:
    """ Returns a dictionary of {word: count}
    
    Input:
       word_list: a python list of words we are interested in
       file_list: a list of file names

    Output:
       a python dictionary where the key is a word (from word_list) and the value
       is the number of times that word appears in all of the files.

    """
    #with Client(n_workers=4) as c:
        #for i in file_list:
            #open(file_list[i],'r')
            #read file
            #tokenize the stringfile



def sortfile(f: str) -> typing.List[str]:
    """ Returns an array consisting of the sorted words in f"""
    with open(f, "r") as infile:
        words = [word for line in infile.readlines() for word in tokenize(line)]
        words.sort()
    return words


def mergesort(file_list: typing.List[str]) -> typing.Tuple[typing.Any, typing.List[str]]:
    """ Performas a parallelized merge sort with branching factor 2 over the files in file_list 
    
    Input: 
       file_list: list of file names

    Output:
       a tuple. The first part of the tuple is the delayed object for the computation, the second part is a list
       of the sorted words
    """
    pass

#file1=open('C:\Users\anika\Desktop\DS410\startercode\hw2\data_files\part-00000', 'r')
file1=open('part-00000','r')
#file2=open('C:\Users\anika\Desktop\DS410\startercode\hw2\data_files\part-00001', 'r')
file2=open('part-00001','r')
#file3=open('C:\Users\anika\Desktop\DS410\startercode\hw2\data_files\part-00002', 'r')
file3=open('part-00002','r')
#file4=open('C:\Users\anika\Desktop\DS410\startercode\hw2\data_files\part-00003', 'r')
file4=open('part-00003','r')
filelist=[file1,file2,file3,file4]
a=file1.read()
print(a)
print(file1.tokenize())

