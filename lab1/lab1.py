# Author: Anikate Ganju
# Date: 9/10/2020
# Assignment: Dask Lab 1
# I collaborated with:
# 1)no one
# ............
import dask
import graphviz
from dask import delayed
from typing import List
import time


@delayed
def addthem(x: List[float]) -> float:
    """ adds the elements in the list x """
    time.sleep(1)
    return sum(x)


@delayed
def increment(x: float) -> float:
    """ adds 1 to x """
    time.sleep(1)
    return x+1


def go(myarray: List[float], branch_factor: int):
    """ This function should return a delayed object corresponding
    to a computation graph that adds up the numbers myarray. The computation
    graph should look like a tree with branching factor branch_factor.

    Inputs: 
        myarray: a python list of numbers
        branch_factor: an integer corresponding to the branching factor

    Output:
        a dask object with the correct computation graph. The type of the output
        obtained by calling type(your return value) should be <class 'dask.delayed.Delayed'>
        

    Notes:
       if the input is myarray=[0,1,2,3,4,5,6,7,8,9,10] and branch_factor=3, then calling 
       the visualize method of the dask object (my_return_value.visualize('filename.png'))
       should return the same graph as in the assignment writeup.

    """

    def group(list, n):
        newlist = []
        for i in range(0, len(list), n):
            a = list[i:(i + n)]
            newlist.append(a)
        return newlist

    inclist=[]
    le=len(myarray)
    for i in range(le):
        x=myarray[i]
        y=increment(x)
        inclist.append(y)
    #inclist=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    hi=inclist
    grp=[]
    while len(hi)>1:
        hi=group(hi,branch_factor)
        #print("Grouped: ", hi)
        hilen=len(hi)
        for i in range(0,hilen):
            a=hi[i]
            b=addthem(a)
            hi[i]=b
        print(hi)

    #hi=dask.persist(hi)
    print(hi[0].compute())
    return hi[0]




delayed_object = go ([0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10] , 3)
delayed_object.visualize(filename='tree.png')
