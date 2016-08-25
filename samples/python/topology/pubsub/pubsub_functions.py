# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import itertools
import time

def sequence():
    return itertools.count()

def delay(v):
    time.sleep(0.1)
    return True
