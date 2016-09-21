import numpy as np
import pybrain
import sys
import time

#
# PyAPI Operators
#
class Counter(object):
    def __init__(self):
        self.count = 0

    def __call__(self):
        while True:
            self.count = self.count + 1
            yield self.count
            time.sleep(.2) 
            
class negative_one(object):
    
    def __call__(self, tup):
        return tup * -1
