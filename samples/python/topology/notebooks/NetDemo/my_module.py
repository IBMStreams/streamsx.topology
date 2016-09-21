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
           
        
        
        
        

#
# ByBrain Operators
#
class linspace_yielder(object):
    def __init__(self, start, stop, count):
        self.nums = np.linspace(start,stop,count)

    def __call__(self):
        for num in self.nums:
            yield num
            time.sleep(.2)
            
class array_yielder(object):
    def __init__(self, arr):
        self.arr = arr

    def __call__(self):
        for num in self.arr:
            yield num
            time.sleep(.2) 
        

class neural_net_model(object):
    def __init__(self, model):
        self.model = model

    def __call__(self, num):
        return self.model.activate([num])[0]
    
class random_walk(object):
    def __init__(self, mean):
        self.mean = mean
    
    def __call__(self):
        mu = self.mean
        sigma = 0.1
        while True:
            s = np.random.normal(mu, sigma, 1)
            for i in s:
                yield i
            mu = np.random.normal(mu,sigma,1)[0]
            time.sleep(.1)