# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import random

def readings():
    """
    Generator function that returns a generator object.
    The `source` operation gets an iterator from the 
    generator object, and will continually call `next()`
    on the iterator to retrieve the next tuple.
    
    In this example, each tuple is a random number.
    
    This function will produce an infinite number of 
    tuples for the stream.
    """
    while True:
        yield random.gauss(0.0, 1.0)
