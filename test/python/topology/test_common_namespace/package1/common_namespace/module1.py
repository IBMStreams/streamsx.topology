# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
class SourceTuples:
   def __init__(self, tuples=[]):
      self.tuples = tuples
   def __call__(self):
      return self.tuples
