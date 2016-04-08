class SourceTuples:
   def __init__(self, tuples=[]):
      self.tuples = tuples
   def __call__(self):
      return self.tuples
