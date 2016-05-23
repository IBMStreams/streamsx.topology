class SourceTuples:
   def __init__(self, tuples=[]):
      self.tuples = tuples
   def __call__(self):
      return self.tuples

class CheckTuples:
   def __init__(self, tuples=[]):
      self.tuples = tuples
      self.index = 0
   def __call__(self, t):
      print("TUPLE", t)
      assert (self.index < len(self.tuples)), ("Expected index=" + str(self.index) + " < " + str(len(self.tuples)))
      assert (t == self.tuples[self.index]), ("Expected=" + self.tuples[self.index] +  ", actual=" + str(t))
      self.index += 1