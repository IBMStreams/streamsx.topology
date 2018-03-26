# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import sys


def hello() :
    return ["Hello",]

def beautiful() :
    return ["beautiful",]

def crazy() :
    return ["crazy",]

def world() :
    return ["World!",]

global _hwcountU
_hwcountU = 0

#
# By default asserts are disabled
# because sc defaults to optimized
# compilation
def check_asserts_disabled(tuple):
    assert False, "Expect assertions to be disabled"
    return True
    

def check_union_hello_world(t) :

   global _hwcountU
   print("TUPLE", t, "count", _hwcountU)
   _hwcountU += 1
   if t == "Hello":
       return None 
   elif t == "World!":
       return None
   elif t == "beautiful" :
       return None
   elif t == "crazy" :
       return None
   elif _hwcountU > 4:
       raise AssertionError()
   else :	
       raise AssertionError()

def mqtt_publish() :
    return [123, 2.344, "4.0", "Garbage text", 1.234e+15,]

def mqtt_publish_class() :
    tp = TestPublish("Message to publish")
    testPublishStr = repr(tp)
    return [testPublishStr,]

def hello_world() :
    return ["Hello", "World!"]

global _hwcount
_hwcount = 0

def check_hello_world(t):
   print("TUPLE", t)
   global _hwcount
   if _hwcount == 0:
      if t != "Hello":
          raise AssertionError()
      _hwcount += 1
      return None
   if _hwcount == 1:
      if t != "World!":
          raise AssertionError()
      _hwcount += 1
      return None
   raise AssertionError()

def mqtt_subscribe(t):
    print("String tuple",t)
    sys.stdout.flush()
    if t not in ["123", "2.344", "4.0", "Garbage text", "1234000000000000.0",] :
        print("Invalid Tuple", t)
        raise AssertionError()	 
    return None

def mqtt_subscribe_class(t):
    print("String tuple",t)
    sys.stdout.flush()
    reprClass = repr(t)
    newTestPublish = TestPublish(reprClass)
    print("ReprClass", newTestPublish)
    if not isinstance(newTestPublish, TestPublish) :
        print("Invalid Tuple", t)
        raise AssertionError()     
    return None

def filter(t):
   return "Wor" in t

global _hwcount_filter
_hwcount_filter = 0
def check_hello_world_filter(t):
   print("TUPLE", t)
   global _hwcount_filter
   if _hwcount_filter == 0:
      if t != "World!":
          raise AssertionError()
      _hwcount_filter += 1
      return None
   raise AssertionError()

def int_strings_transform():
   return ["325", "457", "9325"]

def int_strings_transform_with_drop():
   return ["93", "68", "221"]

def string_to_int_except68(t):
   if t == "68":
      return None
   else:
      return int(t)

def add17(t):
   return t + 17

global _hwcount_transform
_hwcount_transform = 0
def check_int_strings_transform(t):
   print("TUPLE", t)
   global _hwcount_transform
   if _hwcount_transform == 0:
      if t != 342:
         raise AssertionError()
      _hwcount_transform += 1
      return None
   if _hwcount_transform == 1:
      if t != 474:
         raise AssertionError()
      _hwcount_transform += 1
      return None
   if _hwcount_transform == 2:
      if t != 9342:
         raise AssertionError()
      _hwcount_transform += 1
      return None
   raise AssertionError() 

global _hwcount_transform_with_drop
_hwcount_transform_with_drop = 0
def check_int_strings_transform_with_drop(t):
   print("TUPLE", t)
   global _hwcount_transform_with_drop
   if _hwcount_transform_with_drop == 0:
      if t != 110:
         raise AssertionError()
      _hwcount_transform_with_drop += 1
      return None
   if _hwcount_transform_with_drop == 1:
      if t != 238:
         raise AssertionError()
      _hwcount_transform_with_drop += 1
      return None
   raise AssertionError() 
 
def strings_multi_transform():
   return ["mary had a little lamb", "its fleece was white as snow"]
   
global EXPECTED_STRINGS_MULTI_TRANSFORM
EXPECTED_STRINGS_MULTI_TRANSFORM = ["mary", "had", "a", "little", "lamb", "its", "fleece", "was", "white", "as", "snow"]

def split_words(t):
   return t.split()
 
global _hwcount_multi_transform
_hwcount_multi_transform = 0
def check_strings_multi_transform(t):
   print("TUPLE", t)
   global _hwcount_multi_transform
   assert (_hwcount_multi_transform < len(EXPECTED_STRINGS_MULTI_TRANSFORM)), \
      ("Expected index=" + str(_hwcount_multi_transform) + " < " + str(len(EXPECTED_STRINGS_MULTI_TRANSFORM)))
   assert (t == EXPECTED_STRINGS_MULTI_TRANSFORM[_hwcount_multi_transform]), \
      ("Expected=" + EXPECTED_STRINGS_MULTI_TRANSFORM[_hwcount_multi_transform] +  ", actual=" + str(t))
   _hwcount_multi_transform += 1
   
def strings_length_filter():
   return ["hello", "goodbye", "farewell"]

def produceHash(t) :
    print("TUPLE",t)
    print("Hash value: ",hash(t)& 0xffffffff)
    return hash(t) & 0xffffffff

class LengthFilter:
   def __init__(self, upper):
      self.upper = upper  
   def __call__(self, tuple):
      if len(tuple) > self.upper:
         return True
      return False

global EXPECTED_STRINGS_LENGTH_FILTER
EXPECTED_STRINGS_LENGTH_FILTER = ["goodbye", "farewell"]

global _hwcount_length_filter
_hwcount_length_filter = 0
def check_strings_length_filter(t):
   print("TUPLE", t)
   global _hwcount_length_filter
   assert (_hwcount_length_filter < len(EXPECTED_STRINGS_LENGTH_FILTER)), \
      ("Expected index=" + str(_hwcount_length_filter) + " < " + str(len(EXPECTED_STRINGS_LENGTH_FILTER)))
   assert (t == EXPECTED_STRINGS_LENGTH_FILTER[_hwcount_length_filter]), \
      ("Expected=" + EXPECTED_STRINGS_LENGTH_FILTER[_hwcount_length_filter] +  ", actual=" + str(t))
   _hwcount_length_filter += 1
   
class AddNum:
   def __init__(self, increment):
      self.increment = increment  
   def __call__(self, tuple):
      return tuple + self.increment

def seedSource():
   return [1, 2, 3, 4, 1, 1, 1, 1,]


class SeedSinkRR:
   def __init__(self):
      self.goodresult = [1, 2, 3, 4, 2, 3, 4, 5,]
   def __call__(self, tuple):
      print("SINK TUPLE", tuple)
      if( tuple in self.goodresult) :
         self.goodresult.remove(tuple)
         return None
      raise AssertionError()

class SeedSinkRRPU:
   def __init__(self):
      self.goodresult = [1, 1, 2, 2, 3, 3, 4, 4, 2, 2, 3, 3, 4, 4, 5, 5,]
   def __call__(self, tuple):
      print("SINK TUPLE", tuple)
      if( tuple in self.goodresult) :
         self.goodresult.remove(tuple)
         return None
      raise AssertionError()

class SeedSinkHashOrKey:
   def __init__(self):
      self.goodresult = [1, 2, 3, 4, 2, 2, 2, 2,]
   def __call__(self, tuple):
      print("SINK TUPLE", tuple)
      if( tuple in self.goodresult) :
         self.goodresult.remove(tuple)
         return None
      raise AssertionError()

class ProgramedSeed:
   def __init__(self):
      self.first = True 
      self.seed = 0
   def __call__(self, tuple):
      if(self.first):
         self.first = False
         print("SEEDED" , tuple)
         self.seed = tuple
         return tuple
      print("Passing", tuple + self.seed)		
      return (tuple + self.seed)

class IncMaxSplitWords:
   def __init__(self, maxsplit):
      self.maxsplit = maxsplit  
   def __call__(self, tuple):
      words = tuple.split(None, self.maxsplit)
      # test mutable state, increase maxsplit by 1 for next tuple
      self.maxsplit += 1
      return words

global EXPECTED_STRINGS_MULTI_TRANSFORM_INC_MAX_SPLIT
EXPECTED_STRINGS_MULTI_TRANSFORM_INC_MAX_SPLIT = ["mary", "had a little lamb", "its", "fleece", "was white as snow"]
 
global _hwcount_multi_transform_inc_max_split
_hwcount_multi_transform_inc_max_split = 0
def check_strings_multi_transform_inc_max_split(t):
   print("TUPLE", t)
   global _hwcount_multi_transform_inc_max_split
   assert (_hwcount_multi_transform_inc_max_split < len(EXPECTED_STRINGS_MULTI_TRANSFORM_INC_MAX_SPLIT)), \
      ("Expected index=" + str(_hwcount_multi_transform_inc_max_split) + " < " + str(len(EXPECTED_STRINGS_MULTI_TRANSFORM_INC_MAX_SPLIT)))
   assert (t == EXPECTED_STRINGS_MULTI_TRANSFORM_INC_MAX_SPLIT[_hwcount_multi_transform_inc_max_split]), \
      ("Expected=" + EXPECTED_STRINGS_MULTI_TRANSFORM_INC_MAX_SPLIT[_hwcount_multi_transform_inc_max_split] +  ", actual=" + str(t))
   _hwcount_multi_transform_inc_max_split += 1

class SourceTuplesAppendIndex:
   def __init__(self, tuples=[]):
      self.tuples = tuples
   def __call__(self):
      for i in range(len(self.tuples)):
          self.tuples[i] += str(i)
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
      
class TestPublish:
    def __init__(self,idMsg):
        self.idMsg = idMsg
    def __repr__(self):
        return '%s(**%r)' % (self.__class__.__name__, self.__dict__)          
    def print_message(self):
        print(self.idMsg)
