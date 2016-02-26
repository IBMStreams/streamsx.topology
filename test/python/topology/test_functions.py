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

def filter(t):
   return "Wor" in t;

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

def string_to_int(t):
   return int(t)

def string_to_int_except68(t):
   if t == "68":
      return None
   else:
      return int(t)

def add17(t):
   return t + 17

def check_int_strings_transform(t):
   print("TUPLE", t)
   global _hwcount
   if _hwcount == 0:
      if t != 342:
         raise AssertionError()
      _hwcount += 1
      return None
   if _hwcount == 1:
      if t != 474:
         raise AssertionError()
      _hwcount += 1
      return None
   if _hwcount == 2:
      if t != 9342:
         raise AssertionError()
      _hwcount += 1
      return None
   raise AssertionError() 

def check_int_strings_transform_with_drop(t):
   print("TUPLE", t)
   global _hwcount
   if _hwcount == 0:
      if t != 110:
         raise AssertionError()
      _hwcount += 1
      return None
   if _hwcount == 1:
      if t != 238:
         raise AssertionError()
      _hwcount += 1
      return None
   raise AssertionError() 
 
def strings_multi_transform():
   return ["mary had a little lamb", "its fleece was white as snow"]
   
def expected_strings_multi_transform():
   return ["mary", "had", "a", "little", "lamb", "its", "fleece", "was", "white", "as", "snow"]

def splitWords(t):
   return t.split()
 
def check_strings_multi_transform(t):
   print("TUPLE", t)
   global _hwcount
   expected_strings = expected_strings_multi_transform()
   assert (_hwcount < len(expected_strings)), \
      ("Expected index=" + str(_hwcount) + " < " + str(len(expected_strings)))
   assert (t == expected_strings[_hwcount]), \
      ("Expected=" + expected_strings[_hwcount] +  ", actual=" + str(t))
   _hwcount += 1
   
