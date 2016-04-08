import test_functions2_dep

def hello_world() :
    return ["Hello", "World!"]

def filter(t):
   return test_functions2_dep.filter(t) 

global _hwcount_filter
_hwcount_filter = 0
def check_hello_world_filter(t):
   print("TUPLE", t)
   global _hwcount_filter
   if _hwcount_filter == 0:
      if t != "Hello":
          raise AssertionError()
      _hwcount_filter += 1
      return None
   raise AssertionError()