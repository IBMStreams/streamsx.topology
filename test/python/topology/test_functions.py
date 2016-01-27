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
