def string_add(v):
   return v + "_Python234"

def string_filter(v):
   return len(v) <= 3

def json_add(v):
   v["c"] = v["a"] + 235
   return v

def json_filter(v):
   return v["a"] <= 100
