# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
def string_add(v):
   return v + "_Python234"

def string_filter(v):
   return len(v) <= 3

def string_fm(v):
   return v.split()

def json_add(v):
   v["c"] = v["a"] + 235
   return v

def json_filter(v):
   return v["a"] <= 100

def json_fm(v):
   return [v["a"], v["b"]]

# JSON serialization doesn't handle complex numbers, decimal, Timestamp
# IBM Java JSON deserialization can't handle uint64 bigger than Long.MAX_VALUE
def remove_complex(v):
   r = dict(v)
   del r['c32']
   del r['c64']
   del r['d32']
   del r['d64']
   del r['d128']
   del r['u64']
   del r['lui64']
   del r['ts']
   del r['binary']
   return r


# JSON serialization doesn't handle sets, change them to a list
def change_set_to_list(v):
   r = dict(v)
   s = r['si32']
   if isinstance(s, set):
     del r['si32']
     l = list(s)
     r['si32'] = l
   return r
