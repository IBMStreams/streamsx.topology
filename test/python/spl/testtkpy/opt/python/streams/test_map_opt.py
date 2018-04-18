# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test Map functions for optional types
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pymap.opttype"

@spl.map()
class SparseTupleMap:
    def __init__(self):
       pass

    def __call__(self, *t):
        return (t[0]+81, None, None)

@spl.map()
def DictTupleMap(**tuple):
    a = tuple['a']
    if a == 3245:
        tuple['b'] = tuple['c'] + 27
        tuple.pop('c')
        tuple['d'] = None
        tuple['e'] = None
        return tuple
    if a == 831:
        return None
    if a == 1:
       rv2 = tuple.copy()
       rv2['c'] = rv2['c'] + 20
       rv2['d'] = None
       rv2['e'] = rv2['e'] + 20
       return [tuple, rv2]
    if a == None:
       return tuple['a'], tuple['b'] - 7
