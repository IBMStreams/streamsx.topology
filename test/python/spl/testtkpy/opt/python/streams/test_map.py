# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test Map functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pymap"

##
##
@spl.map()
class OffByOne(object):
    def __init__(self):
        self.last = None

    def __call__(self, **tuple):
        rv = self.last
        if self.last is not None:
            rv = (rv["a"], rv["b"], rv["vl"])
        self.last = tuple
        return rv

@spl.map()
class SparseTupleMap(object):
    def __init__(self):
       pass

    def __call__(self, *t):
        return (t[0]+81, 23, None, None, 34) 

@spl.map()
def DictTupleMap(**tuple):
    a = tuple['a']
    if a == 3245:
        tuple['b'] = tuple['c'] + 27
        tuple.pop('c')
        return tuple
    if a == 831:
        return None
    if a == 1:
       rv2 = tuple.copy()
       rv2['c'] = rv2['c'] + 20
       rv2['d'] = rv2['d'] + 20
       rv2['e'] = rv2['e'] + 20
       return [tuple, rv2]
    if a == 0:
       return tuple['a'], tuple['b'] - 7
