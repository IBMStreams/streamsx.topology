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
class OffByOne:
    def __init__(self):
        self.last = None

    def __call__(self, **tuple):
        rv = self.last
        if self.last is not None:
            rv = (rv["a"], rv["b"], rv["vl"])
        self.last = tuple
        return rv

@spl.map()
class SparseTupleMap:
    def __init__(self):
       pass

    def __call__(self, *t):
        return (t[0]+81, 23, None, None, 34) 
