# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017

import itertools

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test Source functions for optional types
#------------------------------------------------------------------

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pysource.opttype"

# Returns a value matching the test schema PYTHON_OPTIONAL_TYPES_SCHEMA
@spl.source()
class SpecificValues:
    def __init__(self):
        pass

    def __iter__(self):
        rv = (
           123, None,
           ["a", "b"], None
           )
        return itertools.repeat(rv, 1)

@spl.source()
class SparseTuple:
    def __init__(self):
        pass

    def __iter__(self):
        rv = (
           37, None, 23, -46, None, 56, 67, 78
           )
        return itertools.repeat(rv, 1)

@spl.source()
class DictTuple:
    def __init__(self):
        pass

    def __iter__(self):
        rv1 = dict()
        rv1['a'] = 3245;
        rv1['c'] = 93;
        rv1['d'] = 1234;
        rv1['f'] = -25;

        rv2 = dict()
        rv2['a'] = 831;
        rv2['b'] = 421;
        rv2['d'] = -4455;

        rv3 = (1,2,3,4,5)

        rv4 = dict()
        rv4['b'] = -32;
        rv4['d'] = None;
        rv4['e'] = -64;
        
        return iter([rv1, rv2, [rv3, rv4]])
