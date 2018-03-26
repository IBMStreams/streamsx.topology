# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017

import decimal
import itertools

# Import the SPL decorators
from streamsx.spl import spl
from streamsx.spl.types import Timestamp

#------------------------------------------------------------------
# Test Source functions
#------------------------------------------------------------------

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pysource"

# Should be ignored (bad namespace)
def splNamespace():
    return "453535351&%$="

#
# Returns a value matching the test schema ALL_PYTHON_TYPES_SCHEMA
@spl.source()
class SpecificValues(object):
    def __init__(self):
        pass

    def __iter__(self):
        rv = (
           True,
           23, -2525, 3252352, -2624565653,
           72, 6873, 43665588, 357568872,
           4367.34, -87657525334.22,
           "⡍⠔⠙⠖ ⡊ ⠙⠕⠝⠰⠞ ⠍⠑⠁⠝ ⠞⠕ ⠎⠁⠹ ⠹⠁⠞ ⡊ ⠅⠝⠪⠂ ⠕⠋ ⠍⠹",
           complex(-23.0, 325.38), complex(-35346.234, 952524.93),
           decimal.Decimal("3.459876E72"), 
           decimal.Decimal("4.515716038731674E-307"),
           decimal.Decimal("1.085059319410602846995696978141388E+5922"),
           Timestamp(781959759, 9320, 76),
           "binary blob 8322".encode('utf-8'),
           ["a", "Streams!", "2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm"],
           [345,-4578],
           [9983, -4647787587, 0],
           [87346],
           [45433674, 41876984848],
           [4.269986E+05, -8.072285E+02, -6.917091E-08, 7.735085E8],
           [765.46477e19],
           [True, False, 7],
           {},
           {},
           {'abc':35320, 'многоязычных':-236325}
           )
        return itertools.repeat(rv, 1)

@spl.source()
class SparseTuple(object):
    def __init__(self):
        pass

    def __iter__(self):
        rv = (
           37, None, None, -46
           )
        return itertools.repeat(rv, 1)

@spl.source()
class DictTuple(object):
    def __init__(self):
        pass

    def __iter__(self):
        rv1 = dict()
        rv1['a'] = 3245;
        rv1['c'] = 93;
        rv1['f'] = -25;

        rv2 = dict()
        rv2['a'] = 831;
        rv2['b'] = 421;
        rv2['d'] = -4455;

        rv3 = (1,2,3,4,5)

        rv4 = dict()
        rv4['b'] = -32;
        rv4['e'] = -64;
        
        return iter([rv1, rv2, [rv3, rv4]])
