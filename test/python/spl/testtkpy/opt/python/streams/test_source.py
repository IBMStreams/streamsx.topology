# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

import itertools

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test Source functions
#------------------------------------------------------------------

def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pysource"

#
# Returns a value matching the test schema TEST_ALL_PYTHON_TYPES
@spl.source()
class SpecificValues:
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
           ["a", "Streams!", "2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm"],
           [345,-4578],
           [9983, -4647787587, 0],
           [87346],
           [45433674, 41876984848],
           [4.269986E+05, -8.072285E+02, -6.917091E-08, 7.735085E8]
           )
        return itertools.repeat(rv, 1)
