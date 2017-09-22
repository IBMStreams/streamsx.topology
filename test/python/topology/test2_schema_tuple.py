# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.schema import StreamSchema
from streamsx.topology.tester import Tester

"""
Test that structured schemas can be passed into Python functions as tuples.
"""

def check_is_tuple(t):
    if type(t) != tuple:
        raise ValueError("Expected a tuple:" + str(t) + " >> type:" + str(type(t)))

def check_is_tuple_for_each(t):
    check_is_tuple(t)
    if t[1] != str(t[0]*2) + "Hi!":
        raise ValueError("Incorrect value:" + str(t))

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestSchemaTuple(unittest.TestCase):
    """ Test invocations handling of SPL schemas in Python ops.
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def test_as_tuple_for_each(self):
        topo = Topology()
        s = topo.source([1,2,3])
        schema=StreamSchema('tuple<int32 x, rstring msg>').as_tuple()
        st = s.map(lambda x : (x,str(x*2) + "Hi!"), schema=schema)
        st.for_each(check_is_tuple_for_each)

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)