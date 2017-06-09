# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import sys
import itertools

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

"""
Test that we can covert Python streams to SPL tuples.
"""

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestPython2SPL(unittest.TestCase):
    """ Test invocations handling of SPL schemas in Python ops.
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def test_object_to_schema(self):
        topo = Topology()
        s = topo.source([1,2,3])
        st = s.as_structured(lambda x : (x,), 'tuple<int32 x>')

        tester = Tester(topo)
        tester.contents(st, [{'x':1}, {'x':2}, {'x':3}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_to_schema(self):
        topo = Topology()
        s = topo.source(['a', 'b', 'c']).as_string()
        st = s.as_structured(lambda x : (x+'struct!',), 'tuple<rstring y>')

        tester = Tester(topo)
        tester.contents(st, [{'y':'astruct!'}, {'y':'bstruct!'}, {'y':'cstruct!'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_json_to_schema(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.as_structured(lambda x : (next(iter(x)), x[next(iter(x))]), 'tuple<rstring y, int32 x>')

        tester = Tester(topo)
        tester.contents(st, [{'y':'a', 'x':7}, {'y':'b', 'x':8}, {'y':'c', 'x':9}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_dict_to_schema(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.as_structured(lambda x : (next(iter(x)), x[next(iter(x))]), 'tuple<rstring y, int32 x>')

        st = st.as_structured(lambda x : (x['y'], x['x']+3), 'tuple<rstring id, int32 value>')

        tester = Tester(topo)
        tester.contents(st, [{'id':'a', 'value':10}, {'id':'b', 'value':11}, {'id':'c', 'value':12}])
        tester.test(self.test_ctxtype, self.test_config)
