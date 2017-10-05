# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import os
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit

class TestKWArgs(unittest.TestCase):
    """ 
    Test decorated operators with **kwargs
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def test_filer_for_each(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, '../testtkpy')
        s = topo.source([(1,2,3), (1,2,201), (2,2,301), (3,4,401), (5,6,78), (8,6,501), (803, 9324, 901)])
        sch = 'tuple<int32 a, int32 b, int32 c>'
        s = s.map(lambda x : x, schema=sch)
        bop = op.Map("com.ibm.streamsx.topology.pytest.pykwargs::KWFilter", s)

        op.Sink("com.ibm.streamsx.topology.pytest.pykwargs::KWForEach", bop.stream)
        
        tester = Tester(topo)
        tester.contents(bop.stream, [{'a':1, 'b':2, 'c':201}, {'a':3, 'b':4, 'c':401}, {'a':803, 'b':9324, 'c':901}])
        tester.test(self.test_ctxtype, self.test_config)
