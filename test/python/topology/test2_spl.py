# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op


@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestSPL(unittest.TestCase):
    """ Test invocations of SPL operators from Python topology.
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def test_SPLBeaconFilter(self):
        """Test a Source and a Map operator.
           Including an output clause.
        """
        topo = Topology('test_SPLBeaconFilter')
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.2, 'iterations':100})
        s.seq = s.output('IterationCount()')

        f = op.Map('spl.relational::Filter', s.stream,
            params = {'filter': op.Expression.expression('seq % 2ul == 0ul')})

        tester = Tester(topo)
        tester.tuple_count(f.stream, 50)
        tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestDistributedSPL(TestSPL):
    def setUp(self):
        Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestBluemixSPL(TestSPL):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        # Ensure the old name still works.
        self.test_ctxtype = "ANALYTICS_SERVICE"
