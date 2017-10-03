# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
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

class TestPrimitives(unittest.TestCase):
    """ 
    Test @spl.primitive_operator decorated operators
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def _noports_check(self):
        job = self.tester.submission_result.job
        import time
        ops = job.get_operators()
        print(ops, flush=True)
        self.assertEqual(1, len(ops))
        ms = ops[0].get_metrics(name='NP_mymetric')
        self.assertEqual(1, len(ms))
        m = ms[0]
        self.assertEqual('NP_mymetric', m.name)
        self.assertEqual(89, m.value)

    def test_noports(self):
        """Operator with no inputs or outputs"""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, '../testtkpy')
        bop = op.Invoke(topo, "com.ibm.streamsx.topology.pytest.pyprimitives::NoPorts", params = {'mn': 'mymetric', 'iv':89})

        self.tester = Tester(topo)
        self.tester.local_check = self._noports_check
        self.tester.test(self.test_ctxtype, self.test_config)
