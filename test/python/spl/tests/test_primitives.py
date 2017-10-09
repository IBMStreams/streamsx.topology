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

    def _get_metric(self, name):
        job = self.tester.submission_result.job
        ops = job.get_operators()
        self.assertEqual(1, len(ops))
        ms = ops[0].get_metrics(name=name)
        self.assertEqual(1, len(ms))
        m = ms[0]
        self.assertEqual(name, m.name)
        return m 

    def _noports_check(self):
        job = self.tester.submission_result.job
        ops = job.get_operators()
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

    def _single_input_port_check(self):
        job = self.tester.submission_result.job
        top = None
        for op in job.get_operators():
            if op.name == 'SIP_OP':
                top = op
                break
        self.assertIsNot(None, top)

        ms = top.get_metrics(name='SIP_METRIC')
        self.assertEqual(1, len(ms))
        m = ms[0]
        self.assertEqual('SIP_METRIC', m.name)

        import time
        for i in range(10):
            print(i, "Metric value", m.value, flush=True)
            if m.value == 1043:
                break
            time.sleep(1.0)
            print("About to refresh", flush=True)
            m = top.get_metrics(name='SIP_METRIC')[0]
        self.assertEqual(1043, m.value)

    def test_single_input_ports(self):
        """Operator with one input port"""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, '../testtkpy')
        s = topo.source([1043])
        s = s.map(lambda x : (x,), schema='tuple<uint64 v>')
        bop = op.Sink("com.ibm.streamsx.topology.pytest.pyprimitives::SingleInputPort", s, name="SIP_OP")

        self.tester = Tester(topo)
        self.tester.local_check = self._single_input_port_check
        self.tester.test(self.test_ctxtype, self.test_config)
