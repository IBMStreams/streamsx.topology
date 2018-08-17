# coding=utf-8
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
import streamsx.scripts.extract

import spl_tests_utils as stu

class TestPrimitives(unittest.TestCase):
    """ 
    Test @spl.primitive_operator decorated operators
    """

    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

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
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
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
            if m.value == 1060:
                break
            time.sleep(1.0)
            m = top.get_metrics(name='SIP_METRIC')[0]
        self.assertEqual(1060, m.value)

    def test_single_input_port(self):
        """Operator with one input port"""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        s = topo.source([1043])
        s = s.map(lambda x : (x,), schema='tuple<uint64 v>')
        bop = op.Sink("com.ibm.streamsx.topology.pytest.pyprimitives::SingleInputPort", s, name="SIP_OP")

        self.tester = Tester(topo)
        self.tester.local_check = self._single_input_port_check
        self.tester.test(self.test_ctxtype, self.test_config)

    def _multi_input_port_check(self):
        job = self.tester.submission_result.job
        top = None
        for op in job.get_operators():
            if op.name == 'MIP_OP':
                top = op
                break
        self.assertIsNot(None, top)

        ms = top.get_metrics(name='MIP_METRIC_0')
        self.assertEqual(1, len(ms))
        m0 = ms[0]
        self.assertEqual('MIP_METRIC_0', m0.name)

        ms = top.get_metrics(name='MIP_METRIC_1')
        self.assertEqual(1, len(ms))
        m1 = ms[0]
        self.assertEqual('MIP_METRIC_1', m1.name)

        ms = top.get_metrics(name='MIP_METRIC_2')
        self.assertEqual(1, len(ms))
        m2 = ms[0]
        self.assertEqual('MIP_METRIC_2', m2.name)

        import time
        for i in range(10):
            if m0.value == 9081 and m1.value == 379 and m2.value == -899:
                break
            time.sleep(1.0)
            m0 = top.get_metrics(name='MIP_METRIC_0')[0]
            m1 = top.get_metrics(name='MIP_METRIC_1')[0]
            m2 = top.get_metrics(name='MIP_METRIC_2')[0]

        self.assertEqual(9054 + 17, m0.value)
        self.assertEqual(345 + 34, m1.value)
        self.assertEqual(-953 + 51, m2.value)

    def test_multi_input_ports(self):
        """Operator with three input ports"""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        s0 = topo.source([9054]).map(lambda x : (x,), schema='tuple<uint64 v>')
        s1 = topo.source([345]).map(lambda x : (x,), schema='tuple<int64 v>')
        s2 = topo.source([-953]).map(lambda x : (x,), schema='tuple<int32 v>')
        bop = op.Invoke(topo, "com.ibm.streamsx.topology.pytest.pyprimitives::MultiInputPort", [s0,s1,s2], name="MIP_OP")

        self.tester = Tester(topo)
        self.tester.local_check = self._multi_input_port_check
        self.tester.test(self.test_ctxtype, self.test_config)

# With output ports it's easier to test thus can use standalone.
#
class TestPrimitivesOutputs(unittest.TestCase):
    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    def test_single_output_port(self):
        """Operator with single output port."""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        s = topo.source([9237, -24])
        s = s.map(lambda x : (x,), schema='tuple<int64 v>')

        bop = op.Map("com.ibm.streamsx.topology.pytest.pyprimitives::SingleOutputPort", s)

        r = bop.stream
    
        self.tester = Tester(topo)
        self.tester.tuple_count(s, 2)
        self.tester.contents(s, [{'v':9237}, {'v':-24}])
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_multi_output_ports(self):
        """Operator with multiple output port."""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        s = topo.source([9237, -24])
        s = s.map(lambda x : (x,), schema='tuple<int64 v>')

        bop = op.Invoke(topo, "com.ibm.streamsx.topology.pytest.pyprimitives::MultiOutputPorts", s, schemas=['tuple<int64 v1>', 'tuple<int32 v2>', 'tuple<int16 v3>'])

        r = bop.outputs
    
        self.tester = Tester(topo)
        self.tester.tuple_count(s, 2)
        self.tester.contents(r[0], [{'v1':9237}, {'v1':-24}])
        self.tester.contents(r[1], [{'v2':9237+921}, {'v2':-24+921}])
        self.tester.contents(r[2], [{'v3':9237-407}, {'v3':-24-407}])
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_dict_output_ports(self):
        """Operator with multiple output port submitting dict objects."""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        s = topo.source([9237, -24])
        s = s.map(lambda x : (x,x*2,x+4), schema='tuple<int64 d, int64 e, int64 f>')

        bop = op.Invoke(topo, "com.ibm.streamsx.topology.pytest.pyprimitives::DictOutputPorts", s, schemas=['tuple<int64 d, int64 e, int64 f>']*2)

        r = bop.outputs
    
        self.tester = Tester(topo)
        self.tester.tuple_count(r[0], 2)
        self.tester.tuple_count(r[1], 4)
        self.tester.contents(r[0], [{'d':9237, 'e':(9237*2), 'f':9237+4}, {'d':-24, 'e':(-24*2), 'f':-24+4}])
        self.tester.contents(r[1], [{'d':9237+7, 'f':(9237*2)+777, 'e':9237+4+77}, {'d':9237, 'e':(9237*2), 'f':9237+4}, {'d':-24+7, 'f':(-24*2)+777, 'e':-24+4+77}, {'d':-24, 'e':(-24*2), 'f':-24+4}])
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_input_by_position(self):
        """Operator with input by position"""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        s = topo.source([3642, -393])
        s = s.map(lambda x : (x,x*2,x+4), schema='tuple<int64 d, int64 e, int64 f>')

        bop = op.Map("com.ibm.streamsx.topology.pytest.pyprimitives::InputByPosition", s)

        r = bop.stream
    
        self.tester = Tester(topo)
        self.tester.tuple_count(r, 2)
        self.tester.contents(r, [{'d':3642, 'e':(3642*2)+89, 'f':-92}, {'d':-393, 'e':(-393*2)+89, 'f':-92}])
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_only_output_port(self):
        """Operator with single output port and no inputs."""
        count = 106
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        bop = op.Source(topo, "com.ibm.streamsx.topology.pytest.pyprimitives::OutputOnly", schema='tuple<int64 c>', params={'count':count})

        r = bop.stream
    
        self.tester = Tester(topo)
        self.tester.tuple_count(r, count)
        self.tester.contents(r, list({'c':i+501} for i in range(count)))
        self.tester.test(self.test_ctxtype, self.test_config)
