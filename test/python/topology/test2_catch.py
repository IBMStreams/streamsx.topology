# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
import streamsx.spl.op as op
from typing import NamedTuple

"""
Test catch_exceptions function
"""

class NumbersSchema(NamedTuple):
    num: int


class TestCatch(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def test_params(self):
        topo = Topology('test_params')
        s = topo.source(['0'])

        m_stream = s.map(lambda t: t)
        with self.assertRaises(ValueError):
           m_stream.catch_exceptions('wrong')
        with self.assertRaises(ValueError):
           m_stream.catch_exceptions(exception_type=None)
        self.assertTrue(m_stream.catch_exceptions('all', True, True))
        self.assertTrue(m_stream.catch_exceptions(exception_type='streams', tuple_trace=False, stack_trace=False))
        self.assertTrue(m_stream.catch_exceptions(exception_type='std', tuple_trace=True, stack_trace=False))
        self.assertTrue(m_stream.catch_exceptions('none'))

    def test_python_operator(self):
        topo = Topology('test_python_operator')
        s = topo.source(['0','1','2','3','4','five','6','7','8','9']).as_string()

        num_stream = s.map(lambda t: {'num': int(t)}, schema=NumbersSchema)
        num_stream.catch_exceptions()
        
        tester = Tester(topo)
        tester.tuple_count(num_stream, 10-1)
        tester.test(self.test_ctxtype, self.test_config)

    def test_functor_operator(self):
        topo = Topology('test_functor_operator')
        s = topo.source(['0','1','2','3','4','five','6','7','8','9']).as_string()

        f = op.Map('spl.relational::Functor', s, schema='tuple<int64 num>')
        f.num = f.output('(int64) string')
        num_stream = f.stream
        num_stream.catch_exceptions(tuple_trace=True)
        
        tester = Tester(topo)
        tester.tuple_count(num_stream, 10-1)
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedCatch(TestCatch):
    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False

class TestSasCatch(TestCatch):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


