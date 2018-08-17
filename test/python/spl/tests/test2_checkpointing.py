import unittest
from datetime import timedelta

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit

import spl_tests_utils as stu

# Test checkpointing in python decorated operators.
# These tests run in a way that should cause checkpoints to be created,
# but do not actually verify that the checkpoints are created and does
# not even attempt to restore them.

# There should be at least one test for each type of python decorated
# operators: source, map, filter, for_each, primitive_operator

class TestCheckpointing(unittest.TestCase):
    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    # Source operator
    def test_source(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        s = bop.stream
        tester = Tester(topo)
        tester.tuple_count(s, 30)
        #tester.contents(s, range(0,30))  # why doesn't this work?
        tester.contents(s, list(zip(range(0,30))))

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    # Source, filter, and map operators
    def test_filter_map(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        evenFilter = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulEvenFilter", timeCounter.stream, None, params={})
        hpo = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulHalfPlusOne", evenFilter.stream, None, params={})
        s = hpo.stream
        tester = Tester(topo)
        tester.tuple_count(s, 15)
        tester.contents(s, list(zip(range(1,16))))

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    # source, primitive, and for_each operators
    def test_primitive_foreach(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        fizzbuzz = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::FizzBuzz", timeCounter.stream, schema.StreamSchema('tuple<int32 f, rstring c>').as_tuple())
        verify = op.Sink("com.ibm.streamsx.topology.pytest.checkpoint::Verify", fizzbuzz.stream)
        s = fizzbuzz.stream
        tester = Tester(topo)
        tester.tuple_count(s, 30)
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

class TestDistributedCheckpointing(TestCheckpointing):
    def setUp(self):
        Tester.setup_distributed(self)

class TestSasCheckpointing(TestCheckpointing):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
