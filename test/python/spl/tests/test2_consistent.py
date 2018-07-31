from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit
from streamsx.topology.consistent import ConsistentRegionConfig

import spl_tests_utils as stu

import unittest
from datetime import timedelta

# Test consistent region in python decorated operators.

# There should be at least one test for each type of python decorated
# operators: source, map, filter, for_each, primitive_operator

# Consistent region is not supported in standalone
#class TestConsistentRegion(unittest.TestCase):
#    def setUp(self):
#        Tester.setup_standalone(self)

class TestDistributedConsistentRegion(unittest.TestCase):
    def setUp(self):
        Tester.setup_distributed(self)

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def add_resetter(self, topo, minimumResets=3): 
        params = {'minimumResets': minimumResets, 'conditionName': 'resetter'}
        resetter = op.Invoke(topo, "com.ibm.streamsx.topology.testing.consistent::Resetter", inputs=None, schemas=None, params=params, name="ConsistentRegionResetter")
        return resetter

    # Source operator
    def test_source(self):
        topo = Topology("test")
        self.add_resetter(topo)

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})

        s = bop.stream
        s.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
         
        tester = Tester(topo)
        tester.tuple_count(s, 30)
        tester.contents(s, list(zip(range(0,30))))

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    # Source, filter, and map operators
    def test_filter_map(self):
        topo = Topology("test")
        self.add_resetter(topo)

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        timeCounter.stream.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
 
        evenFilter = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulEvenFilter", timeCounter.stream, None, params={})
        hpo = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulHalfPlusOne", evenFilter.stream, None, params={})
        s = hpo.stream
        tester = Tester(topo)
        tester.tuple_count(s, 15)
        tester.contents(s, list(zip(range(1,16))))

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    # source, primitive, and for_each operators
    # this will fail to compile because checkpointing is not supported
    # for python primitive operators.
    @unittest.expectedFailure
    def test_primitive_foreach(self):
        topo = Topology("test")
        self.add_resetter(topo)

        topo.checkpoint_period = timedelta(seconds=1)
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        timeCounter.stream.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        fizzbuzz = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::FizzBuzzPrimitive", timeCounter.stream, schema.StreamSchema('tuple<int32 f, rstring c>').as_tuple())
        verify = op.Sink("com.ibm.streamsx.topology.pytest.checkpoint::Verify", fizzbuzz.stream)
        s = fizzbuzz.stream
        tester = Tester(topo)
        tester.tuple_count(s, 30)
        tester.test(self.test_ctxtype, self.test_config)

    # source, map, and for_each operators
    def test_map_foreach(self):
        topo = Topology("test")
        self.add_resetter(topo)

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        timeCounter.stream.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        fizzbuzz = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::FizzBuzzMap", timeCounter.stream, schema.StreamSchema('tuple<int32 f, rstring c>').as_tuple())
        verify = op.Sink("com.ibm.streamsx.topology.pytest.checkpoint::Verify", fizzbuzz.stream)
        s = fizzbuzz.stream
        tester = Tester(topo)
        tester.tuple_count(s, 30)
        tester.test(self.test_ctxtype, self.test_config)


class TestSasConsistentRegion(TestDistributedConsistentRegion):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
