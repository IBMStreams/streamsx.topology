from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit
from streamsx.topology.state import ConsistentRegionConfig

import spl_tests_utils as stu

import unittest
from datetime import timedelta

# Test consistent region in python decorated operators.

# There should be at least one test for each type of python decorated
# operators: source, map, filter, for_each, primitive_operator

# Consistent region is not supported in standalone

class TestDistributedConsistentRegion(unittest.TestCase):
    def setUp(self):
        Tester.setup_distributed(self)

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    # Source operator
    def test_source(self):
        iterations = 3000
        topo = Topology()

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':iterations,'period':0.01})

        s = bop.stream
        s.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))
        
        tester = Tester(topo)
        tester.resets()
        tester.tuple_count(s, iterations)
        tester.contents(s, list(zip(range(0,iterations))))

        # job_config = streamsx.topology.context.JobConfig(tracing='debug')
        # job_config.add(self.test_config)

        tester.test(self.test_ctxtype, self.test_config)

    # Source, filter, and map operators
    def test_filter_map(self):
        iterations = 3000
        topo = Topology()

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':iterations,'period':0.01})
        timeCounter.stream.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))
 
        evenFilter = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulEvenFilter", timeCounter.stream, None, params={})
        hpo = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulHalfPlusOne", evenFilter.stream, None, params={})
        s = hpo.stream
        tester = Tester(topo)
        tester.resets()
        tester.tuple_count(s, iterations/2)
        tester.contents(s, list(zip(range(1,int((iterations/2)+1)))))

        tester.test(self.test_ctxtype, self.test_config)

    # source, primitive, and for_each operators
    # this will fail to compile because checkpointing is not supported
    # for python primitive operators.
    @unittest.expectedFailure
    def test_primitive_foreach(self):
        iterations=3000
        topo = Topology()

        topo.checkpoint_period = timedelta(seconds=1)
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':iterations,'period':0.01})
        timeCounter.stream.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        fizzbuzz = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::FizzBuzzPrimitive", timeCounter.stream, schema.StreamSchema('tuple<int32 f, rstring c>').as_tuple())
        verify = op.Sink("com.ibm.streamsx.topology.pytest.checkpoint::Verify", fizzbuzz.stream)
        s = fizzbuzz.stream
        tester = Tester(topo)
        tester.resets()
        tester.tuple_count(s, iterations)

        tester.test(self.test_ctxtype, self.test_config)

    # source, map, and for_each operators
    def test_map_foreach(self):
        iterations = 3000
        topo = Topology()

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':iterations,'period':0.01})
        timeCounter.stream.set_consistent(ConsistentRegionConfig.periodic(5 , drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        fizzbuzz = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::FizzBuzzMap", timeCounter.stream, schema.StreamSchema('tuple<int32 f, rstring c>').as_tuple())
        verify = op.Sink("com.ibm.streamsx.topology.pytest.checkpoint::Verify", fizzbuzz.stream)
        s = fizzbuzz.stream

        tester = Tester(topo)
        tester.resets()
        tester.tuple_count(s, iterations)
        # Find the expected results.
        fizz=lambda x: (x[0], x[1]+'fizz' if x[0] % 3 == 0 else x[1])
        buzz=lambda x: (x[0], x[1]+'buzz' if x[0] % 5 == 0 else x[1])
        expected = list (map (buzz, (map (fizz, (map (lambda x: (x,''), range(iterations)))))))
        tester.contents(s, expected)

        tester.test(self.test_ctxtype, self.test_config)

    def test_enter_exit(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        source = op.Source(topo, 'com.ibm.streamsx.topology.pytest.checkpoint::EnterExitSource', schema.StreamSchema('tuple<rstring from, int32 enter, int32 exit>').as_tuple(), params={'period':0.1})
        source.stream.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        transit = op.Map('com.ibm.streamsx.topology.pytest.checkpoint::EnterExitMap', source.stream, schema.StreamSchema('tuple<rstring from, int32 enter, int32 exit>').as_tuple())

        tester = Tester(topo)
        tester.resets(10)

        # On each operator, __enter__ and __exit__ should be called once for 
        # each reset.  Also __enter__ should be called at startup and __exit__
        # at shutdown.  It is hard to verify the final __exit__ call (and that
        # is handled by python rather than our code), so 
        # the test is valid if the number of __enter__ calls is one more than
        # the number of resets, and the number of __exit__ calls is equal to
        # number of resets.  The tuples on the two streams indicate the number
        # of times __enter__ and __exit__ have been called. 
        # We are looking for two specific tuples:
        # ('source', 6, 5) and ('transit', 6, 5)
        tester.eventual_result(source.stream, lambda tuple_ : True if tuple_[1] >= 6 and tuple_[1] == tuple_[2] + 1 else Fale if tuple_[1] != tuple_[2] + 1 else None)
        tester.eventual_result(transit.stream, lambda tuple_ : True if tuple_[1] >= 6 and tuple_[1] == tuple_[2] + 1 else Fale if tuple_[1] != tuple_[2] + 1 else None)

        job_config = streamsx.topology.context.JobConfig(tracing='debug')
        job_config.add(self.test_config)

        tester.test(self.test_ctxtype, self.test_config)


class TestSasConsistentRegion(TestDistributedConsistentRegion):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

# Python operators may not be the source of operator-driven consistent regions.
class TestOperatorDriven(unittest.TestCase):
    def setUp(self):
        Tester.setup_distributed(self)

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def test_filter(self):
        topo = Topology()

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
    
        evenFilter = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulEvenFilter", timeCounter.stream, None, params={})
        evenFilter.stream.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        
        hpo = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulHalfPlusOne", evenFilter.stream, None, params={})
        s = hpo.stream
        tester = Tester(topo)

        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_pipe(self):
        topo = Topology()

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        timeCounter = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})

        evenFilter = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulEvenFilter", timeCounter.stream, None, params={})
        
        hpo = op.Map("com.ibm.streamsx.topology.pytest.checkpoint::StatefulHalfPlusOne", evenFilter.stream, None, params={})
        hpo.stream.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        s = hpo.stream
        tester = Tester(topo)

        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_sink(self):
        pass # No way to set consistent on sink

    def test_source(self):

        topo = Topology()

        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})

        s = bop.stream
        s.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
         
        tester = Tester(topo)

        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))


    def test_primitive(self):
        # TODO - if support for a primitive python operator in a consistent
        # region is added, add this test.
        pass
        
    def test_beacon(self):
        # An operator-driven consistent region can be used with a source
        # that supports it, such as Beacon
        iterations = 5000
        topo = Topology()

        beacon = op.Source(topo, "spl.utility::Beacon", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':iterations,'period':0.01,'triggerCount':streamsx.spl.types.uint32(500)})
        beacon.f = beacon.output('(int32)IterationCount()')

        s = beacon.stream
        s.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=4))
         
        tester = Tester(topo)
        # For operator-driven regions, the resetter uses a random interval
        # from 10-40 seconds for resets.  Only one is likely to be completed
        # while processing tuples for this test.
        tester.resets(1)
        tester.tuple_count(s, iterations)
        tester.contents(s, list(zip(range(0,iterations))))

        tester.test(self.test_ctxtype, self.test_config)
