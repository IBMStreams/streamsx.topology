from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.tester_runtime import _PythonCondition
from streamsx.topology.consistent import ConsistentRegionConfig
import streamsx.spl.op as op
import streamsx.ec as ec

import unittest
from datetime import timedelta

# Tests for consistent region.

# Include at least one of each of these basic types, including checkpointing
# and a dillable class:
# Aggregate.
# Filter.
# FlatMap.
# ForEach
# HashAdder
# Map.
# Source.

# Class defining a source of integers from 0 to the limit, including 0 but
# excluding the limit.
# An instance of this class can be dilled.
class TimeCounter(object):
    """Count up from zero every `period` seconds for a given number of 
    iterations."""
    def __init__(self, period=None, iterations=None):
        if period is None:
            period = 1.0

        self.period = period
        self.iterations = iterations
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        # If the number of iterations has been met, stop iterating.
        if self.iterations is not None and self.count >= self.iterations:
            raise StopIteration

        # Otherwise increment, sleep, and return.
        to_return = self.count
        self.count += 1
        time.sleep(self.period)
        return to_return

    def next(self):
        return self.__next__()

# This just provides a __call__ method that computes the average of its
# parameter, but since it is a class it is stateful and will have its state
# saved during checkpointing.
class StatefulAverage(object):
    def __call__(self, x):
        return float(sum(x))/float(len(x))

# Pass only even integers.  This is a class, therefore is checkpointed
# even though it does not have meaningful state.
class StatefulEvenFilter(object):
    def __call__(self, x):
        return x % 2 == 0

# Map x -> x / 2 + 1.  Again this is stateful although it does not have
# meaningful state.
class StatefulHalfPlusOne(object):
    def __call__(self, x):
        return x / 2 + 1

# Split a string.
class StatefulSplit(object):
    def __call__(self, x):
        return x.split()

# Delay.  Used as a filter, but this just delays for a period and returns
# True
class StatefulDelay(object):
    def __init__(self, period):
        self.period = period

    def __call__(self, x):
        time.sleep(self.period)
        return True

# Verify that tuples are received in strict numeric order.
class VerifyNumericOrder(object):
    def __init__(self):
        self._expected = 0
    def __call__(self, x):
        if x == self._expected:
             self._expected += 1
        else:
            raise ValueError("Expected " + str(self._expected) + " got " + x)

# Verify that __enter__ and __exit__ are called, alternately, a specified 
# number of times.
class VerifyEnterExit(_PythonCondition):
    def __init__(self, expected_minimum, name):
        super(VerifyEnterExit, self).__init__(name)
        self.expected_minimum = expected_minimum

    def __call__(self, _):
        pass

    def __enter__(self):
        super(VerifyEnterExit, self).__enter__()

        self._metric_enter_count = self._create_metric("enterCount", ec.MetricKind.Counter)
        self._metric_exit_count = self._create_metric("exitCount", ec.MetricKind.Counter)

        self._metric_enter_count += 1
        enter_count = self._metric_enter_count.value
        exit_count = self._metric_exit_count.value
        # enter count should be exactly one more than exit count
        if enter_count != exit_count + 1:
            self.fail()
        if enter_count >= self.expected_minimum:
            self.valid = True
            # This verifies that the number of enter calls is at least
            # the expected number, and that the number of exit calls is
            # exactly one less than the number of enter calls.  The
            # remaining exit call should happen on shutdown.  The test 
            # really should verify that the final exit call happens, but
            # I don't see a way to do that.

    def __exit__(self, exc_type, exc_value, traceback):
        ret = super(VerifyEnterExit, self).__exit__(exc_type, exc_value, traceback)
        self._metric_exit_count += 1
        return ret

    def __str__(self):
        enter_count = self._metric_enter_count.value
        exit_count = self._metric_exit_count.value
        return "Verify enter and exit: expected:" + str(self.expected_minimum) + " received: " + str(enter_count) + " enter and " + str(exit_count) + " exit."

# Compute a hash code, statefully.  Again the state is not meaningful
class StatefulStupidHash(object):
    def __call__(self, x):
        return hash(x) + 89

# listiterater objects cannot be pickled in python 2.7, so here is a
# list iterator class
class ListIterator(object):
    def __init__(self, listToIterate, period=None):
        self.listToIterate = listToIterate
        if period is None:
            self.period=0.1
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.listToIterate):
            raise StopIteration
        time.sleep(self.period);
        ret = self.listToIterate[self.index];
        self.index += 1
        return ret

    def next(self):
        return __next__(self)
    

# Consistent region is not supported in standalone.  Note that neither
# checkpointing nor consistent region can be used in standalone, but 
# if checkpointing is enabled in standalone, there is a warning message
# but the application runs, while if consistent region is enabled, there
# is an error and the application stops.  There is an inconsistency about
# how we handle the unsupported configuration.

#class TestConsistentRegion(unittest.TestCase): 
#    def setUp(self):
#        Tester.setup_standalone(self)

class TestDistributedConsistentRegion(unittest.TestCase):
    def setUp(self):
        Tester.setup_distributed(self)

    # Source operator
    def test_source(self):
        topo = Topology("test")
        
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        s.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        tester = Tester(topo)
        tester.contents(s, range(0,30))
        tester.resets(3)

#        cfg={}
#        job_config = streamsx.topology.context.JobConfig(tracing='debug')
#        job_config.add(self.test_config)

        tester.test(self.test_ctxtype, self.test_config)

    # Source, ForEach, Filter, Aggregate operators
    # (based on test2_python_window.TestPythonWindowing.test_basicCountCountWindow)
    def test_aggregate(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        s.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        tester = Tester(topo)
        tester.resets(3)
        tester.contents(s, [1.5,2.5,3.5,4.5,5.5,7.5,9.5])
        tester.test(self.test_ctxtype, self.test_config)
        

    # Test flat map (based on a sample)
    def test_flat_map(self):
        topo = Topology();

        lines = topo.source(ListIterator(["mary had a little lamb", "its fleece was white as snow"]))
        lines.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        # slow things down so checkpoints can be taken.
        lines = lines.filter(StatefulDelay(0.5)) 
        words = lines.flat_map(StatefulSplit())
        tester = Tester(topo)
        tester.resets(3)
        tester.contents(words, ["mary","had","a","little","lamb","its","fleece","was","white","as","snow"])
        tester.test(self.test_ctxtype, self.test_config)

    # Test hash adder.  This requires parallel with a hash router.
    # (based on test2_udp.TestUDP.test_TopologyParallelHash)
    def test_hash_adder(self):
        topo = Topology("test_hash_adder")
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        s.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        width =  3
        s = s.parallel(width, Routing.HASH_PARTITIONED, StatefulStupidHash())
        s = s.map(lambda x: x + 23)
        s = s.end_parallel()

        expected = []
        for v in range (0,30):
            expected.append((v + 23))

        tester = Tester(topo)
        tester.resets(3)
        tester.contents(s, expected, ordered=width==1)
        tester.test(self.test_ctxtype, self.test_config)
            
    # Test for_each.  This is a sink. 
    def test_for_each(self):
        N = 3000
        topo = Topology("test")
        s = topo.source(TimeCounter(iterations=N, period=0.01))
        s.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))

        s.for_each(VerifyNumericOrder())
        tester = Tester(topo)
        tester.resets()
        tester.test(self.test_ctxtype, self.test_config)

    def test_enter_exit(self):
        iterations = 3000
        reset_count = 10
        topo = Topology("test")
        s = topo.source(TimeCounter(iterations=iterations, period=0.01))
        s.set_consistent(ConsistentRegionConfig.periodic(1, drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=6))
        
        v = VerifyEnterExit(reset_count, "VerifyEnterExit")
        tester = Tester(topo)
        tester.resets(reset_count)
        tester.add_condition(s, v)

        cfg={}
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

    def test_aggregate(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        s.set_consistent(ConsistentRegionConfig.operator_driven(drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_filter(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        s.set_consistent(ConsistentRegionConfig.operator_driven(drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_flat_map(self):
        topo = Topology();

        lines = topo.source(ListIterator(["mary had a little lamb", "its fleece was white as snow"]))

        # slow things down so checkpoints can be taken.
        lines = lines.filter(StatefulDelay(0.5)) 
        words = lines.flat_map(StatefulSplit())
        words.set_consistent(ConsistentRegionConfig.operator_driven(drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_for_each(self):
        pass # There is no way to set consistent on a sink

    def test_hash_adder(self):
        pass # There is no way to set consistent on hash_adder

    def test_map(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s.set_consistent(ConsistentRegionConfig.operator_driven(drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    # Source operator
    def test_source(self):
        topo = Topology("test")
        
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        s.set_consistent(ConsistentRegionConfig.operator_driven(drainTimeout=40, resetTimeout=40, maxConsecutiveAttempts=3))
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

