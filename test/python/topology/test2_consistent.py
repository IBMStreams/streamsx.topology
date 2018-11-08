from streamsx.topology.context import ContextTypes
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.tester_runtime import _StreamCondition
from streamsx.topology.state import ConsistentRegionConfig
import streamsx.spl.op as op
import streamsx.ec as ec

import itertools
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

    def _done(self):
        return self.iterations is not None and self.count >= self.iterations

    def __next__(self):
        # If the number of iterations has been met, stop iterating.
        if self._done():
            self._metric2.value = 1
            raise StopIteration

        # Otherwise increment, sleep, and return.
        to_return = self.count
        self.count += 1
        self._metric.value = self.count
        time.sleep(self.period)
        return to_return

    def next(self):
        return self.__next__()

    def __enter__(self):
        self._metric = ec.CustomMetric(self, "nTuplesSent", "Logical tuples sent")
        self._metric.value = self.count
        self._metric2 = ec.CustomMetric(self, "stopped", "Logical tuples sent")
        self._metric2.value = int(self._done())

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __getstate__(self):
        state = self.__dict__.copy()
        if '_metric' in state:
            del state['_metric']
        if '_metric2' in state:
            del state['_metric2']
        return state

class StatefulAverage(object):
    def __init__(self):
        self.count = 0
    def __call__(self, x):
        if x:
            self.count += 1
            return self.count, float(sum(x))/float(len(x))

# Due to the timed nature can't check specific values.
class TimedStatefulAverageChecker(object):
    def __init__(self):
        self.count = 0
        self.last = 0.0
    def __call__(self, agg):
        self.count += 1
        if self.count != agg[0]:
            print("TimedStatefulAverageChecker", "Expected count", self.count, "got", agg[0])
            return False
        # Could fire consecutively without receiving more tuples.
        if self.last > agg[1]:
            print("TimedStatefulAverageChecker", "Expected >= ", self.last, "got", agg[1])
            return False
        self.last = agg[1]
        return True
        

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
class VerifyEnterExit(_StreamCondition):
    def __init__(self, expected_minimum, name):
        super(VerifyEnterExit, self).__init__(name)
        self.expected_minimum = expected_minimum

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
        return "VerifyEnterExit:" + str(self.expected_minimum)

# Compute a hash code, statefully.  Again the state is not meaningful
class StatefulStupidHash(object):
    def __call__(self, x):
        return hash(x) + 89

# listiterater objects cannot be pickled in python 2.7, so here is a
# list iterator class
class ListIterator(object):
    def __init__(self, listToIterate, period=None, count=1):
        self.listToIterate = listToIterate
        self.period = period
        self.index = 0
        self.count = count

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.listToIterate) * self.count:
            raise StopIteration
        if self.period is not None:
            time.sleep(self.period)
        ret = self.listToIterate[self.index % len(self.listToIterate)];
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

class TestWithoutConsistentRegion(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def is_cr(self):
        return self.test_ctxtype != ContextTypes.STANDALONE

    # Source operator
    def test_source(self):
        iterations=3000
        reset_count=5
        topo = Topology()
        
        s = topo.source(TimeCounter(iterations=iterations, period=0.01))
        if self.is_cr():
            s.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        tester = Tester(topo)
        tester.contents(s, range(0,iterations))
        if self.is_cr():
            tester.resets(reset_count)

        tester.test(self.test_ctxtype, self.test_config)

    # Source, ForEach, Filter, Aggregate operators
    # (based on test2_python_window.TestPythonWindowing.test_basicCountCountWindow)
    def test_aggregate(self):
        iterations = 3000
        reset_count = 5
        topo = Topology()
        # Generate integers from [0,3000)
        s = topo.source(TimeCounter(iterations=iterations, period=0.01))
        if self.is_cr():
            s.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,(iterations/2))
        s = s.map(StatefulHalfPlusOne())

        sc = s.last(10).trigger(3).aggregate(StatefulAverage())
        st = s.last(17).trigger(datetime.timedelta(seconds=2)).aggregate(StatefulAverage())

        # non-stateful aggregation functions
        nsc = s.last(19).trigger(13).aggregate(lambda tuples : sum(tuples))

        tester = Tester(topo)
        if self.is_cr():
            tester.resets(reset_count)

        # Find the expected results.
        # mimic the processing using Python builtins
        iv = filter(StatefulEvenFilter(), range(iterations))
        iv = list(map(StatefulHalfPlusOne(), iv))

        # Expected stateful averages sc,st
        sagg = StatefulAverage()
        ers = [ sagg(iv[0:i+3][-10:]) for i in range(0, 3*int(len(iv)/3), 3) ]
        tester.contents(sc, ers)

        tester.tuple_check(st, TimedStatefulAverageChecker())

        # Must eventually aggregate on the last 17 items in iv
        # but only if cr otherwise the final marker stops
        # the window before the final trigger
        if self.is_cr():
            tester.eventual_result(st, lambda av : True if av[1] == sagg(iv[-17:])[1] else None)

        # Expected non-stateful averages nsc
        ernsc = [ sum(iv[0:i+13][-19:]) for i in range(0, 13*int(len(iv)/13), 13) ]
        tester.contents(nsc, ernsc)

        tester.test(self.test_ctxtype, self.test_config)

    # Test flat map (based on a sample)
    def test_flat_map(self):
        count = 1000
        reset_count = 5
        topo = Topology();

        lines = topo.source(ListIterator(["All work","and no play","makes Jack","a dull boy"], period=0.01, count=count))

        if self.is_cr():
            lines.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        words = lines.flat_map(StatefulSplit())
        tester = Tester(topo)
        if self.is_cr():
            tester.resets(reset_count)

        # Find the expected results.
        flat_contents = ["All","work","and","no","play","makes","Jack","a","dull","boy"]
        # repeat the contents 1000 times.
        expected = []
        for i in range(count):
            expected.extend(flat_contents)
        tester.contents(words, expected)
        tester.test(self.test_ctxtype, self.test_config)

    # Test hash adder.  This requires parallel with a hash router.
    # (based on test2_udp.TestUDP.test_TopologyParallelHash)
    def test_hash_adder(self):
        iterations=3000
        reset_count=5
        topo = Topology()
        s = topo.source(TimeCounter(iterations=iterations, period=0.01))
        if self.is_cr():
            s.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        width = 3
        s = s.parallel(width, Routing.HASH_PARTITIONED, StatefulStupidHash())
        s = s.map(lambda x: x + 23)
        s = s.end_parallel()

        expected = [v + 23 for v in range(iterations)]

        tester = Tester(topo)
        if self.is_cr():
            tester.resets(reset_count)
        tester.contents(s, expected, ordered=width==1)
        tester.test(self.test_ctxtype, self.test_config)

    # Test for_each.  This is a sink. 
    def test_for_each(self):
        iterations = 3000
        reset_count = 5
        topo = Topology()
        s = topo.source(TimeCounter(iterations=iterations, period=0.01))
        if self.is_cr():
            s.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))

        s.for_each(VerifyNumericOrder())

        tester = Tester(topo)
        tester.contents(s, range(0,iterations))
        if self.is_cr():
            tester.resets(reset_count)
        tester.test(self.test_ctxtype, self.test_config)


class TestDistributedConsistentRegion(TestWithoutConsistentRegion):
    def setUp(self):
        Tester.setup_distributed(self)

    def test_enter_exit(self):
        iterations = 3000
        reset_count = 5
        topo = Topology()
        s = topo.source(TimeCounter(iterations=iterations, period=0.01))
        s.set_consistent(ConsistentRegionConfig.periodic(5, drain_timeout=40, reset_timeout=40, max_consecutive_attempts=6))
        
        v = VerifyEnterExit(reset_count + 1, "VerifyEnterExit")
        tester = Tester(topo)
        tester.contents(s, range(0,iterations))
        tester.resets(reset_count)
        tester.add_condition(s, v)

        tester.test(self.test_ctxtype, self.test_config)


class TestSasConsistentRegion(TestDistributedConsistentRegion):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


# Python operators may not be the source of operator-driven consistent regions.
class TestOperatorDriven(unittest.TestCase):
    def setUp(self):
        Tester.setup_distributed(self)

    def test_opdriven_aggregate(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        s.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_opdriven_filter(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        s.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_opdriven_flat_map(self):
        topo = Topology();

        lines = topo.source(ListIterator(["mary had a little lamb", "its fleece was white as snow"]))

        # slow things down so checkpoints can be taken.
        lines = lines.filter(StatefulDelay(0.5)) 
        words = lines.flat_map(StatefulSplit())
        words.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    # There is no way to set consistent on a sink or hash_adder

    def test_opdriven_map(self):
        topo = Topology()
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))

        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    # Source operator
    def test_opdriven_source(self):
        topo = Topology()
        
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        s.set_consistent(ConsistentRegionConfig.operator_driven(drain_timeout=40, reset_timeout=40, max_consecutive_attempts=3))
        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))
