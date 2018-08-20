from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.ec as ec

import unittest
from datetime import timedelta

# Tests for checkpointing.  This does not actually test that checkpoints
# can be created and destroyed.  It only tests that operators containing
# serializable callables can be run successfully with checkpointing enabled.

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

# Do nothing, statefully.  This is used with for_each.  It seems not so
# easy to test for_each.
class StatefulNothing(object):
    def __call__(self, x):
        pass

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
    

class TestCheckpointing(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    # Source operator
    def test_source(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        tester = Tester(topo)
        tester.contents(s, range(0,30))

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    # Source, ForEach, Filter, Aggregate operators
    # (based on test2_python_window.TestPythonWindowing.test_basicCountCountWindow)
    def test_aggregate(self):
        topo = Topology()
        topo.checkpoint_period = timedelta(seconds=1)
        # Generate integers from [0,30)
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        # Filter the odd ones 
        s = s.filter(StatefulEvenFilter())
        # Halve the even ones and add one.  Now have integers [1,15)
        s = s.map(StatefulHalfPlusOne())
        s = s.last(10).trigger(2).aggregate(StatefulAverage())
        tester = Tester(topo)
        tester.contents(s, [1.5,2.5,3.5,4.5,5.5,7.5,9.5])
        tester.test(self.test_ctxtype, self.test_config)
        

    # Test flat map (based on a sample)
    def test_flat_map(self):
        topo = Topology();
        topo.checkpoint_period = timedelta(seconds=1)

        lines = topo.source(ListIterator(["mary had a little lamb", "its fleece was white as snow"]))
        # slow things down so checkpoints can be taken.
        lines = lines.filter(StatefulDelay(0.5)) 
        words = lines.flat_map(StatefulSplit())
        tester = Tester(topo)
        tester.contents(words, ["mary","had","a","little","lamb","its","fleece","was","white","as","snow"])
        tester.test(self.test_ctxtype, self.test_config)

    # Test hash adder.  This requires parallel with a hash router.
    # (based on test2_udp.TestUDP.test_TopologyParallelHash)
    def test_hash_adder(self):
        topo = Topology("test_hash_adder")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        width =  3
        s = s.parallel(width, Routing.HASH_PARTITIONED, StatefulStupidHash())
        s = s.map(lambda x: x + 23)
        s = s.end_parallel()

        expected = []
        for v in range (0,30):
            expected.append((v + 23))

        tester = Tester(topo)
        tester.contents(s, expected, ordered=width==1)
        tester.test(self.test_ctxtype, self.test_config)
            
    # Test for_each.  This is a sink. 
    def test_for_each(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(TimeCounter(iterations=30, period=0.1))
        s.for_each(StatefulNothing())
        tester = Tester(topo)
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedCheckpointing(TestCheckpointing):
    def setUp(self):
        Tester.setup_distributed(self)

class TestSasCheckpointing(TestCheckpointing):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


