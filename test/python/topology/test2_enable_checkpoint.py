from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import unittest
from datetime import timedelta

# tests for enabling checkpointing

# If an undillable class is used and checkpointing is enabled,
# an exception should be thrown.
# Ensure that suppressing exceptions does not cause the
# checkpoint error to be ignored. Suppression of exceptions
# is documented for conversion errors and processing errors only.

# Class defining a source of integers from 1 to the limit, inclusive.
# An instance of this class can be dilled.
class DillableSource:
    def __init__(self, limit, _):
        self.count = 0
        self.limit = limit

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        self.count += 1
        if self.count > self.limit:
            raise StopIteration
        return self.count

# Class defining a source of integers from 1 to the limit, inclusive.
# An instance of this class returns a generator, which cannot be dilled, 
# and optionally suppresses the exception that is thrown when checkpointing 
# is attempted.
class UndillableSource:
    def __init__(self, limit, suppress):
        self.limit = limit
        self.suppress = suppress

    def __iter__(self):
        for count in range(1, self.limit + 1):
            yield count

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        return self.suppress

def setup_app(topo, cls, suppress=False):
    topo.checkpoint_period = timedelta(seconds=1)
    s = topo.source(cls(3, suppress))
    tester = Tester(topo)
    tester.contents(s, [1, 2, 3])
    return tester

# Test a source that can be dilled.
class TestDillable(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_dillable_class(self):
        tester = setup_app(Topology(), DillableSource)
        tester.test(self.test_ctxtype, self.test_config)

    # Test a source that cannot be dilled, and does not suppress the exception
    # In standalone, no attempt to enable checkpointing should be made,
    # and the topology should run to completion.
    def test_undillable_class(self):
        tester = setup_app(Topology(), UndillableSource)
        tester.test(self.test_ctxtype, self.test_config)

    # Test a source that cannot be dilled, and suppresses the exception
    def test_undillable_class_suppress(self):
        tester = setup_app(Topology(), UndillableSource, True)
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedDillable(TestDillable):
    def setUp(self):
        Tester.setup_distributed(self)

    # Distributed and cloud should fail to become healthy.
    def test_undillable_class(self):
        tester = setup_app(Topology(), UndillableSource)
        # the job should fail to become healthy.
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    def test_undillable_class_suppress(self):
        tester = setup_app(Topology(), UndillableSource, True)
        # the job should fail to become healthy.
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

class TestSasDillable(TestDistributedDillable):
    def setUp(self):
        Tester.setup_streaming_analytics(self)

# checkpoint_period can be either a datetime.timedelta value, or
# any type that can be cast to float.  Here we verify timedelta,
# float, int, string, bool, as well as some negative tests.
# This test is standalone only because the deployment type is
# irrelevant.
class CheckpointPeriodTypes(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_timedelta(self):
        topo = Topology()
        topo.checkpoint_period = timedelta(seconds=1)
        tester = setup_app(topo, DillableSource)
        tester.test(self.test_ctxtype, self.test_config)

    def test_float(self):
        topo = Topology()
        topo.checkpoint_period = 1.0
        tester = setup_app(topo, DillableSource)
        tester.test(self.test_ctxtype, self.test_config)

    def test_int(self):
        topo = Topology()
        topo.checkpoint_period = 1
        tester = setup_app(topo, DillableSource)
        tester.test(self.test_ctxtype, self.test_config)

    # bool can be cast to float
    def test_bool(self):
        topo = Topology()
        topo.checkpoint_period = True
        # If no exception, the test passes.

    # imaginary literal cannot be cast to float.
    def test_imaginary(self):
        topo = Topology()
        with self.assertRaises(TypeError):
            topo.checkpoint_period = 1j

    # None cannot be cast to float
    def test_none(self):
        topo = Topology()
        with self.assertRaises(TypeError):
            topo.checkpoint_period = None

    # test a string that can be cast to float
    def test_string_valid(self):
        topo = Topology()
        topo.checkpoint_period = "42.0"
        # if no exception, the test passed

    # test a string that cannnot be cast to float
    def test_string_invalid(self):
        topo = Topology()
        with self.assertRaises(ValueError):
            topo.checkpoint_period = "Forty-two"

    # Negative period should raise ValueError
    def test_float_negative(self):
        topo = Topology()
        with self.assertRaises(ValueError):
             topo.checkpoint_period = -0.1

    # False is zero, so should raise ValueError
    def test_bool_false(self):
        topo = Topology()
        with self.assertRaises(ValueError):
            topo.checkpoint_period = False


    # Less than 0.001 is not allowed, but exactly 0.001 is.
    def test_float_low(self):
        topo = Topology()

        with self.assertRaises(ValueError):
            topo.checkpoint_period = 0.0009

        # Try again with 0.001
        topo.checkpoint_period = 0.001
        tester = setup_app(topo, DillableSource)
        tester.test(self.test_ctxtype, self.test_config)
