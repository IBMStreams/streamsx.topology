from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import unittest
from datetime import timedelta

# tests for enabling checkpointing

# If an undillable class is used and checkpointing is enabled,
# an exception should be thrown.  If the exception is not suppressed,
# the application should shut down.  Otherwise, it continues without
# checkpointing.

# Class defining a source of integers from 1 to the limit, inclusive.
# An instance of this class can be dilled.
class dillable_source:
    def __init__(self, limit):
        self.count = 0
        self.limit = 3

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
class undillable_source:
    def __init__(self, limit, suppress):
        self.limit = 3
        self.suppress = suppress

    def __iter__(self):
        for count in range(1, self.limit + 1):
            yield count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.suppress

# Test a source that can be dilled.
class test_dillable_class(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_setup_checkpoint(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(dillable_source(3))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

class test_distributed_dillable_class(test_dillable_class):
    def setUp(self):
        Tester.setup_distributed(self)

class test_sas_dillable_class(test_dillable_class):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


# Test a source that cannot be dilled, and does not suppress the exception
# In standalone, no attempt to enable checkpointing should be made,
# and the topology should run to completion.
class test_undillable_class(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_setup_checkpoint(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(undillable_source(3, False))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config)

# Distributed and cloud should fail to become healthy.
class test_distributed_undillable_class(test_undillable_class):
    def setUp(self):
        Tester.setup_distributed(self)

    def test_setup_checkpoint(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(undillable_source(3, False))
        tester = Tester(topo)

        # the job should fail to become healthy.
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

class test_sas_undillable_class(test_distributed_undillable_class):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


# Test a source that cannot be dilled, and suppresses the exception
class test_undillable_class_suppress(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_setup_checkpoint(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(undillable_source(3, True))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config)

class test_distributed_undillable_class_suppress(test_undillable_class_suppress):
    def setUp(self):
        Tester.setup_distributed(self)

class test_sas_undillable_class_suppress(test_undillable_class_suppress):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

# checkpoint_period can be either a datetime.timedelta value, or
# any type that can be cast to float.  Here we verify timedelta,
# float, int, string, bool, as well as some negative tests.
# This test is standalone only because the deployment type is
# irrelevant.
class CheckpointPeriodTypes(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_timedelta(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(undillable_source(3, True))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config)

    def test_float(self):
        topo = Topology("test")
        topo.checkpoint_period = 1.0
        s = topo.source(undillable_source(3, True))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config)

    def test_int(self):
        topo = Topology("test")
        topo.checkpoint_period = 1
        s = topo.source(undillable_source(3, True))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config)

    # bool can be cast to float
    def test_bool(self):
        topo = Topology("test")
        topo.checkpoint_period = True
        # If no exception, the test passes.

    # imaginary literal cannot be cast to float.
    def test_imaginary(self):
        topo = Topology("test")
        with self.assertRaises(TypeError):
            topo.checkpoint_period = 1j

    # None cannot be cast to float
    def test_none(self):
        topo = Topology("test")
        with self.assertRaises(TypeError):
            topo.checkpoint_period = None

    # test a string that can be cast to float
    def test_string_valid(self):
        topo = Topology("test")
        topo.checkpoint_period = "42.0"
        # if no exception, the test passed

    # test a string that cannnot be cast to float
    def test_string_invalid(self):
        topo = Topology("test")
        with self.assertRaises(ValueError):
            topo.checkpoint_period = "Forty-two"

    # Negative period should raise ValueError
    def test_float_negative(self):
        topo = Topology("test")
        with self.assertRaises(ValueError):
             topo.checkpoint_period = -0.1

    # False is zero, so should raise ValueError
    def test_bool_false(self):
        topo = Topology("test")
        with self.assertRaises(ValueError):
            topo.checkpoint_period = False


    # Less than 0.001 is not allowed, but exactly 0.001 is.
    def test_float_low(self):
        topo = Topology("test")

        with self.assertRaises(ValueError):
            topo.checkpoint_period = 0.0009

        # Try again with 0.001
        topo.checkpoint_period = 0.001

        s = topo.source(undillable_source(3, True))
        tester = Tester(topo)
        tester.contents(s, [1, 2, 3])

        tester.test(self.test_ctxtype, self.test_config)
