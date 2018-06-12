from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import unittest
from datetime import timedelta

import test_vers

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
@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
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

@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
class test_dillable_class_distributed(test_dillable_class):
    def setUp(self):
        Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
class test_dillable_class_service(test_dillable_class):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


# Test a source that cannot be dilled, and does not suppress the exception
# In standalone, no attempt to enable checkpointing should be made,
# and the topology should run to completion.
@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
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
@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
class test_undillable_class_distributed(test_undillable_class):
    def setUp(self):
        Tester.setup_distributed(self)

    def test_setup_checkpoint(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        s = topo.source(undillable_source(3, False))
        tester = Tester(topo)

        # the job should fail to become healthy.
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
class test_undillable_class_service(test_undillable_class_distributed):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


# Test a source that cannot be dilled, and suppresses the exception
@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
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

@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
class test_undillable_class_suppress_distributed(test_undillable_class_suppress):
    def setUp(self):
        Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported(), "TesterNotSupported")
class test_undillable_class_suppress_service(test_undillable_class_suppress):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
