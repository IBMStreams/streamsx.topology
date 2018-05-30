# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import time

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import context
from streamsx import rest
import streamsx.ec as ec

import test_vers

def rands():
    r = random.Random()
    while True:
       yield r.random()

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestTester(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_at_least(self):
        """ Test the at least tuple count.
        """
        if self.test_ctxtype == context.ContextTypes.STANDALONE:
            return unittest.skip("Standalone tests must complete")

        topo = Topology()
        s = topo.source(rands)
        tester = Tester(topo)
        tester.tuple_count(s, 100, exact=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_no_tuples(self):
        """ Test exact count with zero tuples.
        """
        topo = Topology()
        s = topo.source([])
        tester = Tester(topo)
        tester.tuple_count(s, 0)
        tester.test(self.test_ctxtype, self.test_config)

    def test_at_least_no_tuples(self):
        """ Test at least count with zero tuples. 
            (kind of a pointless condition, always true).
        """
        if self.test_ctxtype == context.ContextTypes.STANDALONE:
            return unittest.skip("Standalone tests must complete")

        topo = Topology()
        s = topo.source([])
        tester = Tester(topo)
        tester.tuple_count(s, 0, exact=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_checker(self):
        """ Test the per-tuple checker.
        """
        topo = Topology()
        s = topo.source(rands)
        s = s.filter(lambda r : r > 0.8)
        s = s.map(lambda r : r + 7.0 )
        tester = Tester(topo)
        tester.tuple_count(s, 200, exact=False)
        if self.test_ctxtype == context.ContextTypes.STANDALONE:
            tester.run_for(20)
        tester.tuple_check(s, lambda r : r > 7.8)
        tester.test(self.test_ctxtype, self.test_config)

    def test_local_check(self):
        """ Test the at least tuple count.
        """
        if self.test_ctxtype == context.ContextTypes.STANDALONE:
            return unittest.skip("Standalone tests don't support local check")
        topo = Topology()
        s = topo.source(rands)
        self.my_local_called = False
        self.tester = Tester(topo)
        self.tester.tuple_count(s, 100, exact=False)
        self.tester.local_check = self.my_local
        self.tester.test(self.test_ctxtype, self.test_config)
        self.assertTrue(self.my_local_called)

    def my_local(self):
        self.assertTrue(hasattr(self.tester, 'submission_result'))
        self.assertTrue(hasattr(self.tester, 'streams_connection'))
        self.my_local_called = True

    def test_bad_pe(self):
        """Test a failure in a PE is caught as a test failure"""
        topo = Topology()
        s = topo.source(rands)
        # intentional addition of a string with an int
        # to cause a PE failure
        s = s.map(lambda t : t + 'a string')
        tester = Tester(topo)
        tester.tuple_count(s, 0, exact=False)
        tp = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
        self.assertFalse(tp)

    def test_run_for(self):
        topo = Topology()
        s = topo.source([1,2,3])
        self.tester = Tester(topo)
        self.tester.tuple_count(s, 3)
        self.tester.run_for(120)
        if self.test_ctxtype == context.ContextTypes.STANDALONE:
            self.rf_start = time.time()
        else:
            self.tester.local_check = self.get_start_time
        self.tester.test(self.test_ctxtype, self.test_config)
        now = time.time()
        test_duration = now - self.rf_start
        self.assertTrue(test_duration >= 120)

    def get_start_time(self):
        job = self.tester.submission_result.job
        self.rf_start = job.submitTime / 1000.0


@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedTester(TestTester):
    def setUp(self):
        Tester.setup_distributed(self)


@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestCloudTester(TestTester):
    def setUp(self):
        Tester.setup_streaming_analytics(self)

