# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest

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
        Tester.setup_distributed(self)

    def test_at_least(self):
        """ Test the at least tuple count.
        """
        topo = Topology()
        s = topo.source(rands)
        tester = Tester(topo)
        tester.tuple_count(s, 100, exact=False)
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
        tester.tuple_check(s, lambda r : r > 7.8)
        tester.test(self.test_ctxtype, self.test_config)

    def test_local_check(self):
        """ Test the at least tuple count.
        """
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
   

   
