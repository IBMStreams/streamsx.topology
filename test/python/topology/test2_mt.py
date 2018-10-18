# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
import string
import random
import threading

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

class MTFilter(object):
    def __init__(self):
        self.a = 0
        self.b = 0

    def __call__(self, tuple_):
        self.a += 1
        time.sleep(0.001)
        self.b += 1
        return self.a == self.b

class TestMT(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_mt(self):
        topo = Topology()
        N = 1000
        s1 = topo.source(range(N)).low_latency()
        s2 = topo.source(range(N)).low_latency()
        s3 = topo.source(range(N)).low_latency()

        s = s1.union({s2,s3})
        s = s.filter(MTFilter())
        s = s.filter(lambda _ : True)
        s = s.end_low_latency()
      
        tester = Tester(topo)
        tester.tuple_count(s, N*3)
        tester.test(self.test_ctxtype, self.test_config)
