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

class MTChecker(object):
    def __init__(self):
        self.a = 0
        self.b = 0

    def __call__(self, tuple_):
        self.a += 1
        time.sleep(0.001)
        self.b += 1
        return self.a == self.b

class MTForEach(MTChecker):
    def __call__(self, tuple_):
        v = super(MTForEach, self).__call__(tuple_)
        if not v:
            raise ValueError('MTForEach:' + str(self.a) + " != " + str(self.b))

class MTFlatMap(MTChecker):
    def __call__(self, tuple_):
        v = super(MTFlatMap, self).__call__(tuple_)
        return [v]

class MTHashAdder(MTChecker):
    def __call__(self, tuple_):
        v = super(MTHashAdder, self).__call__(tuple_)
        if not v:
            raise ValueError('MTHashAdder:' + str(self.a) + " != " + str(self.b))
        return self.a

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
        s = s.filter(MTChecker())
        s = s.filter(lambda _ : True)
        s = s.map(MTChecker())
        s = s.map()
        s = s.flat_map(MTFlatMap())
        s = s.flat_map(lambda v : [v])
        s.for_each(MTForEach())
        s.for_each(lambda _ : None)
        sp = s.parallel(3, Routing.HASH_PARTITIONED, MTHashAdder())
        s = s.end_low_latency()

        sp = sp.map().end_parallel()
      
        tester = Tester(topo)
        tester.tuple_count(s, N*3)
        tester.tuple_count(sp, N*3)
        tester.test(self.test_ctxtype, self.test_config)
