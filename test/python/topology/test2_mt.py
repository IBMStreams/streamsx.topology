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
from streamsx.topology.state import ConsistentRegionConfig

class MTSource(object):
    def __init__(self, N):
        self.a = 0
        self.b = 0
        self.N = N
        self.c = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.c >= self.N:
            raise StopIteration()
        v = self.c
        self.c += 1
        self.a += 1
        time.sleep(0.0001)
        self.b += 1
        if self.a != self.b:
            raise ValueError('MTSource:' + str(self.a) + " != " + str(self.b))
        return v
        

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

    def add_stateful(self, topo, s):
        pass

    def test_mt(self):
        topo = Topology()
        N = 1000
        s1 = topo.source(MTSource(N)).low_latency()
        s2 = topo.source(range(N)).low_latency()
        s3 = topo.source(range(N)).low_latency()
        self.add_stateful(topo, [s1,s2,s3])

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

    def test_mt_batch_aggregate(self):
        topo = Topology()
        N = 1000
        s1 = topo.source(range(N))
        s2 = topo.source(range(N))
        s3 = topo.source(range(N))
        self.add_stateful(topo, [s1,s2,s3])
        s = s1.union({s2,s3})

        s = s.batch(7).aggregate(lambda tuples : tuples).flat_map()

        tester = Tester(topo)
        tester.tuple_count(s, N*3)
        tester.test(self.test_ctxtype, self.test_config)
        
    def test_mt_last_aggregate(self):
        topo = Topology()
        N = 1000
        s1 = topo.source(range(N))
        s2 = topo.source(range(N))
        s3 = topo.source(range(N))
        self.add_stateful(topo, [s1,s2,s3])
        s = s1.union({s2,s3})

        s = s.last(1).trigger(1).aggregate(lambda tuples : tuples).flat_map()

        tester = Tester(topo)
        tester.tuple_count(s, N*3)
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedMTCheckpoint(TestMT):
    def setUp(self):
        Tester.setup_distributed(self)

    def add_stateful(self, topo, s=None):
        topo.checkpoint_period = 0.5

class TestDistributedMTConsistentRegion(TestMT):
    def setUp(self):
        Tester.setup_distributed(self)

    def add_stateful(self, topo, streams):
        for s in streams:
            s.set_consistent(ConsistentRegionConfig.periodic(0.5))


class TestSasMTCheckpoint(TestDistributedMTCheckpoint):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


class TestSasMTConsistentRegion(TestDistributedMTConsistentRegion):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
