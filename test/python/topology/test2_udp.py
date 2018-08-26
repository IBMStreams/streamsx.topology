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
import streamsx.ec as ec
import streamsx.spl.op as op

class AddChannel(object):
    def __init__(self):
        pass

    def __call__(self, tuple):
        return tuple, self.channel

    def __enter__(self):
        self.channel = ec.channel(self)

    def __exit__(self, type, value, traceback):
        pass

class CheckSameChannel(object):
    def __init__(self, gv=None):
        self.seen = {}
        self.gv = gv

    def __call__(self, tuple):
        if self.gv is None:
            v = tuple[0]
        else:
            v = self.gv(tuple)
        if v in self.seen:
            if tuple[1] != self.seen[v]:
                return "Different channels for " + v + " " + (tuple[1], self.seen[v])
        else:
            self.seen[v] = tuple[1]
        return v

def stupid_hash(v):
    return hash(v+89)

def s2_hash(t):
    return hash(t['s2'])

class TestUDP(unittest.TestCase):
  _multiprocess_can_split_ = True

  # Fake out subTest
  if sys.version_info.major == 2:
      def subTest(self, **args): return threading.Lock()

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologyNestedParallel(self):
      topo = Topology()
      s = topo.source([1])
      s = s.parallel(5, routing=Routing.BROADCAST)
      s = s.parallel(5, routing=Routing.BROADCAST)
      s = s.map(lambda x: x)
      s = s.end_parallel()
      s = s.end_parallel()
      s.print()
      
      tester = Tester(topo)
      tester.contents(s, [1 for i in range(25)])
      tester.test(self.test_ctxtype, self.test_config)
      print(tester.result)

  def test_TopologySetParallel(self):
      topo = Topology("test_TopologySetParallel")
      s = topo.source([1])
      s.set_parallel(5)
      s = s.end_parallel()
      
      tester = Tester(topo)
      tester.contents(s, [1,1,1,1,1])
      tester.test(self.test_ctxtype, self.test_config)
      print(tester.result)

  def test_TopologyMultiSetParallel(self):
      topo = Topology("test_TopologyMultiSetParallel")

      s = topo.source([1])
      s.set_parallel(5)

      s2 = topo.source([2])
      s2.set_parallel(5)

      s = s.union({s2})
      # #1750 ensure we are not depending on last op
      o = topo.source([2])
      s = s.end_parallel()
      
      tester = Tester(topo)
      tester.contents(s, [1,1,1,1,1,2,2,2,2,2], ordered=False)
      tester.test(self.test_ctxtype, self.test_config)
      print(tester.result)

  def test_TopologyParallelRoundRobin(self):
      for width in (1,3):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallel" + str(width))
              s = topo.source(range(17,142))
              s = s.parallel(width)
              s = s.map(lambda tuple : tuple + 19)
              s = s.end_parallel()
              # Issue #1742 - ensure a view can be created
              v = s.view()

              tester = Tester(topo)
              tester.contents(s, range(36,161), ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_TopologyParallelHash(self):
      for width in (1,3):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallelHash" + str(width))
              s = topo.source(range(17,142))
              s = s.parallel(width, routing=Routing.HASH_PARTITIONED)
              s = s.map(lambda tuple : tuple + 19)
              s = s.map(AddChannel())
              s = s.end_parallel()

              expected = []
              for v in range(17,142):
                  expected.append((v+19, hash(v) % width))

              tester = Tester(topo)
              tester.contents(s, expected, ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_TopologyParallelHashFunction(self):
      for width in (1,7):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallelHashFunction" + str(width))
              s = topo.source(range(17,142))
              s = s.parallel(width, Routing.HASH_PARTITIONED, stupid_hash)
              s = s.map(lambda tuple : tuple + 23)
              s = s.map(AddChannel())
              s = s.end_parallel()

              expected = []
              for v in range(17,142):
                  expected.append((v+23, (hash(v) + 89) % width))

              tester = Tester(topo)
              tester.contents(s, expected, ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_StringHash(self):
      """
      Test hashing works when the schema is tuple<rstring string>.
      """
      raw = []
      for v in range(20):
          raw.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(v)))
      data = []
      for v in range(7):
           data.extend(raw)
      random.shuffle(data)
         
      for width in (1,3):
          with self.subTest(width=width):
              topo = Topology("test_StringHash" + str(width))
              s = topo.source(data)
              s = s.as_string()
              s = s.parallel(width, Routing.HASH_PARTITIONED)
              s = s.map(AddChannel())
              s = s.end_parallel()
              s = s.map(CheckSameChannel())

              tester = Tester(topo)
              tester.contents(s, data, ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_SPLHashFunc(self):
      """
      Test hashing works when the schema is a general SPL one
      using an explicit hash function.
      """
      raw = []
      for v in range(20):
          raw.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(v)))
      data = []
      for v in range(7):
           data.extend(raw)
      random.shuffle(data)
         
      for width in (1,4):
          with self.subTest(width=width):
              topo = Topology("test_SPLHash" + str(width))
              s = topo.source(data)
              s = s.as_string()
              f = op.Map('spl.relational::Functor', s,
                   schema = 'tuple<rstring string, rstring s2>')
              f.s2 = f.output('string + "_1234"')
              s = f.stream
              s = s.parallel(width, Routing.HASH_PARTITIONED, s2_hash)
              s = s.map(AddChannel())
              s = s.end_parallel()
              s = s.map(CheckSameChannel(lambda t : t[0]['s2']))

              expected = []
              for v in data:
                 expected.append(v + '_1234')

              tester = Tester(topo)
              tester.contents(s, expected, ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_in_region_multi_use(self):
        topo = Topology("test_TopologyMultiSetParallel")

        N = 132

        s = topo.source(range(0, N))
        s = s.parallel(3)
        s = s.map(lambda v : v+18)
        eo = s.end_parallel()
        # Use s multiple times in region
        sm = s.map(lambda v : v-23)

        # and just for graph generation a termination within region
        s.for_each(lambda v : None)
        em = sm.end_parallel()
      
        tester = Tester(topo)
        tester.contents(eo, list(range(0+18, N+18)), ordered=False)
        tester.contents(em, list(range(0+18-23, N+18-23)), ordered=False)
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedUDP(TestUDP):
  def setUp(self):
      Tester.setup_distributed(self)

class TestBluemixUDP(TestUDP):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)

class TestUDPNoExec(unittest.TestCase):
    def test_no_default_hash(self):
        topo = Topology('test_SPLBeaconFilter')
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.2, 'iterations':100})
        s.seq = s.output('IterationCount()')
        self.assertRaises(NotImplementedError, s.stream.parallel, 3, Routing.HASH_PARTITIONED)
