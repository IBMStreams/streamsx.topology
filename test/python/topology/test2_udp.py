# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import string
import random

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.ec as ec
import streamsx.spl.op as op

import test_vers

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
    return t['s2']

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestUDP(unittest.TestCase):

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologyParallelRoundRobin(self):
      for width in (1,3):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallel" + str(width))
              s = topo.source(range(17,142))
              s = s.parallel(width)
              s = s.map(lambda tuple : tuple + 19)
              s = s.end_parallel()

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

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedUDP(TestUDP):
  def setUp(self):
      Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
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
