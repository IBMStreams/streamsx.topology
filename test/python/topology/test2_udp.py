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
    def __init__(self):
        self.seen = {}

    def __call__(self, tuple):
        v = tuple[0]
        if v in self.seen:
            if tuple[1] != self.seen[v]:
                return "Different channels for " + v + " " + (tuple[1], self.seen[v])
        else:
            self.seen[v] = tuple[1]
        return v

def stupid_hash(v):
    return hash(v+89)

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

  def test_TopologyParallelStringHash(self):
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
              topo = Topology("test_TopologyParallelStringHash" + str(width))
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

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedUDP(TestUDP):
  def setUp(self):
      Tester.setup_standalone(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixUDP(TestUDP):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
