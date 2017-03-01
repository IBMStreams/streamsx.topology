# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

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
              hw = topo.source(range(17,142))
              hw = hw.parallel(width)
              hw = hw.map(lambda tuple : tuple + 19)
              hw = hw.end_parallel()

              tester = Tester(topo)
              tester.contents(hw, range(36,161), ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_TopologyParallelHash(self):
      for width in (1,3):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallelHash" + str(width))
              hw = topo.source(range(17,142))
              hw = hw.parallel(width, routing=Routing.HASH_PARTITIONED)
              hw = hw.map(lambda tuple : tuple + 19)
              hw = hw.map(AddChannel())
              hw = hw.end_parallel()

              expected = []
              for v in range(17,142):
                  expected.append((v+19, hash(v) % width))

              tester = Tester(topo)
              tester.contents(hw, expected, ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)
              print(tester.result)

  def test_TopologyParallelHashFunction(self):
      for width in (1,7):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallelHashFunction" + str(width))
              hw = topo.source(range(17,142))
              hw = hw.parallel(width, Routing.HASH_PARTITIONED, stupid_hash)
              hw = hw.map(lambda tuple : tuple + 23)
              hw = hw.map(AddChannel())
              hw = hw.end_parallel()

              expected = []
              for v in range(17,142):
                  expected.append((v+23, (hash(v) + 89) % width))

              tester = Tester(topo)
              tester.contents(hw, expected, ordered=width==1)
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
