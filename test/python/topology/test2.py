# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import test_vers

def s4():
    return ['one', 'two', 'three', 'four']

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestTopologyMethodsNew(unittest.TestCase):

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologySourceList(self):
     topo = Topology('test_TopologySourceList')
     hw = topo.source(["Hello", "Tester"])
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     tester.test(self.test_ctxtype, self.test_config)

  def test_TopologySourceFn(self):
     topo = Topology('test_TopologySourceFn')
     hw = topo.source(s4)
     tester = Tester(topo)
     tester.contents(hw, s4())
     tester.tuple_count(hw, len(s4()))
     tester.test(self.test_ctxtype, self.test_config)

  def test_TopologySourceItertools(self):
     topo = Topology('test_TopologySourceItertools')
     hw = topo.source(itertools.repeat(9, 3))
     tester = Tester(topo)
     tester.contents(hw, [9, 9, 9])
     tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedTopologyMethodsNew(TestTopologyMethodsNew):
  def setUp(self):
      Tester.setup_standalone(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixTopologyMethodsNew(TestTopologyMethodsNew):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
