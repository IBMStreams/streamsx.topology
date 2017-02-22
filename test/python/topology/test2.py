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

ctxtype = "STANDALONE"

@unittest.skipIf(sys.version_info.major == 2 or not test_vers.tester_supported() , "tester requires Python 3.5 and Streams >= 4.2")
class TestTopologyMethodsNew(unittest.TestCase):

  def test_TopologySourceList(self):
     topo = Topology('test_TopologySourceList')
     hw = topo.source(["Hello", "Tester"])
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     tester.test(ctxtype)

  def test_TopologySourceFn(self):
     topo = Topology('test_TopologySourceFn')
     hw = topo.source(s4)
     tester = Tester(topo)
     tester.contents(hw, s4())
     tester.tuple_count(hw, len(s4()))
     tester.test(ctxtype)

  def test_TopologySourceItertools(self):
     topo = Topology('test_TopologySourceItertools')
     hw = topo.source(itertools.repeat(9, 3))
     tester = Tester(topo)
     tester.contents(hw, [9, 9, 9])
     tester.test(ctxtype)
