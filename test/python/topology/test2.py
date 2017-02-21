# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_functions
from test_utilities import standalone

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context

def s4():
    return ['one', 'two', 'three', 'four']

ctxtype = "STANDALONE"

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
