# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import test_vers

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestNames(unittest.TestCase):

  def setUp(self):
      Tester.setup_standalone(self)

  def test_NotebookDefaultNames(self):
     """ Test default topo names from a notebook
     """
     topo = Topology(name='<module>', namespace='<ipython-input-1-e300f4c6abce>')
     hw = topo.source(["Hello", "Tester"])
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     tester.test(self.test_ctxtype, self.test_config)

  def test_UnicodeTopoNames(self):
     """ Test unicode topo names
     """
     n = '你好'
     ns = '世界'
     topo = Topology(name=n, namespace=ns)

     self.assertEqual(n, topo.name)
     self.assertEqual(ns, topo.namespace)
     hw = topo.source(["Hello", "Tester"])
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     tester.test(self.test_ctxtype, self.test_config)
