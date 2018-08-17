# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.topology.context as stc

class TestNames(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
      Tester.setup_standalone(self)

  def test_NotebookDefaultNames(self):
     """ Test default topo names from a notebook
     """
     topo = Topology(name='<module>', namespace='<ipython-input-1-e300f4c6abce>')
     hw = topo.source(["Hello", "Tester"])
     s = hw.filter(lambda x : True, name = "One.A")
     s = s.filter(lambda x : True, name = "Two.A")
     tester = Tester(topo)
     tester.contents(s, ["Hello", "Tester"])
     tester.test(self.test_ctxtype, self.test_config)

  def test_UnicodeTopoNames(self):
     """ Test unicode topo names
     """
     n = u'你好'
     ns = u'世界'
     topo = Topology(name=n, namespace=ns)

     self.assertEqual(n, topo.name)
     self.assertEqual(ns, topo.namespace)
     hw = topo.source(["Hello", "Tester"])
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     tester.test(self.test_ctxtype, self.test_config)

  def test_LongTopoNames(self):
     """ Test long topo names
     """
     n = 'abcd8' # * 30
     ns = 'def9' #* 100
     topo = Topology(name=n, namespace=ns)

     self.assertEqual(n, topo.name)
     self.assertEqual(ns, topo.namespace)
     hw = topo.source(["Hello", "Tester"], name='sourceabc')
     hw = hw.filter(lambda x : True, name='a'*255)
     hw = hw.filter(lambda x : True, name=u'你好'*100)
     hw = hw.filter(lambda x : True, name=u'你好')
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     tester.test(self.test_ctxtype, self.test_config)

class TestCloudNames(TestNames):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

class TestContextNames(unittest.TestCase):
    def test_expected_values(self):
        self.assertEqual('DISTRIBUTED', stc.ContextTypes.DISTRIBUTED)
        self.assertEqual('ANALYTICS_SERVICE', stc.ContextTypes.ANALYTICS_SERVICE)
        self.assertEqual('STREAMING_ANALYTICS_SERVICE', stc.ContextTypes.STREAMING_ANALYTICS_SERVICE)
