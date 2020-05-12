# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import time
import random
import itertools

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
import streamsx.spl.op as op
import typing
from typing import NamedTuple

"""
Test filter function with one and more output streams.
"""

class TestFilter(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologyFilter(self):
      topo = Topology("test_TopologyFilter")
      hw = topo.source(['Hello', 'World'])
      hwf1 = hw.filter(lambda t : "Wor" in t, non_matching=False)
      hwf2 = hw.filter(lambda t : "Wor" in t, non_matching=None)
      hwf3 = hw.filter(lambda t : "Wor" in t)

      tester = Tester(topo)
      tester.contents(hwf1, ['World'])
      tester.contents(hwf2, ['World'])
      tester.contents(hwf3, ['World'])
      tester.test(self.test_ctxtype, self.test_config)
      
  def test_TopologyFilterNonMatching(self):
      topo = Topology("test_TopologyFilterNonMatching")
      hw = topo.source(['Hello', 'World'])
      hwf,nm = hw.filter((lambda t : "Wor" in t), non_matching=True)

      tester = Tester(topo)
      self.test_config['topology.keepArtifacts'] = True
      tester.contents(hwf, ['World'])
      tester.contents(nm, ['Hello'])
      tester.test(self.test_ctxtype, self.test_config)     


class TestDistributedFilter(TestFilter):

  def setUp(self):
      Tester.setup_distributed(self)
      self.test_config[ConfigParams.SSL_VERIFY] = False


