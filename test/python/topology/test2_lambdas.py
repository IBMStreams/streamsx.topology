# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
from streamsx.topology.tester import Tester

import test_functions
from test_utilities import standalone

try:
    import numpy as np
    skip_numpy = False
except ImportError:
    skip_numpy = True

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context

class TestLambdas(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologyLambdaFilter(self):
      topo = Topology("test_TopologyLambdaFilter")
      hw = topo.source(['Hello', 'World'])
      hwf = hw.filter(lambda t : "Wor" in t)

      tester = Tester(topo)
      tester.contents(hwf, ['World'])
      tester.test(self.test_ctxtype, self.test_config)

  def test_local_capture(self):
      topo = Topology("test_TopologyLambdaFilter")
      n = topo.source([1,2,4])
      x = 93
      n = n.map(lambda v : v + x)

      tester = Tester(topo)
      tester.contents(n, [94,95,97])
      tester.tuple_count(n, 3)
      tester.test(self.test_ctxtype, self.test_config)

  @unittest.skipIf(skip_numpy, "Numpy not available")
  def test_TopologyLambdaModule(self):
      "Lambda using an imported module"
      topo = Topology("test_TopologyLambdaModule")
      hw = topo.source(['A'])
      hw = hw.map(lambda t: t + ":" +  str(round(np.cos(0.4),3)))

      tester = Tester(topo)
      tester.contents(hw, ['A:' + str(round(np.cos(0.4),3))])
      tester.test(self.test_ctxtype, self.test_config)
