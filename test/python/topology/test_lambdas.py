# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import dill

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

  def test_TopologyLambdaFilter(self):
     topo = Topology("test_TopologyLambdaFilter")
     hw = topo.source(test_functions.hello_world)
     hwf = hw.filter(lambda t : "Wor" in t)
     hwf.sink(test_functions.check_hello_world_filter)
     standalone(self, topo)

  @unittest.skipIf(skip_numpy, "Numpy not available")
  def test_TopologyLambdaModule(self):
     "Lambda using an imported module"
     topo = Topology("test_TopologyLambdaModule")
     hw = topo.source(test_functions.hello_world)
     hw = hw.map(lambda t: t + ":" +  str(np.cos(0.4)))
     hw = hw.map(lambda t: t.split(":",1)[0])
     hw.sink(test_functions.check_hello_world)
     standalone(self, topo)
