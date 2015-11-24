import unittest

import test_functions

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context

class TestTopologyMethods(unittest.TestCase):

  def test_TopologyName(self):
     topo = Topology("test_TopologyName")
     self.assertEqual("test_TopologyName", topo.name)

  def test_TopologySourceAndSink(self):
     topo = Topology("test_TopologySourceAndSink")
     hw = topo.source(test_functions.hello_world)
     hw.sink(test_functions.check_hello_world)
     streamsx.topology.context.submit("STANDALONE", topo.graph)

  def test_TopologyStringSubscribe(self):
     topo = Topology("test_TopologyStringSubscribe")
     hw = topo.subscribe("python.test.topic1", schema.CommonSchema.String)
     hw.sink(test_functions.check_hello_world)
     #streamsx.topology.context.submit("BUNDLE", topo.graph)

if __name__ == '__main__':
    unittest.main()


# stateful functions
# import every known module implicitly
# classes for stateful functions
# take the complete packages directory.
