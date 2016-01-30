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

  def test_TopologyFilter(self):
     topo = Topology("test_TopologyFilter")
     hw = topo.source(test_functions.hello_world)
     hwf = hw.filter(test_functions.filter);
     hwf.sink(test_functions.check_hello_world_filter)
     streamsx.topology.context.submit("STANDALONE", topo.graph)

  def test_TopologyIsolate(self):
     topo = Topology("test_TopologyIsolate")
     hw = topo.source(test_functions.hello_world)
     iso = hw.isolate()
     iso.sink(test_functions.check_hello_world)
     streamsx.topology.context.submit("STANDALONE", topo.graph)

  def test_TopologyIsolatedFilter(self):
     topo = Topology("test_TopologyIsolatedFilter")
     hw = topo.source(test_functions.hello_world)
     iso1 = hw.isolate()
     hwf = iso1.filter(test_functions.filter);
     iso2 = hwf.isolate()
     iso2.sink(test_functions.check_hello_world_filter)
     streamsx.topology.context.submit("STANDALONE", topo.graph)
     # switch this to BUNDLE to create a sab file that can 
     # be sumitted to a streams instance and run as 3 PEs
     # streamsx.topology.context.submit("BUNDLE", topo.graph)

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
