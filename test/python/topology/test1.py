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
     hwf = hw.filter(test_functions.filter)
     hwf.sink(test_functions.check_hello_world_filter)
     streamsx.topology.context.submit("STANDALONE", topo.graph)
        
  def test_TopologyLengthFilter(self):
     topo = Topology("test_TopologyLengthFilter")
     hw = topo.source(test_functions.strings_length_filter) 
     hwf = hw.filter(test_functions.LengthFilter(5))
     hwf.sink(test_functions.check_strings_length_filter)
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
     hwf = iso1.filter(test_functions.filter)
     iso2 = hwf.isolate()
     iso2.sink(test_functions.check_hello_world_filter)
     streamsx.topology.context.submit("STANDALONE", topo.graph)
     # switch this to BUNDLE to create a sab file that can 
     # be sumitted to a streams instance and run as 3 PEs
     # streamsx.topology.context.submit("BUNDLE", topo.graph)

  def test_TopologyLowLatency(self):
     topo = Topology("test_TopologyLowLatency")
     hw = topo.source(test_functions.hello_world)
     low1 = hw.low_latency()
     hwf1 = low1.filter(test_functions.filter)
     hwf2 = hwf1.filter(test_functions.filter)
     elow1 = hwf2.end_low_latency()
     hwf3 = elow1.filter(test_functions.filter)
     hwf3.sink(test_functions.check_hello_world_filter)
     streamsx.topology.context.submit("BUNDLE", topo.graph)

  def test_TopologyStringSubscribe(self):
     topo = Topology("test_TopologyStringSubscribe")
     hw = topo.subscribe("python.test.topic1", schema.CommonSchema.String)
     hw.sink(test_functions.check_hello_world)
     #streamsx.topology.context.submit("BUNDLE", topo.graph)
     
  def test_TopologyTransform(self):
     topo = Topology("test_TopologyTransform")
     source = topo.source(test_functions.int_strings_transform)
     i1 = source.transform(test_functions.string_to_int)
     i2 = i1.transform(test_functions.add17)
     i2.sink(test_functions.check_int_strings_transform)
     streamsx.topology.context.submit("STANDALONE", topo.graph)
     
  def test_TopologyTransformWithDrop(self):
     topo = Topology("test_TopologyTransformWithDrop")
     source = topo.source(test_functions.int_strings_transform_with_drop)
     i1 = source.transform(test_functions.string_to_int_except68)
     i2 = i1.transform(test_functions.add17)
     i2.sink(test_functions.check_int_strings_transform_with_drop)
     streamsx.topology.context.submit("STANDALONE", topo.graph)
     
  def test_TopologyMultiTransform(self):
      topo = Topology("test_TopologyMultiTransform")
      source = topo.source(test_functions.strings_multi_transform)
      i1 = source.multi_transform(test_functions.split_words)
      i1.sink(test_functions.check_strings_multi_transform)
      streamsx.topology.context.submit("STANDALONE", topo.graph)
      
  def test_TopologyTransformCallableAddWithDrop(self):
      topo = Topology("test_TopologyTransformCallableAddWithDrop")
      source = topo.source(test_functions.int_strings_transform_with_drop)
      i1 = source.transform(test_functions.string_to_int_except68)
      i2 = i1.transform(test_functions.AddNum(17))
      i2.sink(test_functions.check_int_strings_transform_with_drop)
      streamsx.topology.context.submit("STANDALONE", topo.graph)
  
  def test_TopologyMultiTransformCallableIncMaxSplit(self):
      topo = Topology("test_TopologyMultiTransformCallableIncMaxSplit")
      source = topo.source(test_functions.strings_multi_transform)
      i1 = source.multi_transform(test_functions.IncMaxSplitWords(1))
      i1.sink(test_functions.check_strings_multi_transform_inc_max_split)
      streamsx.topology.context.submit("STANDALONE", topo.graph)
  
  def test_TopologySourceAndSinkCallable(self):
      topo = Topology("test_TopologySourceAndSinkCallable")
      hw = topo.source(test_functions.SourceTuplesAppendIndex(["a", "b", "c", "d"]))
      hw.sink(test_functions.CheckTuples(["a0", "b1", "c2", "d3"]))
    
  def test_TopologyParallel(self):
      topo = Topology("test_TopologyParallel")
      hw = topo.source(test_functions.hello_world)   
      hwp = hw.parallel(4)
      hwf = hwp.filter(test_functions.filter)
      hwef = hwf.end_parallel()
      hwef.sink(test_functions.check_hello_world_filter)
      streamsx.topology.context.submit("STANDALONE", topo.graph)

  def test_TopologyUnion(self):
      topo = Topology("test_TopologyUnion")
      h = topo.source(test_functions.hello)
      b = topo.source(test_functions.beautiful)
      c = topo.source(test_functions.crazy)
      w = topo.source(test_functions.world)
      streamSet = {h, w, b, c}
      hwu = h.union(streamSet)
      hwu.sink(test_functions.check_union_hello_world)
      streamsx.topology.context.submit("STANDALONE", topo.graph) 
      
  def test_TopologyParallelUnion(self):
      topo = Topology("test_TopologyParallelUnion")
      hw = topo.source(test_functions.hello_world)
      hwp = hw.parallel(3)
      hwf = hwp.filter(test_functions.filter)
      hwf2 = hwp.filter(test_functions.filter)    
      streamSet = {hwf2}
      hwu = hwf.union(streamSet)
      hwup = hwu.end_parallel()
      hwup.sink(test_functions.check_union_hello_world)
      streamsx.topology.context.submit("STANDALONE", topo.graph)
          
if __name__ == '__main__':
    unittest.main()


# stateful functions
# import every known module implicitly
# classes for stateful functions
# take the complete packages directory.
