# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys

import test_functions
from test_utilities import standalone

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context


class TestTopologyMethods(unittest.TestCase):

  def test_TopologyName(self):
     topo = Topology("test_TopologyNameExplicit")
     self.assertEqual("test_TopologyNameExplicit", topo.name)
     self.assertEqual("test1", topo.namespace)

  def test_TopologyNoName(self):
     topo = Topology()
     self.assertEqual("test_TopologyNoName", topo.name)
     self.assertEqual("test1", topo.namespace)

  def test_TopologyNamespace(self):
     topo = Topology(namespace="myns")
     self.assertEqual("test_TopologyNamespace", topo.name)
     self.assertEqual("myns", topo.namespace)

  def test_TopologyNameNamespace(self):
     topo = Topology(name="myapp", namespace="myns")
     self.assertEqual("myapp", topo.name)
     self.assertEqual("myns", topo.namespace)

  def test_empty(self):
     topo = Topology(name="what_no_streams")
     self.assertRaises(ValueError, streamsx.topology.context.submit, "TOOLKIT", topo)

  def test_TopologySourceAndSink(self):
     topo = Topology("test_TopologySourceAndSink")
     hw = topo.source(test_functions.hello_world)
     hw.sink(test_functions.check_hello_world)
     standalone(self, topo)

  def test_TopologyFilter(self):
     topo = Topology("test_TopologyFilter")
     hw = topo.source(test_functions.hello_world)
     hwf = hw.filter(test_functions.filter)
     hwf.sink(test_functions.check_hello_world_filter)
     standalone(self, topo)

  def test_TopologyLengthFilter(self):
     topo = Topology("test_TopologyLengthFilter")
     hw = topo.source(test_functions.strings_length_filter) 
     hwf = hw.filter(test_functions.LengthFilter(5))
     hwf.sink(test_functions.check_strings_length_filter)
     standalone(self, topo)

  def test_TopologyIsolate(self):
     topo = Topology("test_TopologyIsolate")
     hw = topo.source(test_functions.hello_world)
     iso = hw.isolate()
     iso.sink(test_functions.check_hello_world)
     standalone(self, topo)

  def test_TopologyIsolatedFilter(self):
     topo = Topology("test_TopologyIsolatedFilter")
     hw = topo.source(test_functions.hello_world)
     iso1 = hw.isolate()
     hwf = iso1.filter(test_functions.filter)
     iso2 = hwf.isolate()
     iso2.sink(test_functions.check_hello_world_filter)
     standalone(self, topo)
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
     standalone(self, topo)
     streamsx.topology.context.submit("BUNDLE", topo.graph)

  def test_TopologyStringSubscribe(self):
     topo = Topology("test_TopologyStringSubscribe")
     hw = topo.subscribe("python.test.topic1", schema.CommonSchema.String)
     hw.sink(test_functions.check_hello_world)
     #streamsx.topology.context.submit("BUNDLE", topo.graph)

  def test_TopologyTransform(self):
     topo = Topology("test_TopologyTransform")
     source = topo.source(test_functions.int_strings_transform)
     i1 = source.transform(int)
     i2 = i1.transform(test_functions.add17)
     i2.sink(test_functions.check_int_strings_transform)
     standalone(self, topo)
   
  def test_TopologyTransformWithDrop(self):
     topo = Topology("test_TopologyTransformWithDrop")
     source = topo.source(test_functions.int_strings_transform_with_drop)
     i1 = source.map(test_functions.string_to_int_except68)
     i2 = i1.map(test_functions.add17)
     i2.sink(test_functions.check_int_strings_transform_with_drop)
     standalone(self, topo)
     
  def test_TopologyMultiTransform(self):
      topo = Topology("test_TopologyMultiTransform")
      source = topo.source(test_functions.strings_multi_transform)
      i1 = source.multi_transform(test_functions.split_words)
      i1.sink(test_functions.check_strings_multi_transform)
      standalone(self, topo)
      
  def test_TopologyTransformCallableAddWithDrop(self):
      topo = Topology("test_TopologyTransformCallableAddWithDrop")
      source = topo.source(test_functions.int_strings_transform_with_drop)
      i1 = source.transform(test_functions.string_to_int_except68)
      i2 = i1.transform(test_functions.AddNum(17))
      i2.sink(test_functions.check_int_strings_transform_with_drop)
      standalone(self, topo)
  
  def test_TopologyMultiTransformCallableIncMaxSplit(self):
      topo = Topology("test_TopologyMultiTransformCallableIncMaxSplit")
      source = topo.source(test_functions.strings_multi_transform)
      i1 = source.flat_map(test_functions.IncMaxSplitWords(1))
      i1.sink(test_functions.check_strings_multi_transform_inc_max_split)
      standalone(self, topo)
  
  def test_TopologySourceAndSinkCallable(self):
      topo = Topology("test_TopologySourceAndSinkCallable")
      hw = topo.source(test_functions.SourceTuplesAppendIndex(["a", "b", "c", "d"]))
      hw.sink(test_functions.CheckTuples(["a0", "b1", "c2", "d3"]))
      standalone(self, topo)
    
  def test_TopologyParallel(self):
      topo = Topology("test_TopologyParallel")
      hw = topo.source(test_functions.seedSource)   
      hwp = hw.parallel(4)
      hwf = hwp.transform(test_functions.ProgramedSeed())
      hwef = hwf.end_parallel()
      hwef.sink(test_functions.SeedSinkRR())
      standalone(self, topo)
      
  def test_TopologyHashedFuncParallel(self):
      topo = Topology("test_TopologyHashedFuncParallel")
      hw = topo.source(test_functions.seedSource)   
      hwp = hw.parallel(4,Routing.HASH_PARTITIONED,test_functions.produceHash)
      hwf = hwp.transform(test_functions.ProgramedSeed())
      hwef = hwf.end_parallel()
      hwef.sink(test_functions.SeedSinkHashOrKey())
      standalone(self, topo)

      
  def test_TopologyHashedParallel(self):
      topo = Topology("test_TopologyHashedParallel")
      hw = topo.source(test_functions.seedSource)   
      hwp = hw.parallel(4,Routing.HASH_PARTITIONED)
      hwf = hwp.transform(test_functions.ProgramedSeed())
      hwef = hwf.end_parallel()
      hwef.sink(test_functions.SeedSinkHashOrKey())
      standalone(self, topo)

  def test_TopologyUnion(self):
      topo = Topology("test_TopologyUnion")
      h = topo.source(test_functions.hello)
      b = topo.source(test_functions.beautiful)
      c = topo.source(test_functions.crazy)
      w = topo.source(test_functions.world)
      streamSet = {h, w, b, c}
      hwu = h.union(streamSet)
      hwu.sink(test_functions.check_union_hello_world)
      standalone(self, topo)
      
  def test_TopologyParallelUnion(self):
      topo = Topology("test_TopologyParallelUnion")
      hw = topo.source(test_functions.seedSource)
      hwp = hw.parallel(4)
      hwf = hwp.transform(test_functions.ProgramedSeed())
      hwf2 = hwp.transform(test_functions.ProgramedSeed())    
      streamSet = {hwf2}
      hwu = hwf.union(streamSet)
      hwup = hwu.end_parallel()
      hwup.sink(test_functions.SeedSinkRRPU())
      standalone(self, topo)

  # test using input functions from a regular package that has __init__.py
  # test using input functions that are fully qualified
  def test_TopologyImportPackage(self):
      import test_package.test_subpackage.test_module
      try:
          topo = Topology("test_TopologyImportPackage")
          hw = topo.source(test_package.test_subpackage.test_module.SourceTuples(["Hello", "World!"]))
          hwf = hw.filter(test_package.test_subpackage.test_module.filter)
          hwf.sink(test_package.test_subpackage.test_module.CheckTuples(["Hello"]))
          standalone(self, topo)
      finally:
          pass
      
  # test using input functions from an implicit namespace package that doesn't have a __init__.py
  # test using input functions that are qualified using a module alias  e.g. 'test_ns_module'
  # test using input functions from a mix of packages and individual modules
  def test_TopologyImportNamespacePackage(self):
      from test_namespace_package.test_subpackage import test_module as test_ns_module
      try:
          topo = Topology("test_TopologyImportNamespacePackage")
          hw = topo.source(test_ns_module.SourceTuples(["Hello", "World!"]))
          hwf = hw.filter(test_functions.filter)
          hwf.sink(test_ns_module.CheckTuples(["World!"]))
          standalone(self, topo)
      finally:
          del test_ns_module

  # test using input functions from a namespace package that merges separate packages into a
  # common namespace    
  def test_TopologyImportCommonNamespacePackage(self):
      this_dir = os.path.dirname(os.path.realpath(__file__))
      tcn = os.path.join(this_dir, 'test_common_namespace')
      tcn_paths = [os.path.join(tcn, 'package1'), os.path.join(tcn,'package2')]
      sys.path.extend(tcn_paths)
      import common_namespace.module1
      import common_namespace.module2
      try:
          topo = Topology("test_TopologyImportCommonNamespacePackage")
          hw = topo.source(common_namespace.module1.SourceTuples(["Hello", "World!"]))
          hwf = hw.filter(common_namespace.module2.filter)
          hwf.sink(common_namespace.module2.CheckTuples(["World!"]))
          standalone(self, topo)
      finally:
          for p in tcn_paths:
              sys.path.remove(p)
          del common_namespace.module1, common_namespace.module2

  # test using input functions from a module that imports another module
  def test_TopologyImportModuleWithDependencies(self):
      import test_functions2
      try:
          topo = Topology("test_TopologyImportModuleWithDependencies")
          hw = topo.source(test_functions2.hello_world)
          hwf = hw.filter(test_functions2.filter)
          hwf.sink(test_functions2.check_hello_world_filter)
          standalone(self, topo)
      finally:
          del test_functions2

class TestPlaceable(unittest.TestCase):

  def test_placeable(self):
      topo = Topology()
      s1 = topo.source([])
      self.assertFalse(s1.resource_tags)
      self.assertIsInstance(s1.resource_tags, set)
      s1.resource_tags.add('ingest')
      s1.resource_tags.add('db')
      self.assertEqual({'ingest', 'db'}, s1.resource_tags)

      s2 = s1.filter(lambda x : True)
      s2.resource_tags.add('cpu1')
      self.assertEqual({'cpu1'}, s2.resource_tags)

      s3 = s1.map(lambda x : x)
      s3.resource_tags.add('cpu2')
      self.assertEqual({'cpu2'}, s3.resource_tags)

      s4 = s1.flat_map(lambda x : [x])
      s4.resource_tags.add('cpu3')
      self.assertEqual({'cpu3'}, s4.resource_tags)

      self.assertEqual({'ingest', 'db'}, s1.resource_tags)
      self.assertEqual({'cpu1'}, s2.resource_tags)
      self.assertEqual({'cpu2'}, s3.resource_tags)
      self.assertEqual({'cpu3'}, s4.resource_tags)

  def test_not_placeable(self):
      topo = Topology()
      s1 = topo.source([])
      s2 = topo.source([])
      s3 = s1.union({s2})
      self._check_not_placeable(s3)
      self._check_not_placeable(s1.autonomous())
      self._check_not_placeable(s1.isolate())
      self._check_not_placeable(s1.parallel(3))
      self._check_not_placeable(s1.low_latency())

  def _check_not_placeable(self, s):
      self.assertFalse(s.resource_tags)
      self.assertIsInstance(s.resource_tags, frozenset)
