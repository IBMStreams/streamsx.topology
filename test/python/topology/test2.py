# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import os

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import test_vers

def s4():
   return ['one', 'two', 'three', 'four']

def assertBundlePath(test, submissionResult):
   test.assertIn('bundlePath', submissionResult)
   test.assertTrue(os.path.isfile(submissionResult['bundlePath']))

def assertToolkitRoot(test, submissionResult):
   test.assertIn('toolkitRoot', submissionResult)
   test.assertTrue(os.path.isdir(submissionResult['toolkitRoot']))

def assertArchivePath(test, submissionResult):
   test.assertIn('archivePath', submissionResult)
   test.assertTrue(os.path.isfile(submissionResult['archivePath']))

# Verify if correct artifacts are left behind when keepArtifacts is True (assume context is submit type)
def verifyKeepArtifacts(test, submissionResult):
   if 'STREAMS_INSTALL' in os.environ:
      assertToolkitRoot(test, submissionResult)
      assertBundlePath(test, submissionResult)
      test.assertNotIn('archivePath', submissionResult)
   else:
      assertToolkitRoot(test, submissionResult)
      assertArchivePath(test, submissionResult)
      test.assertNotIn('bundlePath', submissionResult)

# Verify that no artifacts are left behind when keepArtifacts is False
def verifyNoKeepArtifacts(test, submissionResult):
   test.assertNotIn('toolkitRoot', submissionResult)
   test.assertNotIn('archivePath', submissionResult)
   test.assertNotIn('bundlePath', submissionResult)

@unittest.skipIf(not test_vers.tester_supported(), "Tester not supported")
@unittest.skipUnless('STREAMS_INSTALL' in os.environ, "requires STREAMS_INSTALL")
class TestBundleMethodsNew(unittest.TestCase):

   def test_Bundle(self):
      topo = Topology('test_TopologySource')
      hw = topo.source(['Hello', 'Bundle'])
      result = streamsx.topology.context.submit('BUNDLE', topo)
      self.assertNotIn('toolkitRoot', result)
      self.assertNotIn('archivePath', result)
      assertBundlePath(self, result)

   def test_BundleKeepArtifacts(self):
      topo = Topology('test_TopologySource')
      hw = topo.source(['Hello', 'Bundle'])
      result = streamsx.topology.context.submit('BUNDLE', topo, config={'topology.keepArtifacts': True})
      verifyKeepArtifacts(self, result)

@unittest.skipIf(not test_vers.tester_supported(), "Tester not supported")
class TestBuildArchiveMethodsNew(unittest.TestCase):

   def test_BuildArchive(self):
      topo = Topology('test_TopologySource')
      hw = topo.source(['Hello', 'Bundle'])
      result = streamsx.topology.context.submit('BUILD_ARCHIVE', topo)
      self.assertNotIn('toolkitRoot', result)
      self.assertNotIn('bundlePath', result)
      assertArchivePath(self, result)

   def test_BuildArchiveKeepArtifacts(self):
      topo = Topology('test_TopologySource')
      hw = topo.source(['Hello', 'Bundle'])
      result = streamsx.topology.context.submit('BUILD_ARCHIVE', topo, config={'topology.keepArtifacts': True})
      assertToolkitRoot(self, result)
      assertArchivePath(self, result)
      self.assertNotIn('bundlePath', result)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestTopologyMethodsNew(unittest.TestCase):

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologySourceList(self):
     topo = Topology('test_TopologySourceList')
     hw = topo.source(['Hello', 'Tester'])
     tester = Tester(topo)
     tester.contents(hw, ['Hello', 'Tester'])
     tester.test(self.test_ctxtype, self.test_config)
     result = tester.result['submission_result']
     if self.test_config.get('topology.keepArtifacts', False):
        verifyKeepArtifacts(self, result)
     else:
        verifyNoKeepArtifacts(self, result)

  def test_TopologySourceFn(self):
     topo = Topology('test_TopologySourceFn')
     hw = topo.source(s4)
     tester = Tester(topo)
     tester.contents(hw, s4())
     tester.tuple_count(hw, len(s4()))
     self.test_config['topology.keepArtifacts'] = True
     tester.test(self.test_ctxtype, self.test_config)
     verifyKeepArtifacts(self, tester.result['submission_result'])

  def test_TopologySourceItertools(self):
     topo = Topology('test_TopologySourceItertools')
     hw = topo.source(itertools.repeat(9, 3))
     tester = Tester(topo)
     tester.contents(hw, [9, 9, 9])
     tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedTopologyMethodsNew(TestTopologyMethodsNew):
  def setUp(self):
      Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixTopologyMethodsNew(TestTopologyMethodsNew):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)