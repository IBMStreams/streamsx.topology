# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import os
import shutil

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams, submit
from streamsx import rest
import test_functions

def s4():
    return ['one', 'two', 'three', 'four']

def removeArtifacts(submissionResult):
    if 'bundlePath' in submissionResult:
        os.remove(submissionResult['bundlePath'])
    if 'toolkitRoot' in submissionResult:
        shutil.rmtree(submissionResult['toolkitRoot'])
    if 'archivePath' in submissionResult:
        os.remove(submissionResult['archivePath'])
    if 'jobConfigPath' in submissionResult:
        os.remove(submissionResult['jobConfigPath'])

def assertBundlePath(test, submissionResult):
    test.assertIn('bundlePath', submissionResult)
    test.assertTrue(os.path.isfile(submissionResult['bundlePath']))
    if ((test.test_ctxtype == 'BUNDLE' or
         test.test_ctxtype != 'STANDALONE' and
         test.test_config.get('topology.keepArtifacts', False))):
        test.assertIn('jobConfigPath', submissionResult)
        test.assertTrue(os.path.isfile(submissionResult['jobConfigPath']))
    else:
        test.assertNotIn('jobConfigPath', test.result)

def assertToolkitRoot(test, submissionResult):
    test.assertIn('toolkitRoot', submissionResult)
    test.assertTrue(os.path.isdir(submissionResult['toolkitRoot']))

def assertArchivePath(test, submissionResult):
    test.assertIn('archivePath', submissionResult)
    test.assertTrue(os.path.isfile(submissionResult['archivePath']))

def verifyArtifacts(test):
    if test.test_config.get('topology.keepArtifacts', False):
        # KeepArtifacts is True
        assertToolkitRoot(test, test.result)
        if 'TOOLKIT' == test.test_ctxtype:
            test.assertNotIn('bundlePath', test.result)
            test.assertNotIn('archivePath', test.result)
        elif (test.test_config.get('topology.forceRemoteBuild', False) or
              'STREAMS_INSTALL' not in os.environ or
              'BUILD_ARCHIVE' == test.test_ctxtype):
            assertArchivePath(test, test.result)
            test.assertNotIn('bundlePath', test.result)
        else:
            assertBundlePath(test, test.result)
            test.assertNotIn('archivePath', test.result)
    else:
        # KeepArtifacts is False
        if 'TOOLKIT' == test.test_ctxtype:
            assertToolkitRoot(test, test.result)
        else:
            test.assertNotIn('toolkitRoot', test.result)
        if 'BUNDLE' == test.test_ctxtype:
            assertBundlePath(test, test.result)
            test.assertNotIn('archivePath', test.result)
        elif 'BUILD_ARCHIVE' == test.test_ctxtype:
            assertArchivePath(test, test.result)
            test.assertNotIn('bundlePath', test.result)
        else:
            test.assertNotIn('bundlePath', test.result)
            test.assertNotIn('archivePath', test.result)

class TestToolkitMethodsNew(unittest.TestCase):

    def setUp(self):
        self.topo = Topology('test_ToolkitSource')
        #self.topo.source(['Hello', 'Toolkit'])
        self.topo.source('Toolkit')
        self.test_ctxtype = 'TOOLKIT'
        self.test_config = {}
        self.result = {}

    def tearDown(self):
        removeArtifacts(self.result)

    def test_NoKeepArtifacts(self):
        self.result = submit(self.test_ctxtype, self.topo, self.test_config)
        verifyArtifacts(self)

    def test_KeepArtifacts(self):
        self.test_config['topology.keepArtifacts'] = True
        self.result = submit(self.test_ctxtype, self.topo, self.test_config)
        verifyArtifacts(self)

class TestBuildArchiveMethodsNew(TestToolkitMethodsNew):

    def setUp(self):
        self.topo = Topology('test_BuildArchiveSource')
        self.topo.source(['Hello', 'BuildArchive'])
        self.test_ctxtype = 'BUILD_ARCHIVE'
        self.test_config = {}
        self.result = {}

@unittest.skipUnless('STREAMS_INSTALL' in os.environ, "requires STREAMS_INSTALL")
class TestBundleMethodsNew(TestToolkitMethodsNew):

    def setUp(self):
        self.topo = Topology('test_BundleSource')
        self.topo.source(['Hello', 'Bundle'])
        self.test_ctxtype = 'BUNDLE'
        self.test_config = {}
        self.result = {}

@unittest.skipUnless('STREAMS_INSTALL' in os.environ, "requires STREAMS_INSTALL")
class TestDistributedSubmitMethodsNew(unittest.TestCase):

    def setUp(self):
        self.topo = Topology('test_DistributedSubmit')
        self.topo.source(['Hello', 'DistributedSubmit'])
        self.test_ctxtype = 'DISTRIBUTED'
        self.test_config = {}

    def test_DifferentUsername(self):
        sc = rest.StreamsConnection('user1', 'pass1')
        self.test_config[ConfigParams.STREAMS_CONNECTION] = sc
        with self.assertRaises(RuntimeError):
            submit(self.test_ctxtype, self.topo, self.test_config, username='user2', password='pass1')

    def test_DifferentPassword(self):
        sc = rest.StreamsConnection('user1', 'pass1')
        self.test_config[ConfigParams.STREAMS_CONNECTION] = sc
        with self.assertRaises(RuntimeError):
            submit(self.test_ctxtype, self.topo, self.test_config, username='user1', password='pass2')

@unittest.skipUnless('VCAP_SERVICES' in os.environ, "requires VCAP_SERVICES")
@unittest.skipUnless('STREAMING_ANALYTICS_SERVICE_NAME' in os.environ, "requires STREAMING_ANALYTICS_SERVICE_NAME")
class TestBluemixSubmitMethodsNew(unittest.TestCase):

    def setUp(self):
        self.topo = Topology('test_BluemixSubmit')
        self.topo.source(['Hello', 'BluemixSubmit'])
        self.test_ctxtype = 'STREAMING_ANALYTICS_SERVICE'
        self.test_config = {}

    def test_StreamsConnection(self):
        sc = rest.StreamsConnection('user1', 'pass1')
        self.test_config[ConfigParams.STREAMS_CONNECTION] = sc
        with self.assertRaises(ValueError):
            submit(self.test_ctxtype, self.topo, self.test_config)

    def test_StreamingAnalyticsConnection(self):
        sc = rest.StreamingAnalyticsConnection()
        self.test_config[ConfigParams.STREAMS_CONNECTION] = sc
        result = submit(self.test_ctxtype, self.topo, self.test_config)
        self.assertEqual(result.return_code, 0)
        result.job.cancel()


class TestTopologyMethodsNew(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)
        self.result = {}

    def tearDown(self):
        removeArtifacts(self.result)

    def test_TopologySourceList(self):
        topo = Topology('test_TopologySourceList')
        hw = topo.source(['Hello', 'Tester'])
        tester = Tester(topo)
        tester.contents(hw, ['Hello', 'Tester'])
        tester.test(self.test_ctxtype, self.test_config)
        self.result = tester.result['submission_result']
        verifyArtifacts(self)

    def test_TopologySourceFn(self):
        topo = Topology('test_TopologySourceFn')
        hw = topo.source(s4)
        tester = Tester(topo)
        tester.contents(hw, s4())
        tester.tuple_count(hw, len(s4()))
        self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)
        self.result = tester.result['submission_result']
        verifyArtifacts(self)

    def test_no_func_map(self):
        topo = Topology()
        s = topo.source([(1,2),(3,4),(5,6)])
        snc = s.map().map(name='NoChange!')
        ss = snc.map(schema='tuple<int32 x, int32 y>')
        tester = Tester(topo)
        tester.contents(snc, [(1,2),(3,4),(5,6)])
        tester.contents(ss, [{'x':1,'y':2}, {'x':3,'y':4}, {'x':5,'y':6}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_no_func_flat_map(self):
        topo = Topology()
        s = topo.source(['World', 'Cup', '2018'])
        s1 = s.flat_map()
        s2 = s.flat_map(name='JustFlatten!')
        tester = Tester(topo)
        tester.contents(s1, 'WorldCup2018')
        tester.tuple_count(s1, 12)
        tester.contents(s2, 'WorldCup2018')
        tester.tuple_count(s2, 12)
        tester.test(self.test_ctxtype, self.test_config)

    def test_TopologySourceItertools(self):
        topo = Topology('test_TopologySourceItertools')
        if sys.version_info.major == 2:
            # Iterators not serializable in 2.7
            hw = topo.source(lambda : itertools.repeat(9, 3))
        else:
            hw = topo.source(itertools.repeat(9, 3))

        if sys.version_info.major == 2:
            # Disabling assertions not supported on Python 2.7
            # See splpy_setup.h
            pass
        else:
            hw = hw.filter(test_functions.check_asserts_disabled)
        tester = Tester(topo)
        tester.contents(hw, [9, 9, 9])
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedTopologyMethodsNew(TestTopologyMethodsNew):
    def setUp(self):
        Tester.setup_distributed(self)
        self.result = {}

class TestBluemixTopologyMethodsNew(TestTopologyMethodsNew):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.result = {}
