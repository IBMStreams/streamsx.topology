# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
import unittest
import os
import sys
import itertools
import logging

from streamsx.topology.topology import *
from streamsx.topology.context import JobConfig, ConfigParams
from streamsx.topology.tester import Tester
from streamsx import rest
from streamsx.rest_primitives import _IAMConstants

class TestJobConfig(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
      sc = rest.StreamingAnalyticsConnection()

  def test_UnicodeJobName(self):
     """ Test unicode topo names
     """
     job_name = '你好世界'
     topo = Topology()
     jc = JobConfig(job_name=job_name)

     # When tracing is info some extra code is invoked
     # to trace all Python packages. Ensure it is exercised.
     jc.tracing = 'info'

     hw = topo.source(["Hello", "Tester"])
     tester = Tester(topo)
     tester.contents(hw, ["Hello", "Tester"])
     
     jc.add(self.test_config)
     self.assertIs(jc, self.test_config[ConfigParams.JOB_CONFIG])

     sr = tester.test(self.test_ctxtype, self.test_config)
     self.assertEqual(job_name, tester.result['submission_result']['name'])

def set_trace(jc, level):
    jc.tracing = level

class TestTracingLevels(unittest.TestCase):
    def test_levels(self):
        jc = JobConfig() 
        self.assertIs(None, jc.tracing)

        for tl in {'error', 'warn', 'info', 'debug', 'trace'}:
            jc.tracing = tl
            self.assertEqual(tl, jc.tracing)

        jc.tracing = None
        self.assertIs(None, jc.tracing)

        jc.tracing = logging.CRITICAL
        self.assertEqual('error', jc.tracing)
        jc.tracing = logging.ERROR
        self.assertEqual('error', jc.tracing)

        jc.tracing = logging.WARNING
        self.assertEqual('warn', jc.tracing)

        jc.tracing = logging.INFO
        self.assertEqual('info', jc.tracing)

        jc.tracing = logging.DEBUG
        self.assertEqual('debug', jc.tracing)

        jc.tracing = logging.NOTSET
        self.assertIs(None, jc.tracing)

        self.assertRaises(ValueError, set_trace, jc, 'WARN')

        jc2 = JobConfig(tracing='info')
        self.assertEqual('info', jc2.tracing)


class TestRawOverlay(unittest.TestCase):
     def test_property(self):
        jc = JobConfig() 
        self.assertIs(None, jc.raw_overlay)
        raw = {}
        jc.raw_overlay = raw
        self.assertIs(raw, jc.raw_overlay)

        raw['jobConfig'] = {'jobName': 'myjob72'}
        raw['something'] = {'stuff': 3}

        self.assertIs(raw, jc.raw_overlay)
        self.assertEqual('myjob72', jc.raw_overlay['jobConfig']['jobName'])

        gc = {}
        jc._add_overlays(gc) #type: ignore

        self.assertTrue('jobConfigOverlays' in gc)
        jcol = gc['jobConfigOverlays']
        self.assertIsInstance(jcol, list)
        self.assertEqual(1, len(jcol))

        jco = jcol[0]
        self.assertIsInstance(jco, dict)

        # Copied as-is
        self.assertEqual(raw, jco)

     def test_merge(self):
        jc = JobConfig(job_name='Merge') 
        jc.target_pe_count = 7

        jc.raw_overlay = {'jobConfig': {'jobGroup':'mygroup82'}}
        jc.raw_overlay['deploymentConfig'] = {'threadingModel':'manual'}
        jc.raw_overlay['other'] = {'xx':'yyy'}

        gc = {}
        jc._add_overlays(gc) #type: ignore

        self.assertTrue('jobConfigOverlays' in gc)
        jcol = gc['jobConfigOverlays']
        self.assertIsInstance(jcol, list)
        self.assertEqual(1, len(jcol))

        jco = jcol[0]
        self.assertIsInstance(jco, dict)
        self.assertEqual(3, len(jco))

        # test unknown value copied as-is
        self.assertTrue('other' in jco)
        self.assertEqual(jc.raw_overlay['other'], jco['other'])

        # test merge of job config
        self.assertTrue('jobConfig' in jco)
        jjc = jco['jobConfig']
        self.assertIsInstance(jjc, dict)
        self.assertEqual(2, len(jjc))
        self.assertEqual('Merge', jjc.get('jobName'))
        self.assertEqual('mygroup82', jjc.get('jobGroup'))

        # test merge of deployment config
        self.assertTrue('deploymentConfig' in jco)
        dc = jco['deploymentConfig']
        self.assertIsInstance(dc, dict)
        self.assertEqual(3, len(dc))
        self.assertEqual('manual', dc.get('threadingModel'))
        self.assertEqual('manual', dc.get('fusionScheme'))
        self.assertEqual(7, dc.get('fusionTargetPeCount'))

     def test_overwrite(self):
        jc = JobConfig('Overwrite') 

        raw = {}
        raw['jobConfig'] = {'jobName': 'myjob72', 'jobGroup': 'gg'}
        raw['deploymentConfig'] = {'fusionScheme': 'dummy', 'other': 'xx'}
        raw['unknown'] = {'hasdsd': 32532}
        jc.raw_overlay = raw

        jc.target_pe_count = 93
        self.assertEqual('Overwrite', jc.job_name)
        self.assertEqual(93, jc.target_pe_count)

        gc = {}
        jc._add_overlays(gc) #type: ignore

        self.assertTrue('jobConfigOverlays' in gc)
        jcol = gc['jobConfigOverlays']
        self.assertIsInstance(jcol, list)
        self.assertEqual(1, len(jcol))

        jco = jcol[0]
        self.assertIsInstance(jco, dict)
        self.assertEqual(3, len(jco))

        # test unknown value copied as-is
        self.assertTrue('unknown' in jco)
        self.assertEqual(jc.raw_overlay['unknown'], jco['unknown'])

        # test overwrite/merge of job config
        self.assertTrue('jobConfig' in jco)
        jjc = jco['jobConfig']
        self.assertIsInstance(jjc, dict)
        self.assertEqual(2, len(jjc))
        self.assertEqual('Overwrite', jjc.get('jobName'))
        self.assertEqual('gg', jjc.get('jobGroup'))

        # test overwrite/merge of deployment config
        self.assertTrue('deploymentConfig' in jco)
        dc = jco['deploymentConfig']
        self.assertIsInstance(dc, dict)
        self.assertEqual(3, len(dc))
        self.assertEqual('xx', dc.get('other'))
        self.assertEqual('manual', dc.get('fusionScheme'))
        self.assertEqual(93, dc.get('fusionTargetPeCount'))

class TestOverlays(unittest.TestCase):
    def _check_overlays(self, jc):
        ov = jc.as_overlays()
        self.assertTrue(isinstance(ov, dict))
        self.assertTrue(len(ov) <= 2)

        self.assertIn('jobConfigOverlays', ov)
        jcos = ov['jobConfigOverlays']
        self.assertTrue(isinstance(jcos, list))
        self.assertEqual(1, len(jcos))
        self.assertTrue(isinstance(jcos[0], dict))

        if len(ov) == 2:
            self.assertTrue(jc.comment)
            self.assertIn('comment', ov)
            self.assertTrue(isinstance(ov['comment'], str))
            self.assertEqual(jc.comment, ov['comment'])

        return jcos[0]

    def test_no_comment(self):
        jc = JobConfig()
        self.assertIsNone(jc.comment)
        ov = self._check_overlays(jc)
        self.assertNotIn('comment', ov)

    def test_comment(self):
        jc = JobConfig()
        jc.comment = 'Test configuration'
        self.assertEqual('Test configuration', jc.comment)
        self._check_overlays(jc)

    def test_non_empty(self):
        jc = JobConfig(job_name='TestIngester')
        jc.comment = 'Test configuration'
        jc.target_pe_count = 2
        jco = self._check_overlays(jc)
        self.assertIn('jobConfig', jco)
        self.assertIn('jobName', jco['jobConfig'])
        self.assertEqual('TestIngester', jco['jobConfig']['jobName'])

    def test_from_overlays(self):

        self._check_matching(JobConfig())

        jc = JobConfig(job_name='TestIngester', preload=True, data_directory='/tmp/a', job_group='gg', tracing='info')
        jc.comment = 'Test configuration'
        jc.target_pe_count = 2
        self._check_matching(jc)

        jc = JobConfig(job_name='TestIngester2')
        jc.comment = 'Test configuration2'
        self._check_matching(jc)

        jc = JobConfig(preload=True)
        jc.raw_overlay = {'a': 34}
        self._check_matching(jc)

        jc = JobConfig(preload=True)
        jc.raw_overlay = {'x': 'fff'}
        jc.submission_parameters['one'] = 1
        jc.submission_parameters['two'] = 2
        self._check_matching(jc)

    @unittest.skipUnless('STREAMS_INSTALL' in os.environ, "requires STREAMS_INSTALL")
    def test_from_topology(self):
        topo = Topology('SabTest', namespace='mynamespace')
        s = topo.source([1,2])
        es = s.for_each(lambda x : None)
        cfg = {}
        jc = JobConfig(job_name='ABCD', job_group='XXG', preload=True)
        jc.add(cfg)
        bb = streamsx.topology.context.submit('BUNDLE', topo, cfg)
        self.assertIn('bundlePath', bb)
        self.assertIn('jobConfigPath', bb)

        with open(bb['jobConfigPath']) as json_data:
            jct = JobConfig.from_overlays(json.load(json_data))
            self.assertEqual(jc.job_name, jct.job_name)
            self.assertEqual(jc.job_group, jct.job_group)
            self.assertEqual(jc.preload, jct.preload)
            
        os.remove(bb['bundlePath'])
        os.remove(bb['jobConfigPath'])


    def _check_matching(self, jcs):
        jcf = JobConfig.from_overlays(jcs.as_overlays())

        self.assertEqual(jcs.comment, jcf.comment)

        self.assertEqual(jcs.job_name, jcf.job_name)
        self.assertEqual(jcs.job_group, jcf.job_group)
        self.assertEqual(jcs.preload, jcf.preload)
        self.assertEqual(jcs.data_directory, jcf.data_directory)
        self.assertEqual(jcs.tracing, jcf.tracing)

        self.assertEqual(jcs.target_pe_count, jcf.target_pe_count)

        self.assertEqual(jcs.submission_parameters, jcf.submission_parameters)

        self.assertEqual(jcs.raw_overlay, jcf.raw_overlay)
