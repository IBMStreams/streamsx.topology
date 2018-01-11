# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import sys
import itertools
import logging

from streamsx.topology.topology import *
from streamsx.topology.context import JobConfig
from streamsx.topology.context import ConfigParams
from streamsx.topology.tester import Tester
from streamsx import rest
from streamsx.rest_primitives import _IAMConstants

import test_vers

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestJobConfig(unittest.TestCase):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
      sc = rest.StreamingAnalyticsConnection()
      self.is_v2 = _IAMConstants.V2_REST_URL in sc.credentials

  # Known failure. Submitting a jobconfig during a remote build submission is not supported.
  def test_UnicodeJobName(self):
     """ Test unicode topo names
     """
     if self.is_v2:
       return unittest.expectedFailure(self)
     job_name = '你好世界'
     topo = Topology()
     jc = streamsx.topology.context.JobConfig(job_name=job_name)

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
        jc._add_overlays(gc)

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
        jc._add_overlays(gc)

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
        jc._add_overlays(gc)

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
        
