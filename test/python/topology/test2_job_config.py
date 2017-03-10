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

import test_vers

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestJobConfig(unittest.TestCase):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)

  def test_UnicodeJobName(self):
     """ Test unicode topo names
     """
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
