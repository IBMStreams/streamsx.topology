# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
import unittest
import unittest.mock
import sys
import os
import random
import string

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.context import JobConfig

import streamsx.scripts.runner
import streamsx.rest

def _service_ok():
    return 'VCAP_SERVICES' in os.environ and 'STREAMING_ANALYTICS_SERVICE_NAME' in os.environ

def _spl_ok():
    return 'STREAMS_INSTALL' in os.environ

def app_topology():
    topo = Topology()
    topo.source(['Hello', 'World']).print()
    return topo

def random_job_name(prefix='RJN_'):
    return prefix + ''.join(random.choice('ABCDEFGIHIJLK') for i in range(16))

def app_with_job_config():
    jc = JobConfig(random_job_name())
    return (app_topology(), jc)

class TestRunnerService(unittest.TestCase):

    def _service_args(self):
        args = []
        args.append('runner.py')
        args.append('--service-name')
        args.append(os.environ['STREAMING_ANALYTICS_SERVICE_NAME'])
        return args

    def _spl_tk_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'spl_app')

    def _spl_app_args(self, name='ns1::MyApp', service=True):
        args = self._service_args() if service else []
        args.append('--main-composite')
        args.append(name)
        args.append('--toolkit')
        args.append(self._spl_tk_path())
        return args

    def _run(self, args, cancel=True):
        with unittest.mock.patch('sys.argv', args):
             sr = streamsx.scripts.runner.submit()
             job_id = None
             if 'id' in sr:
                 job_id = sr['id']
             elif 'jobId' in sr:
                 job_id = sr['jobId']
          
             self.assertIsNotNone(job_id)
             self.assertEqual(0, sr['return_code'])
             if cancel:
                 if hasattr(sr, 'job'):
                     sr.job.cancel()
                 else:
                     sas = streamsx.rest.StreamingAnalyticsConnection().get_streaming_analytics()
                     sas.cancel_job(job_id)
             return sr
 
    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_topo_submit(self):
        args = self._service_args()
        args.append('--topology')
        args.append('test_runner.app_topology')
        self._run(args)
  
    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_topo_submit_with_config(self):
        args = self._service_args()
        args.append('--topology')
        args.append('test_runner.app_with_job_config')
        sr = self._run(args)
        self.assertTrue(sr.name.startswith('RJN_'))

    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_topo_submit_job_name(self):
        args = self._service_args()
        args.append('--topology')
        args.append('test_runner.app_with_job_config')
        args.append('--preload')
        args.append('--job-name')
        jn = random_job_name(prefix='MANUAL_JOB_NAME_')
        args.append(jn)
        sr = self._run(args)
        self.assertEqual(jn, sr.name)

    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_spl_app(self):
        args = self._spl_app_args()
        self._run(args)

    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_spl_app_with_job_name(self):
        args = self._spl_app_args()
        args.append('--job-name')
        jn = random_job_name(prefix='SPL_JOB_NAME_')
        args.append(jn)
        sr = self._run(args)
        self.assertEqual(jn, sr.name)

    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_spl_app_with_trace(self):
        args = self._spl_app_args()
        args.append('--trace')
        args.append('debug')
        self._run(args)

    @unittest.skipIf(not _service_ok(), "Service not defined")
    def test_spl_app_with_params(self):
        args = self._spl_app_args(name='ns1::SPApp')
        args.append('--submission-parameters')
        args.append('period=3.4')
        args.append('iters=100')
        args.append('--job-name')
        jn = random_job_name(prefix='SP_JOB_')
        args.append(jn)
        sr = self._run(args)
        self.assertEqual(jn, sr.name)

    def _create_bundle_args(self):
        args = self._spl_app_args(service=False)
        args.insert(0, '--create-bundle')
        args.insert(0, 'runner.py')
        return args

    def _create_bundle(self):
        args = self._create_bundle_args()
        with unittest.mock.patch('sys.argv', args):
             sr = streamsx.scripts.runner.submit()
        self.assertIn('bundlePath', sr)
        self.assertIn('jobConfigPath', sr)
        self.assertTrue(os.path.exists(sr['bundlePath']))
        self.assertTrue(sr['bundlePath'].endswith('sab'))
        self.assertTrue(os.path.exists(sr['jobConfigPath']))
        self.assertTrue(sr['jobConfigPath'].endswith('.json'))
        return sr

    @unittest.skipIf(not _spl_ok(), 'STREAMS_INSTALL not set')
    def test_create_bundle(self):
        sr = self._create_bundle()
        os.remove(sr['bundlePath'])
        os.remove(sr['jobConfigPath'])
        
    @unittest.skipIf(not _spl_ok() or not _service_ok(), "Service & STREAMS_INSTALL not defined")
    def test_submit_bundle(self):
        sr = self._create_bundle()
        args = self._service_args()
        args.append('--bundle')
        args.append(sr['bundlePath'])
        args.append('--job-config-overlays')
        args.append(sr['jobConfigPath'])
        srr = self._run(args)

        jn = random_job_name()
        args.append('--job-name')
        args.append(jn)
        srr = self._run(args)
        self.assertEqual(jn, srr['name'])
        
        os.remove(sr['bundlePath'])
        os.remove(sr['jobConfigPath'])

    def test_simple_main(self):
        args = self._service_args()
        args.append('--main-composite')
        args.append('Main')
        with unittest.mock.patch('sys.argv', args):
             self.assertRaises(ValueError, streamsx.scripts.runner.main)
