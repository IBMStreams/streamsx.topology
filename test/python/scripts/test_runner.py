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

    def _spl_app_args(self, name='ns1::MyApp'):
        args = self._service_args()
        args.append('--main-composite')
        args.append(name)
        args.append('--toolkit')
        args.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'spl_app'))
        return args

    def _run(self, args, cancel=True):
        with unittest.mock.patch('sys.argv', args):
             sr = streamsx.scripts.runner.main()
             self.assertIn('jobId', sr)
             self.assertIsNotNone(sr['jobId'])
             if cancel:
                 sr.job.cancel()
             return sr
 
    def test_topo_submit(self):
        args = self._service_args()
        args.append('--topology')
        args.append('test_runner.app_topology')
        self._run(args)
  
    def test_topo_submit_with_config(self):
        args = self._service_args()
        args.append('--topology')
        args.append('test_runner.app_with_job_config')
        sr = self._run(args)
        self.assertTrue(sr.name.startswith('RJN_'))

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

    def test_spl_app(self):
        args = self._spl_app_args()
        self._run(args)

    def test_spl_app_with_job_name(self):
        args = self._spl_app_args()
        args.append('--job-name')
        jn = random_job_name(prefix='SPL_JOB_NAME_')
        args.append(jn)
        sr = self._run(args)
        self.assertEqual(jn, sr.name)

    def test_spl_app_with_trace(self):
        args = self._spl_app_args()
        args.append('--trace')
        args.append('debug')
        args.append('--submission-parameters')
        self._run(args)

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
