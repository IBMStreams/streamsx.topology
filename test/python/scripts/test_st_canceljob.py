# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import unittest.mock
import sys
import os
import time
import requests

from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
import streamsx.scripts.streamtool as streamtool

# Tests streamtool canceljob script.
#
# Requires environment setup for a ICP4D Streams instance.

@unittest.skipUnless('STREAMS_REST_URL' in os.environ and 'STREAMS_USERNAME' in os.environ and 'STREAMS_PASSWORD' in os.environ , "requires Streams REST API setup")
class TestCancelJob(unittest.TestCase):
    def _submit_job(self):
        topo = Topology()
        topo.source(['Hello', 'World']).print()
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.SSL_VERIFY] = False
        sr = submit('DISTRIBUTED', topo, cfg)
        return sr.job

    def _run_canceljob(self, args):
        args.insert(0, '--disable-ssl-verify')
        args.insert(1, 'canceljob')
        streamtool.main(args=args)

    def _check_job_cancelled(self, job):
        for _ in range(100):
            try:
                job.refresh()
                if job.status == 'canceling':
                    return
                time.sleep(0.2)
            except requests.exceptions.HTTPError as e:
                # Just incase we miss the state change.
                if e.response.status_code == 404:
                    return
                raise
        self.fail('Job not canceled: ' + job.status)

    def test_cancel(self):
        job = self._submit_job()
        self._run_canceljob(args=['--jobs', str(job.id)])

    def test_cancel_user(self):
        job = self._submit_job()
        user = os.environ['STREAMS_USERNAME']
        with unittest.mock.patch.dict(os.environ, {'STREAMS_USERNAME':''}):
            self._run_canceljob(args=['--jobs', str(job.id), '--User', user])
        self._check_job_cancelled(job)

    def test_cancel_multiple(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        job3 = self._submit_job()
        self._run_canceljob(args=['--jobs', str(job1.id)+','+str(job2.id)+','+str(job3.id)])
        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)
        self._check_job_cancelled(job3)
