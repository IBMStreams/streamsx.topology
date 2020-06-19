# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import unittest.mock
import sys
import os
import time
import requests
import uuid
import json
import glob
from pathlib import Path


from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams, JobConfig
from streamsx.rest_primitives import Instance
import streamsx.scripts.streamtool as streamtool
import streamsx.ec


from contextlib import contextmanager
from io import StringIO

my_path = Path(__file__).parent

def cpd_setup():
    env = os.environ
    return (("CP4D_URL" in env and "STREAMS_INSTANCE_ID" in env) or \
         ('STREAMS_BUILD_URL' in env and 'STREAMS_REST_URL')) \
         and \
         "STREAMS_USERNAME" in os.environ and \
         "STREAMS_PASSWORD" in os.environ

# Tests streamtool updateoperators script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(cpd_setup(), "requires Streams REST API setup")
class Testupdateoperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create the job
        topo = Topology()
        s = topo.source([1])
        p = s.parallel(3, name='test1')
        p = p.filter(lambda x : x > 0)
        e = p.end_parallel()

        cfg = {}
        cfg[ConfigParams.SSL_VERIFY] = False
        src = submit("BUNDLE", topo, cfg)

        cls.sab_path = str(src['bundlePath'])
        if cpd_setup():
            cls.initial_config = str(src['jobConfigPath'])

            cls.files_to_remove = [src['bundlePath'], src['jobConfigPath']]
            cls.new_parallelRegionWidth = 'test*=2'

    @classmethod
    def tearDownClass(cls):
        if cpd_setup():
            for file in cls.files_to_remove:
                if os.path.exists(file):
                    os.remove(file)

    def _submitjob(self, sab, job_config):
        args = ["--disable-ssl-verify", "submitjob", sab, '-g', job_config]
        rc, job = streamtool.run_cmd(args=args)
        return rc, job

    def _update_operators(self, job_config=None, jobID=None, job_name=None, parallelRegionWidth=None, force=None):
        args = ["--disable-ssl-verify", "updateoperators"]
        if jobID:
            args.append(jobID)
        elif job_name:
            args.extend(['--jobname', job_name])
        args.extend(['-g', job_config])
        if parallelRegionWidth:
            args.extend(["--parallelRegionWidth", parallelRegionWidth])
        if force:
            args.append('--force')
        rc, val = streamtool.run_cmd(args=args)
        return rc, val

    def setUp(self):
        rc, self.job = self._submitjob(sab=self.sab_path, job_config=self.initial_config)
        self.assertEqual(rc, 0)
        self.jobs_to_cancel = [self.job]

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)

    # Check blank JCO causes error out
    def test_blank_config(self):
        new_config = str((my_path / "updateoperators_test_files/blank_config.json").resolve())
        newRC, val = self._update_operators(job_config = new_config, jobID=self.job.id)
        self.assertEqual(newRC, 1)

    # Check no JCO w/ parallelRegionWidth arg (no force arg) fails bc PE's need to be stopped first
    def test_no_config_with_arg(self):
        newRC, val = self._update_operators(jobID=self.job.id, parallelRegionWidth=self.new_parallelRegionWidth)
        self.assertEqual(newRC, 1)

    # Check parallelRegionWidth arg w/ force arg works as expected
    def test_no_config_with_arg_and_force(self):
        newRC, val = self._update_operators(jobID=self.job.id, parallelRegionWidth=self.new_parallelRegionWidth, force=True)
        self.assertEqual(newRC, 0)
        self.check_update_ops(self.job, 2)

    # Check updateoperators works as expected on valid config
    def test_valid_config(self):
        new_config = str((my_path / "updateoperators_test_files/new_config.json").resolve())
        newRC, val = self._update_operators(job_config = new_config, jobID=self.job.id)
        self.assertEqual(newRC, 0)
        self.check_update_ops(self.job, 3)

    # Check --jobname arg
    def test_jobname(self):
        new_config = str((my_path / "updateoperators_test_files/new_config.json").resolve())
        newRC, val = self._update_operators(job_config = new_config, job_name=self.job.name)
        self.assertEqual(newRC, 0)
        self.check_update_ops(self.job, 3)

    # Check parallelRegionWidth arg overrides arg in JCO
    def test_parallelRegionWidth(self):
        new_config = str((my_path / "updateoperators_test_files/new_config.json").resolve())
        newRC, val = self._update_operators(job_config = new_config, jobID=self.job.id, parallelRegionWidth=self.new_parallelRegionWidth)
        self.assertEqual(newRC, 0)
        self.check_update_ops(self.job, 2)

    def check_update_ops(self, job, new_width, regionName='test*'):
        """ Checks whether the job operators has been updated to the correct new width

        Arguments:
            job {Job}
            new_width {int}
        """
        channel = []
        for op in job.get_operators(name=regionName):
            channel.append(op.channel)

        if len(channel) != new_width:
            self.fail('Update operation failed')
