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
from streamsx.rest import Instance
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
    def _submitjob(self, sab, job_config):
        args = ["--disable-ssl-verify", "submitjob", sab, "-g", job_config]
        rc, job = streamtool.run_cmd(args=args)
        return rc, job
    
    def _update_operators(self, job_config, jobID=None, job_name=None, parallelRegionWidth=None):
        args = ["--disable-ssl-verify", "updateoperators"]
        if jobID:
            args.append(jobID)
        elif job_name:
            args.extend(['--jobname', job_name])
        args.extend(['-g', job_config])
        if parallelRegionWidth:
            args.extend(["--parallelRegionWidth", parallelRegionWidth])
        rc, val = streamtool.run_cmd(args=args)
        return rc, val

    def setUp(self):
        self.jobs_to_cancel = []
        self.files_to_remove = []
        self.sab_path = str((my_path / "updateoperators_test_files/limits.Main.sab").resolve())
        self.initial_config = str((my_path / "updateoperators_test_files/config1.json").resolve())

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)

        for file in self.files_to_remove:
            if os.path.exists(file):
                os.remove(file)

    # Check blank config errors out
    def test_blank_config(self):
        new_config = str((my_path / "updateoperators_test_files/blank_config.json").resolve())

        # Submit job, assert no problems
        rc, job = self._submitjob(sab=str(self.sab_path), job_config=str(self.initial_config))
        self.jobs_to_cancel.extend([job])
        self.assertEqual(rc, 0)

        # updateoperators
        newRC, val = self._update_operators(job_config = new_config, jobID=job.id)
        self.assertEqual(newRC, 1)


    # Check updateoperators works as expected on valid config
    def test_valid_config(self):
        new_config = str((my_path / "updateoperators_test_files/config2.json").resolve())

        # Submit job, assert no problems
        rc, job = self._submitjob(sab=self.sab_path, job_config=self.initial_config)
        self.jobs_to_cancel.extend([job])
        self.assertEqual(rc, 0)

        # updateoperators
        newRC, val = self._update_operators(job_config = new_config, jobID=job.id)
        self.assertEqual(newRC, 0)

    # Check --jobname arg
    def test_jobname(self):
        new_config = str((my_path / "updateoperators_test_files/config2.json").resolve())

        # Submit job, assert no problems
        rc, job = self._submitjob(sab=self.sab_path, job_config=self.initial_config)
        self.jobs_to_cancel.extend([job])
        self.assertEqual(rc, 0)

        # updateoperators
        newRC, val = self._update_operators(job_config = new_config, job_name=job.name)
        self.assertEqual(newRC, 0)

    # Check parallelRegionWidth arg
    def test_parallelRegionWidth(self):
        new_config = str((my_path / "updateoperators_test_files/config2.json").resolve())

        # Submit job, assert no problems
        rc, job = self._submitjob(sab=self.sab_path, job_config=self.initial_config)
        self.jobs_to_cancel.extend([job])
        self.assertEqual(rc, 0)

        # updateoperators
        new_parallelRegionWidth = 'Link*=2'
        newRC, val = self._update_operators(job_config = new_config, jobID=job.id, parallelRegionWidth=new_parallelRegionWidth)
        self.assertEqual(newRC, 0)

