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

# Tests streamtool submitjob script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(cpd_setup(), "requires Streams REST API setup")
class Testupdateoperator(unittest.TestCase):
    def _submitjob(self, sab, job_config):
        args = ["--disable-ssl-verify", "submitjob", sab, "-g", job_config]
        rc, job = streamtool.run_cmd(args=args)
        return rc, job

    def _update_operators(self, jobID, job_config):
        args = ["--disable-ssl-verify", "updateoperators", jobID, job_config]
        rc, val = streamtool.run_cmd(args=args)
        return rc, val

    def setUp(self):
        self.jobs_to_cancel = []
        self.files_to_remove = []

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)

        for file in self.files_to_remove:
            if os.path.exists(file):
                os.remove(file)

    def test_update_operator(self):
        sab_path = (my_path / "temp/limits.Main.sab").resolve()
        initial_config = (my_path / "temp/jco-singleOperatorResourceSpec.json").resolve()
        new_config = (my_path / "temp/jco-Link_2-width3.json").resolve()

        print(str(sab_path))

        # Submit job, assert no problems
        rc, job = self._submitjob(sab=str(sab_path), job_config=str(initial_config))
        self.jobs_to_cancel.extend([job])
        self.assertEqual(rc, 0)

        # file_name = str(job.name) + '_' + str(job.id) + '_config.json'
        # newRC, val = self._update_operators(job.id, str(new_config))
        # self.assertEqual(rc, 0)

        # self.assertTrue(os.path.exists(file_name))

