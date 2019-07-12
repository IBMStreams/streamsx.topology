# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import unittest.mock
import sys
import os
import time
import requests
import uuid


from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
from streamsx.rest import Instance
import streamsx.scripts.streamtool as streamtool

from contextlib import contextmanager
from io import StringIO

# Tests streamtool submitjob script.
# Requires environment setup for a ICP4D Streams instance.
@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


@unittest.skipUnless(
    "ICP4D_DEPLOYMENT_URL" in os.environ
    and "STREAMS_INSTANCE_ID" in os.environ
    and "STREAMS_USERNAME" in os.environ
    and "STREAMS_PASSWORD" in os.environ,
    "requires Streams REST API setup",
)
class TestSubmitJob(unittest.TestCase):

    def _submitjob(self, args):
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "submitjob")
        args.insert(2, "/home/streamsadmin/hostdir/streamsx.topology/test/python/scripts/main.sab")
        rc, val = streamtool.run_cmd(args=args)
        return val

    def setUp(self):
        self.instance = os.environ["STREAMS_INSTANCE_ID"]
        self.username = os.environ["STREAMS_USERNAME"]
        self.stringLength = 10
        self.jobs_to_cancel = []
        # self.instance = Instance.of_endpoint(username= self.username, verify=False)

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)

    # Check --jobname option
    def test_submitjob_name(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        job = self._submitjob(args=['--jobname', str(name)])

        self.jobs_to_cancel.extend([job])

        self.assertEquals(job.name, name)

    # Check --jobgroup option
    def test_submitjob_group(self):
        jobgroup = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        job = self._submitjob(args=['--jobgroup', str(jobgroup)])

        self.jobs_to_cancel.extend([job])

        self.assertEquals(job.jobGroup, jobgroup)

    def test_submitjob_submission_parameters(self):
        key_value_pairs = self.generateRandom()
        my_args = []
        i = 3
        for pair in key_value_pairs:
            my_args.insert(i, "--P")
            i += 1
            my_args.insert(i, pair)
            i += 1
        job = self._submitjob(args=my_args)

        self.jobs_to_cancel.extend([job])




    def get_output(self, my_function):
        """ Helper function that gets the ouput from executing my_function
        Arguments:
            my_function {} -- The function to be executed
        Returns:
            Output [String] -- Output of my_function
            Rc [int] -- 0 indicates succces, 1 indicates error or failure
        """
        rc = None
        with captured_output() as (out, err):
            rc = my_function()
        output = out.getvalue().strip()
        return output, rc

    def generateRandom(self):
        """ Helper function that generates random key-value pairs of the form <KEY>=<VALUE> and returns them in a list

        Returns:
            propList [List] -- A list consisting of elements of the form <KEY>=<VALUE> pairs
        """
        propList = []
        for i in range(10):
            key = "KEY_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            value = "VALUE_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            propList.append(key + "=" + value)
        return propList
