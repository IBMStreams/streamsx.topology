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


from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams, JobConfig
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
        self.name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        # self.instance = Instance.of_endpoint(username= self.username, verify=False)

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)
        if os.path.exists("jobconfig.json"):
            os.remove("jobconfig.json")
        outfile = self.name + '.txt'
        if os.path.exists(outfile):
            os.remove(outfile)

    # Check --jobname option
    def test_submitjob_name(self):
        job = self._submitjob(args=['--jobname', self.name])

        self.jobs_to_cancel.extend([job])

        self.assertEquals(job.name, self.name)

    # Check --jobgroup option
    def test_submitjob_group(self):
        with self.assertRaises(Exception):
            self._submitjob(args=['--jobgroup', str(self.name)])

        jobgroup = "default"
        job = self._submitjob(args=['--jobgroup', str(jobgroup)])

        self.jobs_to_cancel.extend([job])
        self.assertEquals(job.jobGroup, jobgroup)


    # Check --jobConfig option
    def test_submitjob_config(self):
        jc = JobConfig(job_name=self.name)
        my_file = 'jobconfig.json'
        with open(my_file, 'w') as f:
            json.dump(jc.as_overlays(), f)

        job = self._submitjob(args=['--jobConfig', my_file])

        self.jobs_to_cancel.extend([job])

        self.assertEquals(job.name, self.name)

    # Check --outfile option
    def test_submitjob_outfile(self):
        my_file = self.name + '.txt'

        job = self._submitjob(args=['--outfile', my_file])

        self.jobs_to_cancel.extend([job])

        with open(my_file, 'r') as f:
            job_ids = [line.rstrip() for line in f if not line.isspace()]
            self.assertEquals(job_ids[0], job.id)



    def test_submitjob_submission_parameters(self):
        key_value_pairs = self.generateRandom()
        # my_args = []
        # i = 3
        # for pair in key_value_pairs:
        #     my_args.insert(i, "--P")
        #     i += 1
        #     my_args.insert(i, pair)
        #     i += 1
        # job = self._submitjob(args=my_args)

        # self.jobs_to_cancel.extend([job])

        # topo = Topology()
        # lower = topo.create_submission_parameter('lower')
        # upper = topo.create_submission_parameter('upper')
        # s = topo.source([1])
        # s.for_each(MyTestClass())

        # src = submit('BUNDLE', topo)
        #submitjob mysab -P A X -P B Y




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

class EcDuplicateMetric(object):
    def __enter__(self):
        self.m1 = streamsx.ec.CustomMetric(self, name='METRIC1', initialValue=37)
        if int(self.m1.value) != 37:
            raise ValueError("Expected initial 37 got " + int(self.m1.value))
        self.m1 = streamsx.ec.CustomMetric(self, name='METRIC1', initialValue=99)
        if int(self.m1.value) != 37:
            raise ValueError("Expected 37 got " + int(self.m1.value))

        try:
            streamsx.ec.CustomMetric(self, name='METRIC1', kind='Gauge')
            # 4.3 allows metrics of the same name regardless of kind.
            self.okerror = True
        except ValueError as e:
            self.okerror = True

    def __exit__(self, a, b, c):
        pass

    def __call__(self, tuple_):
        return tuple_ + (self.m1.name,self.okerror)
