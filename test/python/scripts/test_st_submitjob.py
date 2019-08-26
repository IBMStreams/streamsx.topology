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


from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams, JobConfig
from streamsx.rest import Instance
import streamsx.scripts.streamtool as streamtool
import streamsx.ec


from contextlib import contextmanager
from io import StringIO

# Tests streamtool submitjob script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(
    "CP4D_URL" in os.environ
    and "STREAMS_INSTANCE_ID" in os.environ
    and "STREAMS_USERNAME" in os.environ
    and "STREAMS_PASSWORD" in os.environ,
    "requires Streams REST API setup",
)
class TestSubmitJob(unittest.TestCase):
    def _submitjob(self, args, sab=None):
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "submitjob")
        if sab:
            args.insert(2, sab)
        else:
            topo = Topology()
            topo.source([1])
            cfg = {}
            cfg[ConfigParams.SSL_VERIFY] = False
            src = submit("BUNDLE", topo, cfg)
            sab_path = src["bundlePath"]
            args.insert(2, sab_path)
            self.files_to_remove.append(sab_path)
        rc, val = streamtool.run_cmd(args=args)
        return rc, val

    def setUp(self):
        self.instance = os.environ["STREAMS_INSTANCE_ID"]
        self.username = os.environ["STREAMS_USERNAME"]
        self.stringLength = 10
        self.jobs_to_cancel = []
        self.files_to_remove = []
        self.name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)

        self.files_to_remove.extend(glob.glob("./test_st_submitjob.*.json"))

        for file in self.files_to_remove:
            if os.path.exists(file):
                os.remove(file)

    # Check --jobname option
    def test_submitjob_name(self):
        rc, job = self._submitjob(args=["--jobname", self.name])

        self.jobs_to_cancel.extend([job])
        self.assertEqual(rc, 0)
        self.assertEqual(job.name, self.name)

    # Check --jobgroup option
    def test_submitjob_group(self):

        # JobGroup name doesn't exist, error should be printed to stderr
        output, error, rc = self.get_output(
            lambda: self._submitjob(args=["--jobgroup", str(self.name)])
        )

        error = error.splitlines()

        if not any(
            "500 Server Error: Internal Server Error for url" in s for s in error
        ):
            self.fail("jobgroup doesn't already exist, should throw 500 error")

        # Check no jobGroup specified results in default jobGroup
        rc, job = self._submitjob([])

        self.jobs_to_cancel.extend([job])
        jobgroup = job.jobGroup.split("/")[-1]

        self.assertEqual(rc, 0)
        self.assertEqual(jobgroup, "default")

    # Check --jobConfig option
    def test_submitjob_config(self):
        jc = JobConfig(job_name=self.name)
        my_file = "jobconfig.json"
        with open(my_file, "w") as f:
            json.dump(jc.as_overlays(), f)

        rc, job = self._submitjob(args=["--jobConfig", my_file])

        self.jobs_to_cancel.extend([job])
        self.files_to_remove.append(my_file)

        self.assertEqual(rc, 0)
        self.assertEqual(job.name, self.name)

    # Check --outfile option
    def test_submitjob_outfile(self):
        my_file = self.name + ".txt"

        rc, job = self._submitjob(args=["--outfile", my_file])

        self.jobs_to_cancel.extend([job])
        self.files_to_remove.append(my_file)

        self.assertEqual(rc, 0)

        with open(my_file, "r") as f:
            job_ids = [line.rstrip() for line in f if not line.isspace()]
            self.assertEqual(job_ids[0], job.id)

    # Check -P option w/ simple key1=value1 submission parameters
    def test_submitjob_submission_parameters_simple(self):
        operator1 = "test1"
        operator2 = "test2"

        topo = Topology()
        lower = topo.create_submission_parameter("key1")
        upper = topo.create_submission_parameter("key2")

        s = topo.source([1])
        s.for_each(Test_metrics(lower), name=operator1)
        s.for_each(Test_metrics(upper), name=operator2)

        cfg = {}
        cfg[ConfigParams.SSL_VERIFY] = False
        src = submit("BUNDLE", topo, cfg)
        sab_path = src["bundlePath"]

        # Submit the job
        args = ["--jobname", str(self.name), "-P", "key1=val1", "-P", "key2=val2"]
        rc, my_job = self._submitjob(args, sab=sab_path)
        self.files_to_remove.append(sab_path)
        self.jobs_to_cancel.extend([my_job])

        test1 = my_job.get_operators(operator1)[0]
        test2 = my_job.get_operators(operator2)[0]
        m1, m2 = None, None
        for _ in range(100):
            if m1 and m2:
                break
            time.sleep(1)
            m1 = test1.get_metrics("val1")
            m2 = test2.get_metrics("val2")

        self.assertEqual(rc, 0)

        if not (m1 and m2):
            self.fail("Submission parameters failed to be created")

    # Check -P option w/ randomly generated key/value submission parameters
    def test_submitjob_submission_parameters_complex(self):
        paramList1, paramList2 = self.generateRandom()
        operator1 = "test1"
        operator2 = "test2"

        topo = Topology()
        lower = topo.create_submission_parameter(paramList1[0][0])
        upper = topo.create_submission_parameter(paramList1[1][0])

        s = topo.source([1])
        s.for_each(Test_metrics(lower), name=operator1)
        s.for_each(Test_metrics(upper), name=operator2)

        cfg = {}
        cfg[ConfigParams.SSL_VERIFY] = False
        src = submit("BUNDLE", topo, cfg)
        sab_path = src["bundlePath"]

        # Submit the job
        args = ["--jobname", str(self.name)]
        for prop in paramList2:
            args.extend(["-P", prop])
        rc, my_job = self._submitjob(args, sab=sab_path)
        self.files_to_remove.append(sab_path)
        self.jobs_to_cancel.extend([my_job])

        test1 = my_job.get_operators(operator1)[0]
        test2 = my_job.get_operators(operator2)[0]
        m1, m2 = None, None
        for _ in range(100):
            if m1 and m2:
                break
            time.sleep(1)
            m1 = test1.get_metrics(paramList1[0][1])
            m2 = test2.get_metrics(paramList1[1][1])

        self.assertEqual(rc, 0)

        if not (m1 and m2):
            self.fail("Submission parameters failed to be created")

    def get_output(self, my_function):
        """ Helper function that gets the ouput from executing my_function

        Arguments:
            my_function {} -- The function to be executed

        Returns:
            stdout [String] -- Output of my_function
            stderr [String] -- Errors and exceptions from executing my_function
            rc [int] -- 0 indicates succces, 1 indicates error or failure
        """
        rc = None
        with captured_output() as (out, err):
            rc, val = my_function()
        stdout = out.getvalue().strip()
        stderr = err.getvalue().strip()
        return stdout, stderr, rc

    def generateRandom(self, num=2):
        """ Helper function that generates random key-value pairs of the form <KEY>=<VALUE> and returns them in a list

        Returns:
            (propList1, propList2) [Tuple] -- A tuple containing 2 lists, the first is of form [(<KEY>, <VALUE>)...],
            second list is of form [ <KEY>=<VALUE>, ..... ]
        """
        propList1 = []
        propList2 = []
        for _ in range(num):
            key = "KEY_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            value = "VALUE_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            propList2.append(key + "=" + value)
            propList1.append((key, value))
        return (propList1, propList2)


@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class Test_metrics(object):
    def __init__(self, val):
        self.val = val

    def __enter__(self):
        self.m1 = streamsx.ec.CustomMetric(self, name=self.val(), initialValue=37)

    def __exit__(self, a, b, c):
        pass

    def __call__(self, tuple_):
        return
