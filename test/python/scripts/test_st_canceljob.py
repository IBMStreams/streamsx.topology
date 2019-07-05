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

from contextlib import contextmanager
from io import StringIO

# Tests streamtool canceljob script.
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
class TestCancelJob(unittest.TestCase):
    def _submit_job(self):
        topo = Topology()
        topo.source(["Hello", "World"]).print()
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.SSL_VERIFY] = False
        sr = submit("DISTRIBUTED", topo, cfg)
        return sr.job

    def _run_canceljob(self, args):
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "canceljob")
        return streamtool.main(args=args)

    def _check_job_cancelled(self, job):
        for _ in range(100):
            try:
                job.refresh()
                if job.status == "canceling":
                    return
                time.sleep(0.2)
            except requests.exceptions.HTTPError as e:
                # Just incase we miss the state change.
                if e.response.status_code == 404:
                    return
                raise
        self.fail("Job not canceled: " + job.status)

    def test_cancel(self):
        job = self._submit_job()
        self._run_canceljob(args=["--jobs", str(job.id)])

    def test_cancel_user(self):
        job = self._submit_job()
        user = os.environ["STREAMS_USERNAME"]
        with unittest.mock.patch.dict(os.environ, {"STREAMS_USERNAME": ""}):
            self._run_canceljob(args=["--jobs", str(job.id), "--User", user])
        self._check_job_cancelled(job)

    def test_cancel_multiple(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        job3 = self._submit_job()
        self._run_canceljob(
            args=["--jobs", str(job1.id) + "," + str(job2.id) + "," + str(job3.id)]
        )
        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)
        self._check_job_cancelled(job3)

    # Check that you can't use --jobs and --jobnames optional args at the same time
    def test_cancel_multiple_mix(self):
        with self.assertRaises(SystemExit):
            self.cancel_multiple_mix()

    def cancel_multiple_mix(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        self.jobs_to_cancel.extend([job1, job2])

        self._run_canceljob(args=["--jobs", str(job1.id), "--jobnames", str(job2.name)])

    # Check that you can't use --jobs, --jobnames and --file optional args at the same time
    def test_cancel_multiple_mix2(self):
        with self.assertRaises(SystemExit):
            self.cancel_multiple_mix2()

    def cancel_multiple_mix2(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        job3 = self._submit_job()

        self.write_file([job3.id])
        self.jobs_to_cancel.extend([job1, job2, job3])

        self._run_canceljob(
            args=[
                "--jobs",
                str(job1.id),
                "--jobnames",
                str(job2.name),
                "--file",
                str("test_st_canceljob_tempfile.txt"),
            ]
        )

    def write_file(self, jobIDs):
        with open("test_st_canceljob_tempfile.txt", "w") as f:
            for jobID in jobIDs:
                f.write("%s\n" % jobID)

    def get_canceljob_output_message(self, job_data, returnMessage):
        switch = {
            1: "The following job ID was canceled: {}. The job was in the {} instance.".format(
                job_data, self.instance
            ),
            2: "The following job name is not found: {}. Specify a job name that is valid and try the request again".format(
                job_data
            ),
            3: "The following job ID was not found {}".format(job_data),
            4: "The following job ID cannot be canceled: {}. See the previous error message".format(
                job_data
            ),
        }
        return switch[returnMessage]

    def setUp(self):
        self.instance = os.environ["STREAMS_INSTANCE_ID"]
        self.stringLength = 10
        self.username = os.environ["STREAMS_USERNAME"]
        self.jobs_to_cancel = []

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)
        if os.path.exists("test_st_canceljob_tempfile.txt"):
            os.remove("test_st_canceljob_tempfile.txt")

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
