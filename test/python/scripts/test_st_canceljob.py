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
    "ICPD_URL" in os.environ
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

    def _check_job_running(self, job):
        try:
            job.refresh()
            if job.status == "running":
                return
            time.sleep(0.2)
        except requests.exceptions.HTTPError as e:
            # Just incase we miss the state change.
            if e.response.status_code == 404:
                return
            raise
        self.fail("Job canceled: " + job.status)

    def test_cancel(self):
        job = self._submit_job()
        self._run_canceljob(args=["--jobs", str(job.id)])

    def test_cancel_user(self):
        job = self._submit_job()
        user = os.environ["STREAMS_USERNAME"]
        with unittest.mock.patch.dict(os.environ, {"STREAMS_USERNAME": ""}):
            self._run_canceljob(args=["--jobs", str(job.id), "--User", user])
        self._check_job_cancelled(job)

    # Check that canceljob fails when given no arguments
    def test_cancel_simple(self):
        with self.assertRaises(SystemExit):
            self._run_canceljob(args=[])

    # Check succesfully cancels jobs
    def test_cancel_simple_1(self):
        job1 = self._submit_job()
        self.jobs_to_cancel.extend([job1])

        self._run_canceljob(
            args=[str(job1.id) + ' ']
        )
        self._check_job_cancelled(job1)

    # Check succesfully cancels jobs seperated by ','
    def test_cancel_simple_2(self):
        job1 = self._submit_job()
        job2 = self._submit_job()

        self._run_canceljob(
            args=[str(job1.id) + ',' + str(job2.id)]
        )
        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)

    # Check succesfully cancels jobs seperated by ' '
    def test_cancel_simple_3(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        self.jobs_to_cancel.extend([job1])

        self._run_canceljob(
            args=[str(job1.id) + ' ' + str(job2.id)]
        )
        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)

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
            self._run_canceljob(args=["--jobs", str("123"), "--jobnames", str("jobName")])

    # Check that you can't use --jobs, --jobnames and --file optional args at the same time
    def test_cancel_multiple_mix2(self):
        with self.assertRaises(SystemExit):
            self._run_canceljob(
            args=[
                "--jobs",
                str("123"),
                "--jobnames",
                str("jobName"),
                "--file",
                str("test_st_canceljob_tempfile.txt"),
            ]
            )

    # Check succesfully cancels jobs via --jobnames arg
    def test_cancel_multiple_2(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        job3 = self._submit_job()

        self.jobs_to_cancel.extend([job1, job2, job3])

        output, error, rc = self.get_output(
            lambda: self._run_canceljob(
                args=[
                    "--jobnames",
                    str(job1.name) + "," + str(job2.name) + "," + str(job3.name),
                ]
            )
        )

        output = output.splitlines()

        self.assertEqual(output[0], self.get_canceljob_output_message(job1.id, 1))
        self.assertEqual(output[1], self.get_canceljob_output_message(job2.id, 1))
        self.assertEqual(output[2], self.get_canceljob_output_message(job3.id, 1))

        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)
        self._check_job_cancelled(job3)

        self.assertEqual(rc, 0)

    # Check able to canceljob via jobsIDS from file
    def test_cancel_multiple_from_file(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        job3 = self._submit_job()

        self.write_file([job1.id, job2.id, job3.id])

        self.jobs_to_cancel.extend([job1, job2, job3])

        output, error, rc = self.get_output(
            lambda: self._run_canceljob(
                args=["--file", str("test_st_canceljob_tempfile.txt")]
            )
        )

        output = output.splitlines()

        self.assertEqual(output[0], self.get_canceljob_output_message(job1.id, 1))
        self.assertEqual(output[1], self.get_canceljob_output_message(job2.id, 1))
        self.assertEqual(output[2], self.get_canceljob_output_message(job3.id, 1))

        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)
        self._check_job_cancelled(job3)

        self.assertEqual(rc, 0)

    # Check raises error when given non integer jobID
    def test_cancel_nonexistant(self):
        job1 = self._submit_job()
        job2 = self._submit_job()

        self.jobs_to_cancel.extend([job1, job2])

        # Check invalid/non-numeric ID followed by valid ID - Should print error message regarding invalid one, then do nothing (ie valid one still running)
        output, error, rc = self.get_output(
            lambda: self._run_canceljob(
                args=["--jobs", str("FAKE_JOB_ID") + "," + str(job1.id)]
            )
        )

        # self.assertEqual(error[-1], self.get_canceljob_output_message("FAKE_JOB_ID", 5))
        self.assertEqual(rc, 1)
        self._check_job_running(job1)

        # Check validID followed by invalid/non-numeric ID - Should cancel valid one w/ success message, then print error message regarding invalid one
        output, error, rc = self.get_output(
            lambda: self._run_canceljob(
                args=["--jobs", str(job1.id) + "," + str("FAKE_JOB_ID")]
            )
        )

        output = output.splitlines()

        self.assertEqual(output[0], self.get_canceljob_output_message(job1.id, 1))
        # self.assertEqual(error[-1], self.get_canceljob_output_message("FAKE_JOB_ID", 5))
        self.assertEqual(rc, 1)
        self._check_job_cancelled(job1)

        # Try cancelling invalid/numeric ID followed by valid id - Should print 2 error messages regarding invalid, then cancel valid one and print success message
        output, error, rc = self.get_output(
            lambda: self._run_canceljob(
                args=["--jobs", str("123456") + "," + str(job2.id)]
            )
        )

        output = output.splitlines()

        # self.assertEqual(error[-2], self.get_canceljob_output_message("123456", 3))
        # self.assertEqual(error[-1], self.get_canceljob_output_message("123456", 4))
        self.assertEqual(output[0], self.get_canceljob_output_message(job2.id, 1))
        self.assertEqual(rc, 1)
        self._check_job_cancelled(job2)

    # Check invalid jobname error message, then check valid jobname cancellation message
    def test_cancel_nonexistant2(self):
        job1 = self._submit_job()

        self.jobs_to_cancel.extend([job1])

        output, error, rc = self.get_output(
            lambda: self._run_canceljob(
                args=["--jobnames", str("FAKE_JOB_NAME") + "," + str(job1.name)]
            )
        )

        output = output.splitlines()

        # Should give error regarding invalid jobname, then cancel valid jobname
        # self.assertEqual(
        #     error[0], self.get_canceljob_output_message("FAKE_JOB_NAME", 2)
        # )
        self.assertEqual(output[0], self.get_canceljob_output_message(job1.id, 1))

        self.assertEqual(rc, 1)
        self._check_job_cancelled(job1)

    # if --collectlogs given, make sure it generates the logs
    def test_cancel_collectlogs(self):
        job1 = self._submit_job()
        job2 = self._submit_job()
        job3 = self._submit_job()

        self.jobs_to_cancel.extend([job1, job2, job3])

        self._run_canceljob(
            args=[
                "--jobs",
                str(job1.id) + "," + str(job2.id) + "," + str(job3.id),
                "--collectlogs",
            ]
        )
        self._check_job_cancelled(job1)
        self._check_job_cancelled(job2)
        self._check_job_cancelled(job3)

        # Check log files were generated for each job
        checkFiles = [job1, job2, job3]
        for job in [job1, job2, job3]:
            for file in os.listdir("."):
                if os.path.isfile(file) and file.startswith("job_" + str(job.id)):
                    if os.path.exists(str(file)):
                        os.remove(file)
                        checkFiles.remove(job)
        self.assertEqual(checkFiles, [])

    def setUp(self):
        self.instance = os.environ["STREAMS_INSTANCE_ID"]
        self.username = os.environ["STREAMS_USERNAME"]
        self.jobs_to_cancel = []

    def tearDown(self):
        for job in self.jobs_to_cancel:
            job.cancel(force=True)
        if os.path.exists("test_st_canceljob_tempfile.txt"):
            os.remove("test_st_canceljob_tempfile.txt")

    ###########################################
    # Helper functions
    ###########################################

    def write_file(self, jobIDs):
        """Create a file in the current directory w/ the name test_st_canceljob_tempfile.txt, containing jobIDs, with each jobID on a newline.
        File should be deleted by teardown method

        Arguments:
            jobIDs {List} -- List containg JobIDs 
        """
        with open("test_st_canceljob_tempfile.txt", "w") as f:
            for jobID in jobIDs:
                f.write("%s\n" % jobID)

    def get_canceljob_output_message(self, job_data, returnMessage):
        """Helper function to retrieve the correct return message for a given canceljob command

        Arguments:
            job_data {String} -- Either job.id or job.name
            returnMessage {1} -- The number representing the desired/correct output for the given canceljob command

        Returns:
            [String] -- the message
        """
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
            5: "The following job identifier is not valid: {}. Specify a job identifier that is numeric and try the request again.".format(
                job_data
            ),
        }
        return switch[returnMessage]

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
        stdout = out.getvalue().strip()
        stderr = err.getvalue().strip()
        return stdout, stderr, rc
