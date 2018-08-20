import unittest
from streamsx.topology.tester import Tester
from streamsx.topology.topology import Topology
from streamsx.topology.context import ConfigParams
from streamsx import rest
import os
import fnmatch

class TestSubmissionResult(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_distributed(self)
        self.username = os.getenv("STREAMS_USERNAME", "streamsadmin")
        self.password = os.getenv("STREAMS_PASSWORD", "passw0rd")

    def _correct_job_ids(self):
        # Test that result.job exists and you can pull values from it.
        json_job_id = self.tester.submission_result.jobId
        job_job_id = self.tester.submission_result.job.id
        self.assertEqual(json_job_id, job_job_id)

    def _can_retrieve_logs(self):
        self.can_retrieve_logs = hasattr(self.tester.submission_result.job, 'applicationLogTrace')

    def test_get_job(self):
        topo = Topology("job_in_result_test")
        topo.source(["foo"])

        sc = rest.StreamsConnection(username=self.username, password=self.password)
        sc.session.verify = False
        config = {ConfigParams.STREAMS_CONNECTION : sc}

        tester = Tester(topo)
        self.tester = tester

        tester.local_check = self._correct_job_ids
        tester.test(self.test_ctxtype, config)

    def test_fetch_logs_on_failure(self):
        topo = Topology("fetch_logs_on_failure")
        s = topo.source(["foo"])

        tester = Tester(topo)
        # Causes test to fail
        tester.contents(s, ["bar"])

        try:
            self.tester = tester
            tester.local_check = self._can_retrieve_logs
            tester.test(self.test_ctxtype, self.test_config)
        except AssertionError:
            # This test is expected to fail, do nothing.
            pass

        # Check if logs were downloaded
        if self.can_retrieve_logs:
            logs = tester.result['application_logs']
            exists = os.path.isfile(logs)
            
            self.assertTrue(exists, "Application logs were not downloaded on test failure")
            
            if exists:
                os.remove(logs)

    def test_always_fetch_logs(self):
        topo = Topology("always_fetch_logs")
        s = topo.source(["foo"])

        tester = Tester(topo)
        tester.contents(s, ["foo"])

        self.tester = tester
        tester.local_check = self._can_retrieve_logs
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

        if self.can_retrieve_logs:
            # streams version is >= 4.2.4. Fetching logs is supported.
            # Check if logs were downloaded
            logs = tester.result['application_logs']
            exists = os.path.isfile(logs)

            self.assertTrue(exists, "Application logs were not downloaded on test success")
            
            if exists:
                os.remove(logs)                            


class TestSubmissionResultStreamingAnalytics(TestSubmissionResult):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

    def test_get_job(self):
        topo = Topology("job_in_result_test")
        topo.source(["foo"])

        tester = Tester(topo)
        self.tester = tester

        tester.local_check = self._correct_job_ids
        tester.test(self.test_ctxtype, self.test_config)

        sr = tester.submission_result
        self.assertIn('submitMetrics', sr)
        self.assertIn('console.application.url', sr)
        self.assertIn('console.application.job.url', sr)
        m = sr['submitMetrics']
        self.assertIn('buildArchiveSize', m)
        self.assertIn('buildArchiveUploadTime_ms', m)
        self.assertIn('totalBuildTime_ms', m)
        self.assertIn('jobSubmissionTime_ms', m)

        self.assertTrue(m['buildArchiveSize'] > 0)
        self.assertTrue(m['buildArchiveUploadTime_ms'] > 0)
        self.assertTrue(m['totalBuildTime_ms'] > 0)
        self.assertTrue(m['jobSubmissionTime_ms'] > 0)

