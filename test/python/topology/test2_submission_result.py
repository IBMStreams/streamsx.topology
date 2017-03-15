import unittest
from streamsx.topology.tester import Tester
from streamsx.topology.topology import Topology
from streamsx.topology.context import ConfigParams
from streamsx import rest

import test_vers

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestSubmissionResult(unittest.TestCase):
    def setUp(self):
        Tester.setup_distributed(self)

    def _correct_job_ids(self):
        # Test that result.job exists and you can pull values from it.
        json_job_id = self.tester.submission_result.jobId
        job_job_id = self.tester.submission_result.job.id
        self.assertEqual(json_job_id, job_job_id)

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
