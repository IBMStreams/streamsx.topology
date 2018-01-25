import unittest
import test_vers
from common_tests import CommonTests, logger
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams
from streamsx.rest import StreamingAnalyticsConnection
from streamsx.rest_primitives import _IAMConstants

instance_response_keys = [
    "auto_stop",
    "plan",
    "state",
    "id",
    "status",
    "maximum",
    "crn",
    "size",
    "documentation",
    "streams_self",
    "enabled",
    "job_count",
    "jobs",
    "streams_console",
    "minimum",
    "self"
]


@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestRestFeaturesBluemix(CommonTests):
    @classmethod
    def setUpClass(cls):
        cls.logger = logger

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.sc = StreamingAnalyticsConnection()
        self.test_config[ConfigParams.STREAMS_CONNECTION]=self.sc

        self.is_v2 = False
        if _IAMConstants.V2_REST_URL in self.sc.credentials:
            self.is_v2 = True

    # The underscore in front of this test causes it to be skipped by default
    # This is to prevent the starting and stopping of the instance from 
    # interfering with other tests.
    # The test can be run manually: 
    # python -m unittest test_rest_bluemix.TestRestFeaturesBluemix._test_service_stop_start
    def _test_service_stop_start(self):
        self.logger.debug("Beginning test: test_service_stop_start")
        sas = self.sc.get_streaming_analytics()

        status = sas.get_instance_status()
        self.valid_response(status)        
        self.assertEqual('running', status['status'])

        res = sas.stop_instance()
        self.valid_response(res)
        status = sas.get_instance_status()
        self.assertEqual('stopped', status['status'])
        
        res = sas.start_instance()
        self.valid_response(res)
        status = sas.get_instance_status()
        self.assertEqual('running', status['status'])
        
    def valid_response(self, res):
        for key in instance_response_keys:
            self.assertTrue(key in res)
