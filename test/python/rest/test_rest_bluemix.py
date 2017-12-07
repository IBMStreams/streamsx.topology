import unittest
import test_vers
from common_tests import CommonTests, logger
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams
from streamsx.rest import StreamingAnalyticsConnection
from streamsx.rest_primitives import _IAMConstants

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

    def test_service_stop_start(self):
        self.logger.debug("Beginning test: test_service_stop_start")
        sas = self.sc.get_streaming_analytics()

        status = sas.get_instance_status()
        self.assertEqual('running', status['status'])

        sas.stop_instance()
        status = sas.get_instance_status()
        self.assertEqual('stopped', status['status'])
        
        sas.start_instance()
        status = sas.get_instance_status()
        self.assertEqual('running', status['status'])
