import unittest
import test_vers
from common_tests import CommonTests, logger
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams
from streamsx.rest import StreamingAnalyticsConnection
from streamsx.rest_primitives import IAMConstants

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
        if IAMConstants.V2_REST_URL in self.sc.credentials:
            self.is_v2 = True
