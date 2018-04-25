import os
import time
import unittest
import test_vers
from common_tests import CommonTests, logger
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams
from streamsx.rest import StreamingAnalyticsConnection
from streamsx.rest_primitives import _IAMConstants
from streamsx.topology.topology import Topology
import streamsx.topology.context
import streamsx.rest

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

    # The underscore in front of this test causes it to be skipped by default
    # This is because the test must run on an os version that matches
    # the service and has a local Streams Install.
    # python3 -m unittest test_rest_bluemix.TestRestFeaturesBluemix._test_submit_sab
    def _test_submit_sab(self):
        topo = Topology('SabTest', namespace='mynamespace')
        s = topo.source([1,2])
        es = s.for_each(lambda x : None)
        bb = streamsx.topology.context.submit('BUNDLE', topo, {})
        self.assertIn('bundlePath', bb)
        self.assertIn('jobConfigPath', bb)

        sas = self.sc.get_streaming_analytics()

        sr = sas.submit_job(bundle=bb['bundlePath'])
        job_id = sr.get('id', sr.get('jobId'))
        self.assertIsNotNone(job_id)
        self.assertIn('name', sr)
        self.assertIn('application', sr)
        self.assertEqual('mynamespace::SabTest', sr['application'])
        cr = sas.cancel_job(job_id=job_id)

        jn = 'SABTEST:' + str(time.time())
        jc = streamsx.topology.context.JobConfig(job_name=jn)
        sr = sas.submit_job(bundle=bb['bundlePath'], job_config=jc)
        job_id = sr.get('id', sr.get('jobId'))
        self.assertIsNotNone(job_id)
        self.assertIn('application', sr)
        self.assertEqual('mynamespace::SabTest', sr['application'])
        self.assertIn('name', sr)
        self.assertEqual(jn, sr['name'])
        cr = sas.cancel_job(job_id=job_id)
       
        os.remove(bb['bundlePath'])
        os.remove(bb['jobConfigPath'])
