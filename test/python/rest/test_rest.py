import logging
import json
import unittest
import time
import uuid
from operators import DelayedTupleSourceWithLastTuple
from requests import exceptions

from streamsx.topology.tester import Tester
from streamsx.topology import topology, schema
from streamsx.topology.context import ConfigParams, JobConfig
from streamsx.rest import StreamsConnection

import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.spl.types

from streamsx.rest_primitives import *
import primitives_caller

logger = logging.getLogger('streamsx.test.rest_test')

class TestDistributedRestFeatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Initialize the logger and get the SWS username, password, and REST URL.
        :return: None
        """
        cls.is_v2 = None
        cls.logger = logger

    def setUp(self):
        Tester.setup_distributed(self)
        self.sc = StreamsConnection()
        self.sc.session.verify = False
        self.test_config[ConfigParams.STREAMS_CONNECTION] = self.sc

    def test_username_and_password(self):
        self.logger.debug("Beginning test: test_username_and_password.")
        # Ensure, at minimum, that the StreamsContext can connect and retrieve valid data from the SWS resources path
        resources = self.sc.get_resources()
        self.logger.debug("Number of retrieved resources is: " + str(len(resources)))
        self.assertGreater(len(resources), 0, msg="Returned zero resources from the \"resources\" endpoint.")

    def test_streamsconnection_samplecode(self):
        self.logger.debug("Beginning test: test_streamsconnection_samplecode.")
        domains = self.sc.get_domains()
        if domains is not None:
            self.assertGreater(len(domains), 0, msg="Should have more than 0 domains.")
        instances = self.sc.get_instances()
        self.assertGreater(len(instances), 0, msg="Should have more than 0 instances.")
        jobs_count = 0
        for instance in instances:
            jobs_count += len(instance.get_jobs())

    def _verify_basic_view(self):
        q = self._view.start_data_fetch()

        try:
            view_tuple_value = q.get(block=True, timeout=25.0)
        except:
            logger.exception("Timed out while waiting for tuple.")
            raise

        self._view.stop_data_fetch()
        self.logger.debug("Returned view value in basic_view_support is " + view_tuple_value)
        self.assertTrue(view_tuple_value.startswith('hello'))

    def test_basic_view_support(self):
        self.logger.debug("Beginning test: test_basic_view_support.")
        top = topology.Topology()
        # Send only one tuple
        stream = top.source(DelayedTupleSourceWithLastTuple(['hello'], 20))
        self._view = stream.view(start=True, buffer_time=60)

        # Temporary workaround for Bluemix TLS issue with views
        #stream.publish(schema=schema.CommonSchema.String, topic="__test_topic::test_basic_view_support")

        self.logger.debug("Beginning compilation and submission of basic_view_support topology.")
        tester = Tester(top)
        tester.local_check = self._verify_basic_view
        tester.test(self.test_ctxtype, self.test_config)

    def _verify_job_refresh(self):
        result = self.tester.submission_result
        self.job = self.sc.get_instance(result['instanceId']).get_job(result['jobId'])

        self.assertEqual('healthy', self.job.health)

    def test_job_refresh(self):
        top = topology.Topology()
        src = top.source(['Hello'])

        self.tester = Tester(top)
        self.tester.tuple_count(src, 1)
        self.tester.local_check = self._verify_job_refresh
        self.tester.test(self.test_ctxtype, self.test_config)

        # Job was cancelled by test wait for health to change
        timeout = 10
        while hasattr(self.job, 'health') and 'healthy' == self.job.health:
            time.sleep(0.2)
            timeout -= 1
            try:
                self.job.refresh()
            except exceptions.HTTPError:
                self.job = None
                break
            self.assertGreaterEqual(timeout, 0, msg='Timeout exceeded while waiting for job to cancel')

        if hasattr(self.job, 'health'):
            self.assertNotEqual('healthy', self.job.health)

    def _call_rest_apis(self):
        job = self.tester.submission_result.job
        self.assertIsInstance(job, Job)
        primitives_caller.check_job(self, job)

        instance = job.get_instance()
        self.assertIsInstance(instance, Instance)
        primitives_caller.check_instance(self, instance)

        domain = instance.get_domain()
        if domain is not None:
            self.assertIsInstance(domain, Domain)
            primitives_caller.check_domain(self, domain)

        nops = job.get_operators(name='.*BASIC.$')
        self.assertEqual(2, len(nops))

        nops = job.get_operators(name='.*BASICD$')
        self.assertEqual(1, len(nops))
        self.assertTrue(nops[0].name.endswith('BASICD'))
        

    def test_basic_calls(self):
        """
        Test the basic rest apis.
        """
        top = topology.Topology()
        src = top.source(['Rest', 'tester'])
        src = src.filter(lambda x : True, name='BASICC')
        src.view()
        src = src.map(lambda x : x, name='BASICD')

        self.tester = Tester(top)
        self.tester.tuple_count(src, 2)
        self.tester.local_check = self._call_rest_apis
        self.tester.test(self.test_ctxtype, self.test_config)

    # Underscore as the local evironment must match the remote environment
    # such as OS version and architecture type.
    def _test_instance_submit(self):
        """ Test submitting a bundle from an Instance.
        Tests all four mechanisms.
        """
        sab_name = 'ISJ_'+uuid.uuid4().hex
        topo = topology.Topology(sab_name, namespace='myinstancens')
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':100})
        s.seq = s.output('IterationCount()')
        f = op.Map('spl.relational::Filter', s.stream,
            params = {'filter': op.Expression.expression('seq % 2ul == 0ul')})

        bb = streamsx.topology.context.submit('BUNDLE', topo, {})
        self.assertIn('bundlePath', bb)
        self.assertIn('jobConfigPath', bb)

        sc = self.sc
        instances = sc.get_instances()
        if len(instances) == 1:
             instance = instances[0]
        else:
             instance = sc.get_instance(os.environ['STREAMS_INSTANCE_ID'])

        job = instance.submit_job(bb['bundlePath'])
        self.assertIsInstance(job, Job)
        self.assertEqual('myinstancens::'+sab_name, job.applicationName)
        job.cancel()

        with open(bb['jobConfigPath']) as fp:
             jc = JobConfig.from_overlays(json.load(fp))
        jn = 'JN_'+uuid.uuid4().hex
        jc.job_name = jn
        job = instance.submit_job(bb['bundlePath'], jc)
        self.assertIsInstance(job, Job)
        self.assertEqual('myinstancens::'+sab_name, job.applicationName)
        self.assertEqual(jn, job.name)
        job.cancel()

        ab = instance.upload_bundle(bb['bundlePath'])
        self.assertIsInstance(ab, ApplicationBundle)

        job = ab.submit_job()
        self.assertIsInstance(job, Job)
        self.assertEqual('myinstancens::'+sab_name, job.applicationName)
        job.cancel()

        jn = 'JN_'+uuid.uuid4().hex
        jc.job_name = jn
        job = ab.submit_job(jc)
        self.assertIsInstance(job, Job)
        self.assertEqual('myinstancens::'+sab_name, job.applicationName)
        self.assertEqual(jn, job.name)
        job.cancel()

        os.remove(bb['bundlePath'])
        os.remove(bb['jobConfigPath'])


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

from streamsx.rest_primitives import _IAMConstants
from streamsx.rest import StreamingAnalyticsConnection


class TestSasRestFeatures(TestDistributedRestFeatures):

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.sc = StreamingAnalyticsConnection()
        self.test_config[ConfigParams.STREAMS_CONNECTION]=self.sc

        self.is_v2 = False
        if _IAMConstants.V2_REST_URL in self.sc.session.auth._credentials:
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
        sab_name = 'Sab_'+uuid.uuid4().hex
        topo = topology.Topology(sab_name, namespace='mynamespace')
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
        self.assertEqual('mynamespace::' + sab_name, sr['application'])
        cr = sas.cancel_job(job_id=job_id)

        jn = 'SABTEST:' + uuid.uuid4().hex
        jc = streamsx.topology.context.JobConfig(job_name=jn)
        sr = sas.submit_job(bundle=bb['bundlePath'], job_config=jc)
        job_id = sr.get('id', sr.get('jobId'))
        self.assertIsNotNone(job_id)
        self.assertIn('application', sr)
        self.assertEqual('mynamespace::'+sab_name, sr['application'])
        self.assertIn('name', sr)
        self.assertEqual(jn, sr['name'])
        cr = sas.cancel_job(job_id=job_id)
       
        os.remove(bb['bundlePath'])
        os.remove(bb['jobConfigPath'])

class TestDistributedRestEnv(unittest.TestCase):
    def setUp(self):
        self._si = None
        Tester.setup_distributed(self)
        if not 'STREAMS_REST_URL' in os.environ:
            sc = StreamsConnection()
            self._ru = sc.resource_url
            self._si = os.environ['STREAMS_INSTALL']
            del os.environ['STREAMS_INSTALL']
            os.environ['STREAMS_REST_URL'] = self._ru
        else:
            self._ru = os.environ['STREAMS_REST_URL']
            
    def tearDown(self):
        if self._si:
            del os.environ['STREAMS_REST_URL']
            os.environ['STREAMS_INSTALL'] = self._si

    def test_url_from_env(self):
        if self._si:
            self.assertNotIn('STREAMS_INSTALL', os.environ)
        sc = StreamsConnection()
        sc.session.verify = False
        self.assertEqual(self._ru, sc.resource_url)
