import logging
import unittest
import time
from operators import DelayedTupleSourceWithLastTuple
from requests import exceptions

from streamsx.topology.tester import Tester
from streamsx.topology import topology, schema

from streamsx.rest_primitives import *
import primitives_caller

logger = logging.getLogger('streamsx.test.rest_test')

class CommonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Initialize the logger and get the SWS username, password, and REST URL.
        :return: None
        """
        if cls is CommonTests:
            raise unittest.SkipTest("Skipping base tests.")
        cls.is_v2 = None

    def test_username_and_password(self):
        self.logger.debug("Beginning test: test_username_and_password.")
        # Ensure, at minimum, that the StreamsContext can connect and retrieve valid data from the SWS resources path
        resources = self.sc.get_resources()
        self.logger.debug("Number of retrieved resources is: " + str(len(resources)))
        self.assertGreater(len(resources), 0, msg="Returned zero resources from the \"resources\" endpoint.")

    def test_streamsconnection_samplecode(self):
        self.logger.debug("Beginning test: test_streamsconnection_samplecode.")
        domains = self.sc.get_domains()
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
        top = topology.Topology('basicViewTest')
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
        top = topology.Topology('jobRefreshTest')
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
        self.assertIsInstance(domain, Domain)
        primitives_caller.check_domain(self, domain)

        nops = job.get_operators(name='.*BASIC.')
        self.assertEqual(2, len(nops))

        nops = job.get_operators(name='.*BASICD')
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
