from __future__ import print_function
import json
import logging
import subprocess
import time
import unittest
from test_operators import DelayedTupleSourceWithLastTuple

from streamsx import rest
from streamsx.topology import topology, schema

credentials_file_name = 'sws_credentials.json'
vcap_service_config_file_name = 'vcap_service_config.json'

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

    def test_ensure_correct_rest_module(self):
        self.logger.debug("Beginning test: test_ensure_correct_rest_module.")
        # Ensure that the rest module being tested is from streamsx.utility
        self.assertTrue('streamsx.utility' in rest.__file__)

    def test_username_and_password(self):
        self.logger.debug("Beginning test: test_username_and_password.")
        # Ensure, at minimum, that the StreamsContext can connect and retrieve valid data from the SWS resources path
        ctxt = rest.StreamsConnection(self.sws_username, self.sws_password, self.sws_rest_api_url)
        resources = ctxt.get_resources()
        self.logger.debug("Number of retrieved resources is: " + str(len(resources)))
        self.assertGreater(len(resources), 0, msg="Returned zero resources from the \"resources\" endpoint.")

    def test_basic_view_support(self):
        self.logger.debug("Beginning test: test_basic_view_support.")
        top = topology.Topology('basicViewTest')
        # Send only one tuple
        stream = top.source(DelayedTupleSourceWithLastTuple(['hello'], 20))
        view = stream.view()

        # Temporary workaround for Bluemix TLS issue with views
        stream.publish(schema=schema.CommonSchema.String, topic="__test_topic::test_basic_view_support")

        self.logger.debug("Begging compilation and submission of basic_view_support topology.")

        self._submit(top)

        time.sleep(5)
        queue = view.start_data_fetch()

        try:
            view_tuple_value = queue.get(block=True, timeout=20.0)
        except:
            logger.exception("Timed out while waiting for tuple.")
            raise
        finally:
            view.stop_data_fetch()
        self.logger.debug("Returned view value in basic_view_support is " + view_tuple_value)
        self.assertTrue(view_tuple_value.startswith('hello'))