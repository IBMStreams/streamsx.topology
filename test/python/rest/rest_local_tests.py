from __future__ import print_function
import json
import logging
import subprocess
import unittest
from common_tests import CommonTests

from common_tests import logger
from common_tests import vcap_service_config_file_name
from common_tests import credentials_file_name

from streamsx.topology import context

class TestRestFeaturesLocal(CommonTests):
    @classmethod
    def setUpClass(cls):
        """
        Initialize the logger and get the SWS username, password, and REST URL.
        :return: None
        """
        cls.logger = logger
        cls.logger.debug("Performing setUpClass of TestRestFeaturesLocal")
        cls._submission_context = context.ContextTypes.DISTRIBUTED

        # Get credentials from creds file.
        creds_file = open(credentials_file_name, mode='r')
        try:
            creds_json = json.loads(creds_file.read())
            cls.sws_username = creds_json['username']
            cls.sws_password = creds_json['password']
        except:
            cls.logger.exception("Error while reading and parsing " + credentials_file_name)
            raise

        # Get the SWS REST URL
        try:
            cls.sws_rest_api_url = creds_json['rest_api_url']
        except:
            try:
                process = subprocess.Popen(['streamtool', 'geturl', '--api'],
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                cls.sws_rest_api_url = process.stdout.readline().strip().decode('utf-8')
            except:
                cls.logger.exception("Error getting SWS rest api url via streamtool")
                raise

    def _submit(self, topology):
        return context.submit(self._submission_context, topology, username = self.sws_username, password=self.sws_password)
