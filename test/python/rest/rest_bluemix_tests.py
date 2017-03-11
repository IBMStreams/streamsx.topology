from common_tests import logger
from common_tests import vcap_service_config_file_name
from common_tests import CommonTests
from streamsx import rest
from streamsx.topology import context
import json

class TestRestFeaturesBluemix(CommonTests):
    @classmethod
    def setUpClass(cls):
        """
        Initialize the logger and get the SWS username, password, and REST URL.
        :return: None
        """
        cls.logger = logger
        cls._submission_context = context.ContextTypes.ANALYTICS_SERVICE

        # Get credentials from creds file.
        vcap_service_config_file = open(vcap_service_config_file_name, mode='r')
        try:
            vcap_service_config = json.loads(vcap_service_config_file.read())
        except:
            cls.logger.exception("Error while reading and parsing " + vcap_service_config_file_name)
            raise

        vs = rest.VcapUtils.get_vcap_services(vcap_service_config)
        credentials = rest.VcapUtils.get_credentials(vcap_service_config, vs)
        rest_api_url = rest.VcapUtils.get_rest_api_url_from_creds(credentials)

        cls.vcap_service_config = vcap_service_config
        cls.sws_username = credentials['userid']
        cls.sws_password = credentials['password']
        cls.sws_rest_api_url = rest_api_url


    def _submit(self, topology):
        return context.submit(self._submission_context, topology, config=self.vcap_service_config)