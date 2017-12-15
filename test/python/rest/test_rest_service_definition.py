import unittest
import os
import json
from streamsx.rest import StreamingAnalyticsConnection

class TestServiceDefinition(unittest.TestCase):
    """Tests getting a connection using a service definition
    rather than a vcap, service name pair.
    """
    def setUp(self):
        vcap_file = os.environ.get('VCAP_SERVICES')
        if vcap_file is None:
            raise unittest.SkipTest("No VCAP SERVICES env var")
        
        if os.path.isfile(vcap_file):
            with open(vcap_file) as vcap_json_data:
                self.vcap_services = json.load(vcap_json_data)
        else:
            self.vcap_services = json.loads(vcap_file)
        self.service_name = os.environ.get('STREAMING_ANALYTICS_SERVICE_NAME')
 

    def test_service_def(self):
        """ Test a connection using a service definition."""
        creds = {}
        for s in self.vcap_services['streaming-analytics']:
            if s['name'] == self.service_name:
               creds = s['credentials']

        service = {'type':'streaming-analytics', 'name':self.service_name}
        service['credentials'] = creds
        sasc = StreamingAnalyticsConnection.of_definition(service)
        instances = sasc.get_instances()
        self.assertEqual(1, len(instances))

    def test_credentials(self):
        """ Test a connection using a credentials."""
        creds = {}
        for s in self.vcap_services['streaming-analytics']:
            if s['name'] == self.service_name:
               creds = s['credentials']

        sasc = StreamingAnalyticsConnection.of_definition(creds)
        instances = sasc.get_instances()
        self.assertEqual(1, len(instances))
