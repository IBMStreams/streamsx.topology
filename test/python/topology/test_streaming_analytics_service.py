# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
from __future__ import print_function
import unittest
import os
import json
import sys
import tempfile

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.context import submit
from streamsx.topology.context import ConfigParams

def build_simple_app(name):
    topo = Topology(name)
    hw = topo.source(["Bluemix", "Streaming", "Analytics"])
    hw.print()
    return topo


@unittest.skipIf(not (sys.version_info.major == 3 and sys.version_info.minor == 5), "Streaming Analytics service requires 3.5")
class TestStreamingAnalytics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.delete_file = None
        cls.vcap_services = os.environ.pop('VCAP_SERVICES', None)
        if cls.vcap_services and not cls.vcap_services.startswith('/'):
            # VCAP_SERVICES is JSON, not a file, create a temp file
            fd, tp = tempfile.mkstemp(suffix='.json', prefix='vcap', text=True)
            os.write(fd, cls.vcap_services.encode('utf-8'))
            os.close(fd)
            cls.vcap_services = tp
            cls.delete_file = tp

        cls.service_name = os.environ.pop('STREAMING_ANALYTICS_SERVICE_NAME', None)
    @classmethod
    def tearDownClass(cls):
        if cls.vcap_services is not None:
            os.environ['VCAP_SERVICES'] = cls.vcap_services
            if cls.delete_file:
                os.remove(cls.delete_file)
        if cls.service_name is not None:
            os.environ['STREAMING_ANALYTICS_SERVICE_NAME'] = cls.service_name

    @classmethod
    def require_vcap(cls):
        if cls.vcap_services is None:
            raise unittest.SkipTest("No VCAP SERVICES env var")
        if cls.service_name is None:
            raise unittest.SkipTest("No service name provided: env var STREAMING_ANALYTICS_SERVICE_NAME")

        fn = cls.vcap_services
        with open(fn) as vcap_json_data:
            vs = json.load(vcap_json_data)
        sn = cls.service_name

        cls.is_v2 = False
        for creds in vs['streaming-analytics']:
            if creds['name'] == sn:
                if 'v2_rest_url' in creds['credentials']:
                    cls.is_v2 = True            

        return {'vcap': vs, 'service_name': sn, 'vcap_file': fn}

    def submit_to_service(self, topo, cfg):
        rc = submit("ANALYTICS_SERVICE", topo, cfg)
        self.assertEqual(0, rc['return_code'])
        rc.job.cancel()
        return rc

    def test_no_vcap(self):
        topo = build_simple_app("test_no_vcap")
        self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo)

    def test_no_vcap_cfg(self):
        topo = build_simple_app("test_no_vcap_cfg")
        self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo, {})

    def test_no_service(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_no_service")
        cfg = {}
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
        self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo, cfg)

    def test_vcap_json(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_vcap_json")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = False
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        self.submit_to_service(topo, cfg)

    def test_vcap_json_remote(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_vcap_json_remote")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        self.submit_to_service(topo, cfg)

    def test_vcap_string_remote(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_vcap_string_remote")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = json.dumps(vsi['vcap'])
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        self.submit_to_service(topo, cfg)

    def test_vcap_file_remote(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_vcap_file_remote")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap_file']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        self.submit_to_service(topo, cfg)

    def test_service_def(self):
        """ Test a submit using a service definition."""
        vsi = self.require_vcap()
        topo = build_simple_app("test_service_def")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        services = vsi['vcap']['streaming-analytics']
        creds = {}
        for s in services:
            if s['name'] == vsi['service_name']:
               creds = s['credentials']
            
        service = {'type':'streaming-analytics', 'name':vsi['service_name']}
        service['credentials'] = creds
        cfg[ConfigParams.SERVICE_DEFINITION] = service
        self.submit_to_service(topo, cfg)

    def test_service_creds(self):
        """ Test a submit using service credentials."""
        vsi = self.require_vcap()
        topo = build_simple_app("test_service_creds")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        services = vsi['vcap']['streaming-analytics']
        creds = {}
        for s in services:
            if s['name'] == vsi['service_name']:
               creds = s['credentials']
            
        cfg[ConfigParams.SERVICE_DEFINITION] = creds
        self.submit_to_service(topo, cfg)

    def test_submit_job_results(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_submit_job_results")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap_file']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        rc = self.submit_to_service(topo, cfg)

        self.assertIn("jobId", rc, "\"jobId\" field not in returned json dict")
        self.assertIn("application", rc, "\"application\" field not in returned json dict")
        self.assertIn("name", rc, "\"name\" field not in returned json dict")

        if not self.is_v2:
            self.assertIn("artifact", rc, "\"artifact\" field not in returned json dict")
            self.assertIn("state", rc, "\"state\" field not in returned json dict")
            self.assertIn("plan", rc, "\"plan\" field not in returned json dict")
            self.assertIn("enabled", rc, "\"enabled\" field not in returned json dict")
            self.assertIn("status", rc, "\"status\" field not in returned json dict")
            self.assertIn("instanceId", rc, '"instanceId" field not in returned json dict')
        
        else:
            self.assertIn("streams_self", rc, "\"streams_self\" field not in returned json dict")            
            self.assertIn("health", rc, "\"health\" field not in returned json dict")
            self.assertIn("self", rc, "\"self\" field not in returned json dict")
