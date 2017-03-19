# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
from __future__ import print_function
import unittest
import os
import json
import sys

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.context import submit
from streamsx.topology.context import ConfigParams

def build_simple_app(name):
    topo = Topology(name)
    hw = topo.source(["Bluemix", "Streaming", "Analytics"])
    hw.print()
    return topo


@unittest.skipIf(sys.version_info.major == 2, "Streaming Analytics service requires 3.5")
class TestStreamingAnalytics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.vcap_services = os.environ.pop('VCAP_SERVICES', None)
        cls.service_name = os.environ.pop('STREAMING_ANALYTICS_SERVICE_NAME', None)
    @classmethod
    def tearDownClass(cls):
        if cls.vcap_services is not None:
            os.environ['VCAP_SERVICES'] = cls.vcap_services
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

    def test_submit_job_results(self):
        vsi = self.require_vcap()
        topo = build_simple_app("test_submit_job_results")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap_file']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        rc = self.submit_to_service(topo, cfg)
        self.assertIn("artifact", rc, "\"artifact\" field not in returned json dict")
        self.assertIn("jobId", rc, "\"jobId\" field not in returned json dict")
        self.assertIn("application", rc, "\"application\" field not in returned json dict")
        self.assertIn("name", rc, "\"name\" field not in returned json dict")
        self.assertIn("state", rc, "\"state\" field not in returned json dict")
        self.assertIn("plan", rc, "\"plan\" field not in returned json dict")
        self.assertIn("enabled", rc, "\"enabled\" field not in returned json dict")
        self.assertIn("status", rc, "\"status\" field not in returned json dict")
        self.assertIn("instanceId", rc, '"instanceId" field not in returned json dict')

