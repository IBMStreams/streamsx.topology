# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import os
import json

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.context import submit
from streamsx.topology.context import ConfigParams

def check_setup(test):
    test.assertNotIn('VCAP_SERVICES', os.environ)

def require_vcap(test):
    check_setup(test)
    if 'topology_test_vcap_services' not in os.environ:
        raise unittest.SkipTest("No VCAP file provided: env var topology_test_vcap_services")
    if 'topology_test_vcap_service_name' not in os.environ:
        raise unittest.SkipTest("No service name provided: env var topology_test_vcap_service_name")

    
    fn = os.environ['topology_test_vcap_services']
    with open(fn) as vcap_json_data:
        vs = json.load(vcap_json_data)
    sn = os.environ['topology_test_vcap_service_name']
    return {'vcap': vs, 'service_name': sn, 'vcap_file': fn}

def build_simple_app(name):
    topo = Topology(name)
    hw = topo.source(["Bluemix", "Streaming", "Analytics"])
    hw.print()
    return topo

def submit_to_service(test, topo, cfg):
    rc = submit("ANALYTICS_SERVICE", topo, cfg)
    test.assertEqual(0, rc)

class TestStreamingAnalytics(unittest.TestCase):

  def test_no_vcap(self):
    check_setup(self)
    topo = build_simple_app("test_no_vcap")
    self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo)

  def test_no_vcap_cfg(self):
    check_setup(self)
    topo = build_simple_app("test_no_vcap_cfg")
    self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo, {})

  def test_no_service(self):
    vsi = require_vcap(self)
    topo = build_simple_app("test_no_service")
    cfg = {}
    cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
    self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo, cfg)

  def test_vcap_json(self):
    vsi = require_vcap(self)
    topo = build_simple_app("test_vcap_json")
    cfg = {}
    cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
    cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
    submit_to_service(self, topo, cfg)

  def test_vcap_json_remote(self):
    vsi = require_vcap(self)
    topo = build_simple_app("test_vcap_json_remote")
    cfg = {}
    cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
    cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
    cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
    submit_to_service(self, topo, cfg)

  def test_vcap_string_remote(self):
    vsi = require_vcap(self)
    topo = build_simple_app("test_vcap_string_remote")
    cfg = {}
    cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
    cfg[ConfigParams.VCAP_SERVICES] = json.dumps(vsi['vcap'])
    cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
    submit_to_service(self, topo, cfg)

  def test_vcap_file_remote(self):
    vsi = require_vcap(self)
    topo = build_simple_app("test_vcap_file_remote")
    cfg = {}
    cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
    cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap_file']
    cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
    submit_to_service(self, topo, cfg)
