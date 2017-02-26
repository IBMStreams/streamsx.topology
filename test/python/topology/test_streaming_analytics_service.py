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

vcap_services = os.environ.get('VCAP_SERVICES', None)
service_name = os.environ.get('STREAMS_SERVICE_NAME', None)

def require_vcap(test):
    if 'VCAP_SERVICES' not in os.environ:
        raise unittest.SkipTest("No VCAP SERVICES env var")
    if 'STREAMS_SERVICE_NAME' not in os.environ:
        raise unittest.SkipTest("No service name provided: env var STREAMS_SERVICE_NAME")

    fn = os.environ['VCAP_SERVICES']
    with open(fn) as vcap_json_data:
        vs = json.load(vcap_json_data)
    sn = os.environ['STREAMS_SERVICE_NAME']
    clear_vcap_env()
    return {'vcap': vs, 'service_name': sn, 'vcap_file': fn}

def clear_vcap_env():
    del os.environ['VCAP_SERVICES']
    del os.environ['STREAMS_SERVICE_NAME']

def restore_vcap_env():
    os.environ['VCAP_SERVICES'] = vcap_services
    os.environ['STREAMS_SERVICE_NAME'] = service_name

def build_simple_app(name):
    topo = Topology(name)
    hw = topo.source(["Bluemix", "Streaming", "Analytics"])
    hw.print()
    return topo

def submit_to_service(test, topo, cfg):
    rc = submit("ANALYTICS_SERVICE", topo, cfg)
    test.assertEqual(0, rc['return_code'])
    return rc

@unittest.skipIf(sys.version_info.major == 2, "Streaming Analytics service requires 3.5")
class TestStreamingAnalytics(unittest.TestCase):

  def test_no_vcap(self):
    clear_vcap_env()
    try:
        topo = build_simple_app("test_no_vcap")
        self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo)
    finally:
        restore_vcap_env()

  def test_no_vcap_cfg(self):
    clear_vcap_env()
    try:
        topo = build_simple_app("test_no_vcap_cfg")
        self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo, {})
    finally:
        restore_vcap_env()

  def test_no_service(self):
    vsi = require_vcap(self)
    try:
        topo = build_simple_app("test_no_service")
        cfg = {}
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
        self.assertRaises(ValueError, submit, "ANALYTICS_SERVICE", topo, cfg)
    finally:
        restore_vcap_env()

  def test_vcap_json(self):
    vsi = require_vcap(self)
    try:
        topo = build_simple_app("test_vcap_json")
        cfg = {}
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        submit_to_service(self, topo, cfg)
    finally:
        restore_vcap_env()

  def test_vcap_json_remote(self):
    vsi = require_vcap(self)
    try:
        topo = build_simple_app("test_vcap_json_remote")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap']
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        submit_to_service(self, topo, cfg)
    finally:
        restore_vcap_env()

  def test_vcap_string_remote(self):
    vsi = require_vcap(self)
    try:
        topo = build_simple_app("test_vcap_string_remote")
        cfg = {}
        cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ConfigParams.VCAP_SERVICES] = json.dumps(vsi['vcap'])
        cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
        submit_to_service(self, topo, cfg)
    finally:
        restore_vcap_env()

  def test_vcap_file_remote(self):
    vsi = require_vcap(self)
    try:
      topo = build_simple_app("test_vcap_file_remote")
      cfg = {}
      cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
      cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap_file']
      cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
      submit_to_service(self, topo, cfg)
    finally:
        restore_vcap_env()

  def test_submit_job_results(self):
    vsi = require_vcap(self)
    try:
      topo = build_simple_app("test_submit_job_results")
      cfg = {}
      cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
      cfg[ConfigParams.VCAP_SERVICES] = vsi['vcap_file']
      cfg[ConfigParams.SERVICE_NAME] = vsi['service_name']
      rc = submit_to_service(self, topo, cfg)
      self.assertIn("artifact", rc, "\"artifact\" field not in returned json dict")
      self.assertIn("jobId", rc, "\"jobId\" field not in returned json dict")
      self.assertIn("application", rc, "\"application\" field not in returned json dict")
      self.assertIn("name", rc, "\"name\" field not in returned json dict")
      self.assertIn("state", rc, "\"state\" field not in returned json dict")
      self.assertIn("plan", rc, "\"plan\" field not in returned json dict")
      self.assertIn("enabled", rc, "\"enabled\" field not in returned json dict")
      self.assertIn("status", rc, "\"status\" field not in returned json dict")
    finally:
        restore_vcap_env()

