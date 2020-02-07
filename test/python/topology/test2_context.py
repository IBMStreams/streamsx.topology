# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import os
import tempfile
import time

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.context import build


class TestContext(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)
        self._verify = None

    def _check_result(self, bundle, jco, sr, dest=None):
        self.assertTrue(os.path.exists(bundle))
        self.assertTrue(os.path.exists(jco))
        self.assertEqual(bundle, sr['bundlePath'])
        self.assertEqual(jco, sr['jobConfigPath'])

        if dest:
            print('DDDD', dest)
            self.assertEqual(dest, os.path.dirname(bundle))
            self.assertEqual(dest, os.path.dirname(jco))
     

    def _remove_result(self, sr):
        os.remove(sr['bundlePath'])
        os.remove(sr['jobConfigPath'])

    def test_build(self):
        topo = Topology()
        topo.source(['a'])

        bundle, jco, sr = build(topo, verify=self._verify)
        self._check_result(bundle, jco, sr)

        # Ensure we can run the same build multiple times
        b_mt = os.path.getmtime(bundle)
        j_mt = os.path.getmtime(jco)
   
        time.sleep(1)
        bundle, jco, sr = build(topo, verify=self._verify)

        self._check_result(bundle, jco, sr)
        self.assertGreater(os.path.getmtime(bundle), b_mt)
        self.assertGreater(os.path.getmtime(jco), j_mt)
        
        self._remove_result(sr)

    def test_build_with_dest(self):
        topo = Topology()
        topo.source(['a'])

        with tempfile.TemporaryDirectory() as td:

            bundle, jco, sr = build(topo, verify=self._verify, dest=td)
            self._check_result(bundle, jco, sr, dest=td)
            self._remove_result(sr)
  
