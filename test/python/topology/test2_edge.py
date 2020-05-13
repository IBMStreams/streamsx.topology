# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import itertools
import os
import sys

from streamsx.topology.topology import *
from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig


class TestEdge(unittest.TestCase):

    def _is_not_blank(self, s):
        if s is None:
            return false;
        return bool(s and s.strip())

    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_submit_edge(self):
        topo = Topology("test_submit_edge")
        heartbeat = topo.source(lambda : itertools.count())
        heartbeat.print()

        cfg = {
            ConfigParams.SSL_VERIFY: False
        }
        try:
            submission_result = submit(ContextTypes.EDGE, topo.graph, cfg)
            print (str(submission_result))
            self.assertTrue(submission_result is not None)
            self.assertTrue(self._is_not_blank(submission_result.image))
            self.assertTrue(self._is_not_blank(submission_result.imageDigest))
        except RuntimeError as e:
            print(str(e))
            self.skipTest("Skip test, CPD does not support EDGE.")


    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_submit_edge_bundle(self):
        topo = Topology("test_submit_edge_bundle")
        heartbeat = topo.source(lambda : itertools.count())
        heartbeat.print()

        cfg = {
            ConfigParams.SSL_VERIFY: False
        }
        try:
            submission_result = submit(ContextTypes.EDGE_BUNDLE, topo.graph, cfg)
            self.assertTrue(submission_result is not None)
            sab_file = submission_result.bundlePath
            self.assertTrue(os.path.isfile(sab_file))
            if os.path.isfile(sab_file):
                os.remove(sab_file)
            json_file = submission_result.jobConfigPath
            self.assertTrue(os.path.isfile(json_file))
            if os.path.isfile(json_file):
                os.remove(json_file)
            
        except RuntimeError as e:
            print(str(e))
            self.skipTest("Skip test, CPD does not support EDGE.")


