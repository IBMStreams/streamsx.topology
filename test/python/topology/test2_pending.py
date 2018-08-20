# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
from __future__ import print_function
import unittest
import sys
import itertools
import time
import os

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op


class TestPending(unittest.TestCase):
    _multiprocess_can_split_ = True

    """ Test pending connections.
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def test_simple_map(self):
        """Test pending connection simple case.
        """
        data = ['A1','B1', 'A2', 'A3', 'C1', 'C1']
        expected =  [ e + "PC" for e in data ]
        topo = Topology()
        pending = PendingStream(topo)
        ap = pending.stream.map(lambda s : s + "PC")
        self.assertFalse(pending.is_complete())
        pending.complete(topo.source(data))
        self.assertTrue(pending.is_complete())

        tester = Tester(topo)
        tester.contents(ap, expected)
        tester.test(self.test_ctxtype, self.test_config)

    def test_simple_filter(self):
        """Test pending connection simple case.
        """
        data = ['A1','B1', 'A2', 'A3', 'C1', 'C1']
        expected =  ['A3']
        topo = Topology()
        pending = PendingStream(topo)
        ap = pending.stream.filter(lambda s : s.startswith('A'))
        ap = ap.filter(lambda s : s.endswith('3'))
        self.assertFalse(pending.is_complete())
        pending.complete(topo.source(data))
        self.assertTrue(pending.is_complete())

        tester = Tester(topo)
        tester.contents(ap, expected)
        tester.test(self.test_ctxtype, self.test_config)

    def test_fan_in_out(self):
        """Test pending connection fan in/out.
        """
        data1 = ['A1','B1', 'A2', 'A3', 'C1', 'C1']
        data2 = ['X','Y', 'Z', 'Q', 'T', 'X']

        all_data = data1 + data2
        expected_pc =  [ e + "PC" for e in all_data ]
        expected_cp =  [ "CP" + e for e in all_data ]
        expected_su =  [ "SU" + e + "US" for e in all_data ]
        topo = Topology()
        pending = PendingStream(topo)
        apc = pending.stream.map(lambda s : s + "PC")
        acp = pending.stream.map(lambda s : 'CP' + s)
        self.assertFalse(pending.is_complete())

        s1 = topo.source(data1)
        s2 = topo.source(data2)

        su = s1.union({s2})

        asu = su.map(lambda s : 'SU' + s + 'US')

        pending.complete(su)
        self.assertTrue(pending.is_complete())

        tester = Tester(topo)
        tester.contents(apc, expected_pc, ordered=False)
        tester.contents(acp, expected_cp, ordered=False)
        tester.contents(asu, expected_su, ordered=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_feedback_loop(self):
        topo = Topology()

        data = ['A','B', 'A', 'A', 'X', 'C', 'C', 'D', 'A', 'A', 'E']
        expected = ['B', 'X', 'C', 'C', 'D', 'A', 'A', 'E']
        s = topo.source(data)
        s = s.filter(lambda t : time.sleep(1) or True).as_string();
        feedback = PendingStream(topo)
        
        df = op.Invoke(topo, 'spl.utility::DynamicFilter',
            inputs = [s, feedback.stream],
            schemas= [schema.CommonSchema.String])

        df.params['key'] = df.attribute(s, 'string')
        df.params['addKey'] = df.attribute(feedback.stream, 'string')

        delayed_out = op.Map('spl.utility::Delay', df.outputs[0], params={'delay': 0.05}).stream

        x = delayed_out.filter(lambda s : s == 'X').map(lambda s : 'A').as_string()
        i = topo.source(['B', 'X', 'C', 'D', 'E']).as_string()
        x = x.union({i})
        feedback.complete(x)

        result = delayed_out
        result.print()

        #streamsx.topology.context.submit('TOOLKIT', topo)

        tester = Tester(topo)
        tester.contents(result, expected)
        tester.test(self.test_ctxtype, self.test_config)

class TestPendingCloud(TestPending):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

class TestPendingCompileOnly(unittest.TestCase):

    @unittest.skipIf("STREAMS_INSTALL" not in os.environ, "STREAMS_INSTALL not set")
    def test_pure_loop(self):
        topo = Topology()

        feedback = PendingStream(topo)
        
        df = op.Map('spl.utility::Custom',
            feedback.stream,
            schema=schema.CommonSchema.String)

        delayed_out = op.Map('spl.utility::Delay', df.stream, params={'delay': 0.05}).stream

        feedback.complete(delayed_out)

        sr = streamsx.topology.context.submit('BUNDLE', topo)
        self.assertEqual(0, sr['return_code'])
        os.remove(sr.bundlePath)

