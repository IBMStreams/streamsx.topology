# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
from streamsx.topology.schema import StreamSchema as StSc
from streamsx.topology.schema import CommonSchema as CmnSc
import streamsx.spl.op as op
import streamsx.types

import uuid


class TestPubSub(unittest.TestCase):
    """ Test publish/subscribe
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_distributed(self)

    def _check_topics(self):
        ins = self.tester.streams_connection.get_instance(self.tester.submission_result['instanceId'])
        # topics are created dynamically so give some time
        # for them to be created
        for _ in range(10):
            topics = ins.get_published_topics()
            if topics:
                break
            time.sleep(2)

        # Don't assume this is the only app running so
        # other topics may exist
        sts = 0
        stp = 0
        for pt in topics:
            if self.topic_spl == pt.topic:
                sts += 1
                if pt.schema is not None:
                    self.assertEqual(StSc('tuple<uint64 seq>'), pt.schema)
            elif self.topic_python == pt.topic:
                if pt.schema is not None:
                    self.assertEqual(CmnSc.Python.value, pt.schema)
                stp += 1

        self.assertEqual(1, sts)
        self.assertEqual(1, stp)

    def test_published_topics(self):
        """Test a published stream is available through get_published_topics.
        """
        tspl = ''.join(random.choice('0123456789abcdef') for x in range(20))
        tpy = ''.join(random.choice('0123456789abcdef') for x in range(20))

        self.topic_spl = 'topology/test/python/' + tspl
        self.topic_python = 'topology/test/python/' + tpy
        self.assertNotEqual(self.topic_spl, self.topic_python)

        topo = Topology()
        beacon = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02})
        beacon.seq = beacon.output('IterationCount()')

        beacon.stream.publish(topic=self.topic_spl)

        s = beacon.stream.map(lambda x : x)
        s.publish(topic=self.topic_python)

        # Publish twice to ensure its only listed once
        s = s.filter(lambda x : True)
        s.publish(topic=self.topic_python)

        self.tester = Tester(topo)
        self.tester.local_check = self._check_topics
        self.tester.tuple_count(s, 100, exact=False)
        self.tester.test(self.test_ctxtype, self.test_config)

    def _check_buffer(self):
        job = self.tester.submission_result.job
        seen_sub = False
        seen_qs = False
        for op in job.get_operators():
            if op.operatorKind == 'spl.relational::Filter':
                seen_sub = True
                ip = op.get_input_ports()[0]
                for m in ip.get_metrics():
                    if m.name == 'queueSize':
                        seen_qs = True
                        self.assertEqual(self._buf_size, m.value)

        self.assertTrue(seen_sub)
        self.assertTrue(seen_qs)

    def test_subscribe_direct_explicit(self):
        topic = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topo = Topology()
        s = topo.subscribe(topic=topic, connect=SubscribeConnection.Direct)
        self._buf_size = 0

        self.tester = Tester(topo)
        self.tester.local_check = self._check_buffer
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_subscribe_buffered(self):
        topic = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topo = Topology()
        s = topo.subscribe(topic=topic, connect=SubscribeConnection.Buffered)
        self._buf_size = 1000

        self.tester = Tester(topo)
        self.tester.local_check = self._check_buffer
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_subscribe_buffered_capacity(self):
        topic = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topo = Topology()
        N = 789
        s = topo.subscribe(topic=topic, connect=SubscribeConnection.Buffered, buffer_capacity=N)
        self._buf_size = N

        self.tester = Tester(topo)
        self.tester.local_check = self._check_buffer
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_subscribe_buffered_drop(self):
        topic = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topo = Topology()
        N = 563
        s = topo.subscribe(topic=topic, connect=SubscribeConnection.Buffered, buffer_capacity=N, buffer_full_policy = streamsx.types.CongestionPolicy.DropLast)
        self._buf_size = N

        self.tester = Tester(topo)
        self.tester.local_check = self._check_buffer
        self.tester.test(self.test_ctxtype, self.test_config)

class TestBluemixPubSub(TestPubSub):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
