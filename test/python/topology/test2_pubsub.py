# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
from streamsx.topology.schema import StreamSchema as StSc
from streamsx.topology.schema import CommonSchema as CmnSc
import streamsx.spl.op as op

import uuid


@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestPubSub(unittest.TestCase):
    """ Test publish/subscribe
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def _check_topics(self):
        ins = self.tester.streams_connection.get_instance(self.tester.submission_result['instanceId'])
        topics = ins.get_published_topics()
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
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02})
        s.seq = s.output('IterationCount()')

        s.stream.publish(topic=self.topic_spl)

        s = s.stream.map(lambda x : x)
        s.publish(topic=self.topic_python)

        # Publish twice to ensure its only listed once
        s = s.filter(lambda x : True)
        s.publish(topic=self.topic_python)

        self.tester = Tester(topo)
        self.tester.local_check = self._check_topics
        self.tester.tuple_count(s, 100, exact=False)
        self.tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestBluemixPubSub(TestPubSub):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
