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
import streamsx.spl.op as op

import uuid


@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestPubSub(unittest.TestCase):
    """ Test publish/subscribe
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def _check_topics(self):
        pass

    def test_published_topics(self):
        """Test a published stream is available through get_published_topics.
        """
        self.topic_spl = 'topology/test/python/' + str(uuid.uuid4())
        self.topic_python = 'topology/test/python/' + str(uuid.uuid4())
        self.assertNotEqual(self.topic_spl, self.topic_python)

        topo = Topology()
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02})
        s.seq = s.output('IterationCount()')

        s.stream.publish(topic=self.topic_spl)

        s = s.stream.map(lambda x : x)
        s.publish(topic=self.topic_python)

        self.tester = Tester(topo)
        #self.tester.local_check = self._check_topics
        self.tester.tuple_count(s, 300)
        self.tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestBluemixPubSub(TestPubSub):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
