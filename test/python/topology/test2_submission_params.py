# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
from enum import IntEnum
import datetime
import decimal

import test_vers

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.context import JobConfig
import streamsx.spl.op as op
from streamsx.spl.types import Timestamp

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestSubmissionParams(unittest.TestCase):
    """ Test  submission params.
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def test_spl(self):
        """
        Test passing as an SPL parameter.
        """
        N=22
        t = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topic = 'topology/test/python/' + t
       
        topo = Topology()
        spTopic = topo.create_submission_parameter('mytopic')

        sch = StreamSchema('tuple<uint64 seq, rstring s>')
        b = op.Source(topo, "spl.utility::Beacon", sch,
            params = {'initDelay': 10.0, 'period': 0.02, 'iterations':N})
        b.seq = b.output('IterationCount()')
     
        p = op.Sink("com.ibm.streamsx.topology.topic::Publish", b.stream,
            params={'topic': topic})

        s = op.Source(topo, "com.ibm.streamsx.topology.topic::Subscribe", sch,
            params = {'streamType': sch, 'topic': spTopic})

        jc = JobConfig()
        jc.submission_parameters['mytopic'] = topic
        jc.add(self.test_config)

        tester = Tester(topo)
        tester.tuple_count(s.stream, N)
        tester.test(self.test_ctxtype, self.test_config)
