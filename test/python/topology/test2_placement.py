# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *

import unittest
import sys
import itertools
import logging
import socket

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx import rest
from streamsx.rest_primitives import _IAMConstants
from streamsx.spl.op import Source

import test_vers

def host_name():
    return [socket.gethostname()]

class RemoveDup(object):
    def __init__(self):
        self.last = None
    def __call__(self, t):
         if self.last is None:
             self.last = t
             return t
         if t == self.last:
             return None
         return t


@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestPlacement(unittest.TestCase):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        
        sc = rest.StreamingAnalyticsConnection()
        self.is_v2 = _IAMConstants.V2_REST_URL in sc.credentials

    def check_placements(self):
        job = self.tester.submission_result.job

        ops = job.get_operators(name='.*SinkOnHost1')
        self.assertEqual(1, len(ops))
        resource = ops[0].get_host()
        self.assertIn('host1', resource.tags)

        ops = job.get_operators(name='.*BeaconOnHost1')
        self.assertEqual(1, len(ops))
        resource = ops[0].get_host()
        self.assertIn('host2', resource.tags)

    def test_ResourceTags(self):
        # Host tags might not be present when using a v2 service
        if self.is_v2:
            return unittest.expectedFailure(self)

        topo = Topology()
        h1 = topo.source(host_name)
        h1.resource_tags.add('host1')
        
        h2 = topo.source(host_name)
        h2.resource_tags.add('host2')
        
        h = h1.union({h2})
        h.print()
        h = h.map(RemoveDup())

        h2sink = h2.for_each(lambda x : None, name='SinkOnHost1')
        h2sink.resource_tags.add('host1')

        beacon = Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':100},
            name = 'BeaconOnHost1')
        beacon.seq = beacon.output('IterationCount()')
        beacon.resource_tags.add('host2')

        h2sinkNotags = h2.for_each(lambda x : None, name='SinkNoTags')
        self.assertFalse(h2sinkNotags.resource_tags)
        
        self.tester = Tester(topo)
        self.tester.tuple_count(h, 2)
        self.tester.local_check = self.check_placements
     
        sr = self.tester.test(self.test_ctxtype, self.test_config)
