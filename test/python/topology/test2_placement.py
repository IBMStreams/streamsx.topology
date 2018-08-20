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
from streamsx.topology.context import JobConfig
from streamsx import rest
from streamsx.rest_primitives import _IAMConstants
from streamsx.spl.op import Source

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


class TestPlacement(unittest.TestCase):
    _multiprocess_can_split_ = True

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

    def check_colocations(self):
        job = self.tester.submission_result.job

        s1_op = job.get_operators(name='.*S1')[0]
        s2_op = job.get_operators(name='.*S2')[0]

        s1f_op = job.get_operators(name='.*S1F')[0]
        s2f_op = job.get_operators(name='.*S2F')[0]

        s1e_op = job.get_operators(name='.*S1E')[0]
        s2e_op = job.get_operators(name='.*S2E')[0]

        self.assertNotEqual(s1_op.get_pe().id, s2_op.get_pe().id)
        self.assertEqual(s1_op.get_pe().id, s2e_op.get_pe().id)

        self.assertEqual(s2f_op.get_pe().id, s2_op.get_pe().id)
        self.assertEqual(s2f_op.get_pe().id, s1f_op.get_pe().id)
        self.assertEqual(s1f_op.get_pe().id, s1e_op.get_pe().id)

        beacon_op = job.get_operators(name='.*BeaconColo')[0]
        self.assertEqual(beacon_op.get_pe().id, s2_op.get_pe().id)

        s3_op = job.get_operators(name='.*S3')[0]
        s3f_op = job.get_operators(name='.*S3F')[0]
        s3e_op = job.get_operators(name='.*S3E')[0]

        self.assertEqual(s3_op.get_pe().id, s3e_op.get_pe().id)
        self.assertNotEqual(s3_op.get_pe().id, s3f_op.get_pe().id)

        self.assertEqual(s3_op.get_pe().id, s1_op.get_pe().id)

    def test_colocation(self):
        topo = Topology()

        s1 = topo.source([], name='S1')
        s2 = topo.source([], name='S2')

        s1f = s1.filter(lambda x : True, name='S1F')
        s2f = s2.filter(lambda x : True, name='S2F')

        s1e = s1f.for_each(lambda x : None, name='S1E')
        s2e = s2f.for_each(lambda x : None, name='S2E')

        # S1 -> S1F -> S1E
        # S2 -> S2F -> S2E

        s2e.colocate(s1)
        # S1(X) -> S1F -> S1E
        # S2 -> S2F -> S2E(X)

        s2f.colocate([s2,s1f])
        # S1(X) -> S1F(Y) -> S1E
        # S2(Y) -> S2F(Y) -> S2E(X)

        s1f.colocate(s1e)
        # S1(X) -> S1F(Y) -> S1E(Y)
        # S2(Y) -> S2F(Y) -> S2E(X)

        beacon = Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':100},
            name = 'BeaconColo')
        beacon.seq = beacon.output('IterationCount()')
        beacon.colocate(s2)

        # Now a colocation independent of the other sets
        s3 = topo.source([], name='S3')
        s3f = s3.filter(lambda x : True, name='S3F')
        s3e = s3f.for_each(lambda x : None, name='S3E')

        s3.colocate(s3e)

        # and then join to an existing set
        #print("MERGE!!!")
        s3.colocate(s1)
        
        self.tester = Tester(topo)
        self.tester.local_check = self.check_colocations

        jc = JobConfig()
        jc.raw_overlay = {'deploymentConfig': {'fusionScheme':'legacy'}}
        jc.add(self.test_config)
     
        sr = self.tester.test(self.test_ctxtype, self.test_config)
