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

    def test_ResourceTags(self):
        # Host tags might not be present when using a v2 service
        #if self.is_v2:
        #    return unittest.skip('Host tags might not be present when using a v2 service')

        topo = Topology()
        h1 = topo.source(host_name)
        h1.resource_tags.add('host1')
        
        h2 = topo.source(host_name)
        h2.resource_tags.add('host2')
        
        h = h1.union({h2})
        h.print()
        h = h.map(RemoveDup())
        
        tester = Tester(topo)
        tester.tuple_count(h, 2)
     
        sr = tester.test(self.test_ctxtype, self.test_config)
