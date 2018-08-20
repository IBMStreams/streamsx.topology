# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import context
from streamsx import rest
import streamsx.ec as ec
import streamsx.spl.op as op
import streamsx.spl.types as spltypes
import queue

def rands():
    r = random.Random()
    while True:
       yield r.random()

class TestViews(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_distributed(self)

    def _object_view(self):
        q = self._ov.start_data_fetch()
        seen_items = False
        for i in range(100):
             try:
                  v = q.get(timeout=1.0)
                  self.assertTrue(isinstance(v, self._expected_type))
                  seen_items = True
             except queue.Empty :
                  pass
        self.assertTrue(seen_items)
        self._ov.stop_data_fetch()

    def test_object_view(self):
        """ Test a view of Python objects.
        """
        topo = Topology()
        s = topo.source(rands)
        throttle = op.Map('spl.utility::Throttle', s,
            params = {'rate': 25.0})
        s = throttle.stream
        self._ov = s.view()
        self._expected_type = float
        
        tester = Tester(topo)
        tester.local_check = self._object_view
        tester.tuple_count(s, 1000, exact=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_view(self):
        """ Test a view of strings
        """
        topo = Topology()
        s = topo.source(rands)
        throttle = op.Map('spl.utility::Throttle', s,
            params = {'rate': 25.0})
        s = throttle.stream
        s = s.map(lambda t : "ABC" + str(t))
        s = s.as_string()
        self._ov = s.view()
        self._expected_type = str
        
        tester = Tester(topo)
        tester.local_check = self._object_view
        tester.tuple_count(s, 1000, exact=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_schema_view(self):
        """ Test a view of SPL tuples.
        """
        topo = Topology()
        beacon = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq, rstring fixed>',
            params = {'period': 0.05, 'iterations':1000})
        beacon.seq = beacon.output('IterationCount()')
        beacon.fixed = beacon.output(spltypes.rstring('FixedValue'))

        s = beacon.stream
        self._ov = s.view()
        self._expected_type = dict
        
        tester = Tester(topo)
        tester.local_check = self._object_view
        tester.tuple_count(s, 1000)
        tester.test(self.test_ctxtype, self.test_config)

class TestViewsCloud(TestViews):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
