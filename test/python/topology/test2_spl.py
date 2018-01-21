# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_vers

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.topology.context
import streamsx.spl.op as op
from streamsx.spl.types import Timestamp

def ts_check(tuple_):
    return isinstance(tuple_[1], Timestamp)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestSPL(unittest.TestCase):
    """ Test invocations of SPL operators from Python topology.
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def test_SPLBeaconFilter(self):
        """Test a Source and a Map operator.
           Including an output clause.
        """
        topo = Topology('test_SPLBeaconFilter')
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':100})
        s.seq = s.output('IterationCount()')

        f = op.Map('spl.relational::Filter', s.stream,
            params = {'filter': op.Expression.expression('seq % 2ul == 0ul')})

        tester = Tester(topo)
        tester.tuple_count(f.stream, 50)
        tester.test(self.test_ctxtype, self.test_config)

    def test_map_attr(self):
        """Test a Source and a Map operator.
           Including an output clause.
        """
        topo = Topology('test_SPLBeaconFilter')
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':27})
        s.seq = s.output('IterationCount()')

        f = op.Map('spl.relational::Functor', s.stream,
            schema = 'tuple<uint64 a>',
            params = {'filter': op.Expression.expression('seq % 4ul == 0ul')})
        f.a = f.output(f.attribute('seq'))
        s = f.stream.map(lambda x : x['a'])

        tester = Tester(topo)
        tester.contents(s, [0, 4, 8, 12, 16, 20, 24])
        tester.test(self.test_ctxtype, self.test_config)

    def test_SPL_as_json(self):
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq, rstring b>',
            params = {'period': 0.02, 'iterations':5})
        b.seq = b.output('IterationCount()')

        s = b.stream.as_json()

        tester = Tester(topo)
        tester.contents(s, [{'seq':0, 'b':''}, {'seq':1, 'b':''}, {'seq':2, 'b':''}, {'seq':3, 'b':''}, {'seq':4, 'b':''}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_SPL_as_string(self):
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq, rstring b>',
            params = {'period': 0.02, 'iterations':5})
        b.seq = b.output('IterationCount()')
        b.b = b.output('"str!"')

        s = b.stream.as_string()
        s = s.map(lambda x : eval(x))

        tester = Tester(topo)
        tester.contents(s, [{'seq':0, 'b':'str!'}, {'seq':1, 'b':'str!'}, {'seq':2, 'b':'str!'}, {'seq':3, 'b':'str!'}, {'seq':4, 'b':'str!'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_timestamp(self):
        ts_schema = StreamSchema('tuple<int32 a, timestamp ts>').as_tuple()

        ts1 = Timestamp(133001, 302245576, 56)
        ts2s = Timestamp(23543463, 876265298, 32)
        dt1 = ts2s.datetime()
        ts2 = Timestamp.from_datetime(dt1)

        self.assertEqual(ts2s.seconds, ts2s.seconds);

        topo = Topology()
        s = topo.source([(1,ts1), (2,dt1)])
        s = s.map(lambda x : x, schema=ts_schema)
        as_ts = s.map(lambda x : x[1])
        s.print()

        tester = Tester(topo)
        tester.tuple_check(s, ts_check)
        tester.tuple_count(s, 2)
        tester.contents(as_ts, [ts1.tuple(), ts2.tuple()])
        tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestDistributedSPL(TestSPL):
    def setUp(self):
        Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestBluemixSPL(TestSPL):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        # Ensure the old name still works.
        self.test_ctxtype = "ANALYTICS_SERVICE"
