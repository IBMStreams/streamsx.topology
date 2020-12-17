# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import sys
import itertools
import os
import datetime
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op
from streamsx.topology.schema import StreamSchema
from streamsx.topology.context import ConfigParams
from streamsx.spl.types import Timestamp



class TestEventTime(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    @unittest.skipIf("STREAMS_INSTALL" not in os.environ, "STREAMS_INSTALL not set")
    def test_spl_types_compile_only(self):
        topo = Topology('test_spl_types_compile_only')
        beacon = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring ticks>')
        beacon.ticks = beacon.output('"1608196235"')

        # event-time annotation TYPE=uint64
        f1 = op.Map('spl.relational::Functor', beacon.stream, schema='tuple<uint64 tsEventTimeUInt64>')
        f1.tsEventTimeUInt64 = f1.output('(uint64)ticks')
        f1 = f1.stream.set_event_time('tsEventTimeUInt64', lag=1.0, minimum_gap=0.2)
       
        # event-time annotation TYPE=int64
        lag = topo.create_submission_parameter('lag', 0.4)
        gap = topo.create_submission_parameter('gap', 0.2)
        f2 = op.Map('spl.relational::Functor', beacon.stream, schema='tuple<int64 tsEventTimeInt64>')
        f2.tsEventTimeInt64 = f2.output('(int64)ticks')
        f2 = f2.stream.set_event_time('tsEventTimeInt64', lag=lag, minimum_gap=gap, resolution='Milliseconds')

        # event-time annotation TYPE=timestamp
        f3 = op.Map('spl.relational::Functor', beacon.stream, schema='tuple<timestamp tsEventTime>')
        f3.tsEventTime = f3.output('toTimestamp((int64)ticks, Sys.Milliseconds)')
        f3 = f3.stream.set_event_time('tsEventTime')

        # timeInterval window
        win = f3.time_interval(interval_duration=10.0, creation_period=1.0)
        agg = op.Map('spl.relational::Aggregate', win, schema='tuple<uint64 pi, timestamp start, timestamp end>')
        agg.pi = agg.output('paneIndex()')
        agg.start = agg.output('intervalStart()')
        agg.end = agg.output('intervalEnd()')

        sr = streamsx.topology.context.submit('BUNDLE', topo)
        self.assertEqual(0, sr['return_code'])
        os.remove(sr.bundlePath)


    def test_timestamp_event_time_attribute(self):
        topo = Topology('test_timestamp_event_time_attribute')

        ts1 = Timestamp(1608196, 235000000, 0)
        ts2 = Timestamp(1608199, 876265298, 0)
        ts_schema = StreamSchema('tuple<int64 num, timestamp ts>').as_tuple(named=True)
        s = topo.source([(1,ts1), (2,ts2)])

        # map() named "event_time_source" transforms to structured schema
        s = s.map(lambda x : x, schema=ts_schema, name='event_time_source')
        # add event-time annotation for attribute ts to the "event_time_source"
        s = s.set_event_time('ts')

        # use SPL function getEventTime() to get the event time of the input tuple
        # copies the event-time timestamp value to a new output attribute "eventtime"
        f = op.Map('spl.relational::Functor', s, schema=StreamSchema('tuple<timestamp eventtime>').as_tuple(named=True))
        f.eventtime = f.output('getEventTime(event_time_source)')

        # map to Python schema (prepare for content comparision)
        as_ts = f.stream.map(lambda x : x.eventtime)

        tester = Tester(topo)
        tester.tuple_count(s, 2)
        tester.contents(as_ts, [ts1, ts2])
        tester.test(self.test_ctxtype, self.test_config)


    def test_window_python_aggregate(self):
        topo = Topology('test_window_python_aggregate')

        ts1 = Timestamp(1608196, 235000000, 0)
        ts_schema = StreamSchema('tuple<int64 num, timestamp ts>').as_tuple(named=True)
        s = topo.source([(1,ts1)])

        # map() named "event_time_source" transforms to structured schema
        s = s.map(lambda x : x, schema=ts_schema, name='event_time_source')
        # add event-time annotation for attribute ts to the "event_time_source"
        s = s.set_event_time('ts')

        win = s.time_interval(interval_duration=10.0)
        # expect TypeError: Time-Interval window is not supported
        self.assertRaises(TypeError, win.aggregate, lambda t: sum(t.num))


    def test_window_spl_aggregate(self):
        topo = Topology('test_window_spl_aggregate')

        ts1 = Timestamp(1608196, 235000000, 0)
        ts2 = Timestamp(1608199, 876265298, 0)
        ts3 = Timestamp(1608506, 123456078, 0)
        ts4 = Timestamp(1608507, 654326980, 0)
        ts_schema = StreamSchema('tuple<int64 num, timestamp ts>').as_tuple(named=True)
        s = topo.source([(1,ts1), (2,ts2), (3,ts3), (4,ts4)])

        # map() named "event_time_source" transforms to structured schema
        s = s.map(lambda x : x, schema=ts_schema, name='event_time_source')
        # add event-time annotation for attribute ts to the "event_time_source"
        s = s.set_event_time('ts')

        agg_schema = StreamSchema('tuple<uint64 sum>').as_tuple(named=True)
        # timeInterval window
        win = s.time_interval(interval_duration=5.0, discard_age=30.0)
        agg = op.Map('spl.relational::Aggregate', win, schema=agg_schema)
        agg.sum = agg.output('Sum((uint64)num)')

        result = agg.stream.map(lambda x : x.sum)
        #result.print()

        tester = Tester(topo)
        tester.tuple_count(result, 2)
        tester.contents(result, [3, 7])
        tester.test(self.test_ctxtype, self.test_config)


