# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2018
from __future__ import print_function
import unittest
import sys
import itertools
from enum import IntEnum
import datetime
import decimal
import os

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.spl.types
from streamsx.spl.types import Timestamp

def ts_check(tuple_):
    return isinstance(tuple_.ts, Timestamp)


class TestFormats(IntEnum):
    csv = 0
    txt = 1


class TestParseOption(IntEnum):
    strict = 0
    permissive = 1
    fast = 2
    

class TestSPL(unittest.TestCase):
    """ Test invocations of SPL operators from Python topology.
    """
    _multiprocess_can_split_ = True

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
        s.category = 'Generator'
        self.assertEqual(s.category, 'Generator')

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
        beacon = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':27})
        beacon.seq = beacon.output('IterationCount()')

        f = op.Map('spl.relational::Functor', beacon.stream,
            schema = 'tuple<uint64 a>',
            params = {'filter': op.Expression.expression('seq % 4ul == 0ul')})
        f.a = f.output(f.attribute('seq'))
        s = f.stream.map(lambda x : x['a'])

        tester = Tester(topo)
        tester.contents(s, [0, 4, 8, 12, 16, 20, 24])
        tester.test(self.test_ctxtype, self.test_config)

    def test_map_attr_opt(self):
        """Test a Source and a Map operator with optional types.
           Including with operator parameters and output clauses.
        """
        Tester.require_streams_version(self, '4.3')
        topo = Topology('test_map_attr_opt')
        this_dir = os.path.dirname(os.path.realpath(__file__))
        spl_dir = os.path.join(os.path.dirname(os.path.dirname(this_dir)), 'spl')
        tk_dir = os.path.join(spl_dir, 'testtkopt')
        streamsx.spl.toolkit.add_toolkit(topo, tk_dir)
        schema = 'tuple<' \
            'rstring r, ' \
            'optional<rstring> orv, ' \
            'optional<rstring> ornv, ' \
            'int32 i32, ' \
            'optional<int32> oi32v, ' \
            'optional<int32> oi32nv>'
        s = op.Source(topo, "testgen::TypeLiteralTester", schema, params = {
            'r': 'a string',
            'orv': 'optional string',
            'ornv': None,
            'i32': 123,
            'oi32v': 456,
            'oi32nv': streamsx.spl.types.null()})
        f = op.Map('spl.relational::Functor', s.stream, schema = schema)
        f.orv = f.output("null")
        f.ornv = f.output('"string value"')
        f.oi32v = f.output(streamsx.spl.types.null())
        f.oi32nv = f.output('789')
        tester = Tester(topo)
        tester.contents(s.stream, [{
            'r': 'a string',
            'orv': 'optional string',
            'ornv': None,
            'i32': 123,
            'oi32v': 456,
            'oi32nv': None}])
        tester.contents(f.stream, [{
            'r': 'a string',
            'orv': None,
            'ornv': 'string value',
            'i32': 123,
            'oi32v': None,
            'oi32nv': 789}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_stream_alias(self):
        """
        test a stream alias to ensure the SPL expression
        is consistent with hand-coded SPL expression.
        """
        topo = Topology('test_stream_alias')
        s = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'period': 0.02, 'iterations':27}, name='SomeName')
        s.seq = s.output('IterationCount()')

        stream = s.stream.aliased_as('IN')

        f = op.Map('spl.relational::Functor', stream,
            schema = 'tuple<uint64 a>',
            params = {'filter': op.Expression.expression('IN.seq % 4ul == 0ul')})
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
        ts_schema = StreamSchema('tuple<int32 a, timestamp ts>').as_tuple(named=True)

        ts1 = Timestamp(133001, 302245576, 56)
        ts2s = Timestamp(23543463, 876265298, 32)
        dt1 = ts2s.datetime()
        ts2 = Timestamp.from_datetime(dt1)

        self.assertEqual(ts2s.seconds, ts2s.seconds);

        topo = Topology()
        s = topo.source([(1,ts1), (2,dt1)])
        s = s.map(lambda x : x, schema=ts_schema)
        as_ts = s.map(lambda x : x.ts.tuple())
        s.print()

        tester = Tester(topo)
        tester.tuple_check(s, ts_check)
        tester.tuple_count(s, 2)
        tester.contents(as_ts, [ts1.tuple(), ts2.tuple()])
        tester.test(self.test_ctxtype, self.test_config)

    def test_custom_literal(self):
        schema = StreamSchema('tuple<int32 a, rstring b>')
        topo = Topology()
        s = topo.source([(1,'ABC'), (2,'DEF')])
        s = s.map(lambda x : x, schema=schema)

        fmt = op.Map('spl.utility::Format', s, 'tuple<blob data>',
            {'format':TestFormats.csv})
        fmt.data = fmt.output('Output()')

        parse = op.Map('spl.utility::Parse', fmt.stream, schema,
            {'format':TestFormats.csv, 'parsing': TestParseOption.fast})

        ts = parse.stream

        tester = Tester(topo)
        tester.tuple_count(ts, 2)
        tester.contents(ts, [{'a':1,'b':'ABC'},{'a':2,'b':'DEF'}])
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedSPL(TestSPL):
    def setUp(self):
        Tester.setup_distributed(self)

class TestBluemixSPL(TestSPL):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        # Ensure the old name still works.
        self.test_ctxtype = "ANALYTICS_SERVICE"

SPL_TYPES = {
             'float32', 'float64',
             'uint8','uint16', 'uint32', 'uint64',
             'int8','int16', 'int32', 'int64',
             'decimal32', 'decimal64', 'decimal128',
             'complex32', 'complex64',
             'timestamp'
            }

GOOD_DATA = {
    'float32': [7.5, 3, 0, False],
    'float64': [10.5, -2, 0, True],
    'int8': [23.5, -7, 0, 127, -128, False],
    'int16': [43.5, -7, 0, 32767, -32768, False],
    'int32': [9.5, -7, 0, 2147483647, -2147483648, False],
    'int64': [-83.5, -7, 0, 9223372036854775807,  -9223372036854775808, False],
    'decimal32': [-83.5, -7, 0, '4.33', decimal.Decimal('17.832')],
    'decimal64': [-993.335, -8, 0, '933.4543', decimal.Decimal('4932.3221')],
    'decimal128': [-83993.7883, -9, 0, '9355.332222', decimal.Decimal('5345.79745902883')],
    'complex32': [complex(8.0, -32.0), 0, 10.5, 93],
    'complex64': [complex(27.0, -8.0), 0, -83.5, 134],
    'timestamp': [Timestamp.now(7), datetime.datetime.today()]
}


class TestConversion(unittest.TestCase):
    """ Test conversions of Python values to SPL attributes/types.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_bad_from_string(self):
        for dt in SPL_TYPES:
            topo = Topology()
            schema = StreamSchema('tuple<' + dt + ' a>')
            s = topo.source(['ABC'])
            c = s.map(lambda x : (x,), schema=schema)
            e = c.filter(lambda t : True)
            #e.print(tag=dt)
        
            tester = Tester(topo)
            tester.tuple_count(e, 1)
            tr = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
            self.assertFalse(tr, msg=dt)

    def test_good(self):
        for dt in SPL_TYPES:
            if dt in GOOD_DATA:
                data = GOOD_DATA[dt]
                topo = Topology()
                schema = StreamSchema('tuple<' + dt + ' a>')
                s = topo.source(data)
                c = s.map(lambda x : (x,), schema=schema)
                #c.print(tag=dt)
        
                if dt.startswith('float'):
                    expected = [{'a':float(d)} for d in data]
                elif dt.startswith('int'):
                    expected = [{'a':int(d)} for d in data]
                elif dt == 'decimal32':
                    ctx = decimal.Context(prec=7, rounding=decimal.ROUND_HALF_EVEN)
                    expected = [{'a':decimal.Decimal(str(d)).normalize(ctx)} for d in data]
                elif dt == 'decimal64':
                    ctx = decimal.Context(prec=16, rounding=decimal.ROUND_HALF_EVEN)
                    expected = [{'a':decimal.Decimal(str(d)).normalize(ctx)} for d in data]
                elif dt == 'decimal128':
                    ctx = decimal.Context(prec=34, rounding=decimal.ROUND_HALF_EVEN)
                    expected = [{'a':decimal.Decimal(str(d)).normalize(ctx)} for d in data]
                elif dt.startswith('complex'):
                    expected = [{'a':complex(d)} for d in data]
                elif dt == 'timestamp':
                    expected = [{'a': d if isinstance(d, Timestamp) else Timestamp.from_datetime(d)} for d in data]
                    

                tester = Tester(topo)
                tester.tuple_count(c, len(data))
                tester.contents(c, expected)
                tester.test(self.test_ctxtype, self.test_config)

import shutil
import uuid
import subprocess
@unittest.skipIf('STREAMS_INSTALL' not in os.environ, 'STREAMS_INSTALL not set')
class TestMainComposite(unittest.TestCase):
    def test_main_composite(self):
        si = os.environ['STREAMS_INSTALL']
        tkl = 'tkl_mc_' + str(uuid.uuid4().hex)
        this_dir = os.path.dirname(os.path.realpath(__file__))
        shutil.copytree(os.path.join(this_dir, 'spl_mc'), tkl)
        ri = subprocess.call([os.path.join(si, 'bin', 'spl-make-toolkit'), '-i', tkl])
        self.assertEqual(0, ri)
        r = op.main_composite(kind='app::MyMain', toolkits=[tkl])
        self.assertIsInstance(r, tuple)
        self.assertIsInstance(r[0], Topology)
        self.assertIsInstance(r[1], op.Invoke)
        rc = streamsx.topology.context.submit('BUNDLE', r[0])
        self.assertEqual(0, rc['return_code'])
        shutil.rmtree(tkl)
        os.remove(rc['bundlePath'])
        os.remove(rc['jobConfigPath'])
