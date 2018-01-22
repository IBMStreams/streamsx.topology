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
import streamsx.spl.toolkit
import streamsx.spl.types


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

    @unittest.skipIf(not test_vers.optional_type_supported() , "Optional type not supported")
    def test_map_attr_opt(self):
        """Test a Source and a Map operator with optional types.
           Including with operator parameters and output clauses.
        """
        topo = Topology('test_map_attr_opt')
        streamsx.spl.toolkit.add_toolkit(topo, '../../spl/testtkopt')
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
