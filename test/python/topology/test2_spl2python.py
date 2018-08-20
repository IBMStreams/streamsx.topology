# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import threading

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema as cs
import streamsx.spl.op as op

"""
Test that we can covert SPL tuples to all Python operators.
"""

def beacon(topo, schema):
    s = op.Source(topo, "spl.utility::Beacon", schema,
        params = {'iterations':100})
    if schema is cs.Json:
        s.jsonString = s.output('"{}"')
    return s.stream

schemas = [
     'tuple<uint64 seq, rstring rs, float64 f64>',
     cs.Json, cs.String]

class TestSPL2Python(unittest.TestCase):
    """ Test invocations handling of SPL schemas in Python ops.
    """
    _multiprocess_can_split_ = True

    # Fake out subTest
    if sys.version_info.major == 2:
        def subTest(self, **args): return threading.Lock()

    def setUp(self):
        Tester.setup_standalone(self)

    def test_schemas_filter(self):
        """Test various schemas are handled by filter
        """
        for schema in schemas:
            with self.subTest(schema=schema):

                topo = Topology('test_schemas_filter')
                b = beacon(topo, schema)
                f = b.filter(lambda tuple : True)

                f2 = b.filter(lambda tuple : True)
                f2 = op.Map('spl.relational::Filter', f2).stream

                f = f.union({f2})
        
                tester = Tester(topo)
                tester.tuple_count(f, 200)
                tester.test(self.test_ctxtype, self.test_config)

    def test_schemas_map(self):
        """Test various schemas are handled by map
        """
        for schema in schemas:
            with self.subTest(schema=schema):

                topo = Topology('test_schemas_map')
                b = beacon(topo, schema)
                f = b.map(lambda tuple : tuple)

                f2 = b.map(lambda tuple : tuple)
                f2 = op.Map('spl.relational::Filter', f2).stream

                f = f.union({f2})
        
                tester = Tester(topo)
                tester.tuple_count(f, 200)
                tester.test(self.test_ctxtype, self.test_config)

    def test_schemas_flat_map(self):
        """Test various schemas are handled by flat_map
        """
        for schema in schemas:
            with self.subTest(schema=schema):

                topo = Topology('test_schemas_flat_map')
                b = beacon(topo, schema)
                f = b.flat_map(lambda tuple : [tuple, tuple])

                # Check all combinations of the output type
                # passing by value
                f2 = b.flat_map(lambda tuple : [tuple, tuple, tuple])
                f2 = op.Map('spl.relational::Filter', f2).stream

                f = f.union({f2})

                tester = Tester(topo)
                tester.tuple_count(f, 500)
                tester.test(self.test_ctxtype, self.test_config)

    def test_schemas_for_each(self):
        """Test various schemas are handled by for_each
        """
        for schema in schemas:
            with self.subTest(schema=schema):

                topo = Topology('test_schemas_for_each')
                b = beacon(topo, schema)
                b.for_each(lambda tuple : None)
        
                tester = Tester(topo)
                tester.tuple_count(b, 100)
                tester.test(self.test_ctxtype, self.test_config)

    def test_map_opt(self):
        """Test optional type value and no value are passed correctly
        """
        Tester.require_streams_version(self, '4.3')
        topo = Topology('test_map_opt')
        schema='tuple<optional<uint64> i>'
        b = op.Source(topo, "spl.utility::Beacon", schema,
            params = {'iterations':3})
        b.i = b.output('IterationCount() % 2ul == 0ul ?' +
           'IterationCount() : (optional<uint64>) null')
        s = b.stream
        f = s.map(lambda tuple :
            (1,) if tuple['i'] == None
            else (None,) if tuple['i'] == 2
            else (tuple['i'],), schema=schema)
        tester = Tester(topo)
        tester.contents(s, [{'i':0}, {'i':None}, {'i':2}])
        tester.contents(f, [{'i':0}, {'i':1}, {'i':None}])
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributedSPL(TestSPL2Python):
    def setUp(self):
        Tester.setup_distributed(self)

class TestBluemixSPL(TestSPL2Python):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
