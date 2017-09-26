# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.schema import StreamSchema
from streamsx.topology.tester import Tester

"""
Test that structured schemas can be passed into Python functions as tuples.
"""

def check_is_tuple(t):
    if type(t) != tuple:
        raise ValueError("Expected a tuple:" + str(t) + " >> type:" + str(type(t)))

def check_is_tuple_for_each(t):
    check_is_tuple(t)
    if t[1] != str(t[0]*2) + "Hi!":
        raise ValueError("Incorrect value:" + str(t))

def check_is_tuple_filter(t):
    check_is_tuple_for_each(t)
    return t[0] != 2

def check_is_tuple_hash(t):
    print("ISHASH:", t, flush=True)
    check_is_tuple_for_each(t)
    print("ISHASH_RET:", t[0], flush=True)
    return t[0]

def check_is_tuple_map(t):
    check_is_tuple(t)
    return (t[0]*7, t[1] + "-Map")


def check_is_tuple_map_to_string(t):
    check_is_tuple(t)
    return str(t[0]*11) + t[1] + "-MapString"

def check_is_tuple_map_to_schema(t):
    check_is_tuple(t)
    return (t[0]*13, t[1] + "-MapSPL")

def check_is_tuple_flat_map(t):
    check_is_tuple(t)
    return [(t[0]*9, t[1] + "-FlatMap")]

def check_is_tuple_map_to_json(t):
    check_is_tuple(t)
    return {'a':t[0]*15, 'b':t[1] + "-MapJSON"}

@unittest.skipIf(not test_vers.tester_supported() , "tester not supported")
class TestSchemaTuple(unittest.TestCase):
    """ Test invocations handling of SPL schemas in Python ops.
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def _create_stream(self, topo):
        s = topo.source([1,2,3])
        schema=StreamSchema('tuple<int32 x, rstring msg>').as_tuple()
        return s.map(lambda x : (x,str(x*2) + "Hi!"), schema=schema)

    def test_as_tuple_for_each(self):
        topo = Topology()
        st = self._create_stream(topo)
        st.for_each(check_is_tuple_for_each)

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.map(check_is_tuple_map)

        tester = Tester(topo)
        tester.contents(st, [(7,'2Hi!-Map'), (14,'4Hi!-Map'), (21,'6Hi!-Map')])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_filter(self):
        topo = Topology()
        s = self._create_stream(topo)
        s = s.filter(check_is_tuple_filter)

        tester = Tester(topo)
        tester.tuple_count(s, 2)
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_flat_map(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.flat_map(check_is_tuple_flat_map)

        tester = Tester(topo)
        tester.contents(st, [(9,'2Hi!-FlatMap'), (18,'4Hi!-FlatMap'), (27,'6Hi!-FlatMap')])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_hash(self):
        topo = Topology()
        s = self._create_stream(topo)
        s = s.parallel(width=2, routing=Routing.HASH_PARTITIONED, func=check_is_tuple_hash)
        s = s.map(check_is_tuple_map)
        s = s.end_parallel()

        tester = Tester(topo)
        tester.contents(s, [(7,'2Hi!-Map'), (14,'4Hi!-Map'), (21,'6Hi!-Map')], ordered=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map_to_string(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.map(check_is_tuple_map_to_string, schema=CommonSchema.String)

        tester = Tester(topo)
        tester.contents(st, ['112Hi!-MapString', '224Hi!-MapString', '336Hi!-MapString'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map_to_schema(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.map(check_is_tuple_map_to_schema, schema=StreamSchema('tuple<int32 y, rstring txt>'))

        tester = Tester(topo)
        tester.contents(st, [{'y':13, 'txt':'2Hi!-MapSPL'}, {'y':26, 'txt':'4Hi!-MapSPL'}, {'y':39, 'txt':'6Hi!-MapSPL'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map_to_json(self):
        topo = Topology()
        s = self._create_stream(topo)
        s = s.map(check_is_tuple_map_to_json, schema=CommonSchema.Json)
        s = s.map(lambda x : str(x['a']) + x['b'])

        tester = Tester(topo)
        tester.contents(s, ['152Hi!-MapJSON', '304Hi!-MapJSON', '456Hi!-MapJSON'])
        tester.test(self.test_ctxtype, self.test_config)

