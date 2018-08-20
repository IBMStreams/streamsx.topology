# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester

"""
Test that structured schemas can be passed into Python functions as tuples.
"""

def check_correct(named, t):
    if named:
        if not isinstance(t, tuple):
            raise ValueError("Expected a tuple:" + str(t) + " >> type:" + str(type(t)))
        for fn in type(t)._fields:
            if not hasattr(t, fn):
                raise ValueError("Expected tuple:" + str(t) + " >> type:" + str(type(t)) + " to have attribute " + fn)
 
    elif type(t) != tuple:
        raise ValueError("Expected a tuple:" + str(t) + " >> type:" + str(type(t)))

class check_for_tuple_type(object):
  def __init__(self, named):
     self.named = named

class check_is_tuple_for_each(check_for_tuple_type):
  def __call__(self, t):
    check_correct(self.named, t)
    if t[1] != str(t[0]*2) + "Hi!":
        raise ValueError("Incorrect value:" + str(t))

class check_is_tuple_filter(check_is_tuple_for_each):
  def __call__(self, t):
    super(check_is_tuple_filter, self).__call__(t)
    return t[0] != 2

def check_is_tuple_hash(t):
    check_correct(False, t)
    if t[1] != str(t[0]*2) + "Hi!":
        raise ValueError("Incorrect value:" + str(t))
    return t[0]

def check_is_namedtuple_hash(t):
    check_correct(True, t)
    if t.msg != str(t.x*2) + "Hi!":
        raise ValueError("Incorrect value:" + str(t))
    return t.x

class check_is_tuple_map(check_for_tuple_type):
  def __call__(self, t):
    check_correct(self.named, t)
    return (t[0]*7, t[1] + "-Map")


class check_is_tuple_map_to_string(check_for_tuple_type):
  def __call__(self, t):
    check_correct(self.named, t)
    return str(t[0]*11) + t[1] + "-MapString"

class check_is_tuple_map_to_schema(check_for_tuple_type):
  def __call__(self, t):
    check_correct(self.named, t)
    return (t[0]*13, t[1] + "-MapSPL")

class check_is_tuple_flat_map(check_for_tuple_type):
  def __call__(self, t):
    check_correct(self.named, t)
    return [(t[0]*9, t[1] + "-FlatMap")]

class check_is_tuple_map_to_json(check_for_tuple_type):
  def __call__(self, t):
    check_correct(self.named, t)
    return {'a':t[0]*15, 'b':t[1] + "-MapJSON"}

class TestSchemaTuple(unittest.TestCase):
    """ Test invocations handling of SPL schemas in Python ops.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def is_named(self):
        return False

    def hash_check(self):
        return check_is_tuple_hash

    def _create_stream(self, topo):
        s = topo.source([1,2,3])
        schema=StreamSchema('tuple<int32 x, rstring msg>').as_tuple()
        return s.map(lambda x : (x,str(x*2) + "Hi!"), schema=schema)

    def test_as_tuple_for_each(self):
        topo = Topology()
        st = self._create_stream(topo)
        st.for_each(check_is_tuple_for_each(self.is_named()))

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.map(check_is_tuple_map(self.is_named()))

        tester = Tester(topo)
        tester.contents(st, [(7,'2Hi!-Map'), (14,'4Hi!-Map'), (21,'6Hi!-Map')])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_filter(self):
        topo = Topology()
        s = self._create_stream(topo)
        s = s.filter(check_is_tuple_filter(self.is_named()))

        tester = Tester(topo)
        tester.tuple_count(s, 2)
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_flat_map(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.flat_map(check_is_tuple_flat_map(self.is_named()))

        tester = Tester(topo)
        tester.contents(st, [(9,'2Hi!-FlatMap'), (18,'4Hi!-FlatMap'), (27,'6Hi!-FlatMap')])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_hash(self):
        topo = Topology()
        s = self._create_stream(topo)
        s = s.parallel(width=2, routing=Routing.HASH_PARTITIONED, func=self.hash_check())
        s = s.map(check_is_tuple_map(self.is_named()))
        s = s.end_parallel()

        tester = Tester(topo)
        tester.contents(s, [(7,'2Hi!-Map'), (14,'4Hi!-Map'), (21,'6Hi!-Map')], ordered=False)
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map_to_string(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.map(check_is_tuple_map_to_string(self.is_named()), schema=CommonSchema.String)

        tester = Tester(topo)
        tester.contents(st, ['112Hi!-MapString', '224Hi!-MapString', '336Hi!-MapString'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map_to_schema(self):
        topo = Topology()
        s = self._create_stream(topo)
        st = s.map(check_is_tuple_map_to_schema(self.is_named()), schema=StreamSchema('tuple<int32 y, rstring txt>'))

        tester = Tester(topo)
        tester.contents(st, [{'y':13, 'txt':'2Hi!-MapSPL'}, {'y':26, 'txt':'4Hi!-MapSPL'}, {'y':39, 'txt':'6Hi!-MapSPL'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_tuple_map_to_json(self):
        topo = Topology()
        s = self._create_stream(topo)
        s = s.map(check_is_tuple_map_to_json(self.is_named()), schema=CommonSchema.Json)
        s = s.map(lambda x : str(x['a']) + x['b'])

        tester = Tester(topo)
        tester.contents(s, ['152Hi!-MapJSON', '304Hi!-MapJSON', '456Hi!-MapJSON'])
        tester.test(self.test_ctxtype, self.test_config)


class TestSchemaNamedTuple(TestSchemaTuple):
    def is_named(self):
        return True

    def hash_check(self):
        return check_is_namedtuple_hash

    def _create_stream(self, topo):
        s = topo.source([1,2,3])
        schema=StreamSchema('tuple<int32 x, rstring msg>').as_tuple(named=True)
        return s.map(lambda x : (x,str(x*2) + "Hi!"), schema=schema)
