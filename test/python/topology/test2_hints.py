# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import typing
import random
import time

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit
from streamsx.topology.schema import StreamSchema, CommonSchema, _normalize

SensorReading = typing.NamedTuple('SensorReading',
    [('sensor_id', int), ('ts', int), ('reading', float)])

class P(object): pass
class S(P): pass

def s_none() : pass
def s_int() -> typing.Iterable[int] : pass
def s_str() -> typing.Iterable[str] : pass
def s_any() -> typing.Iterable[typing.Any] : pass
def s_sensor() -> typing.Iterable[SensorReading] : pass
def s_str_it() -> typing.Iterator[str] : pass

def s_p() -> typing.Iterable[P] : pass
def s_s() -> typing.Iterable[S] : pass

def f_none(t) : pass
def f_int(t:int) : pass
def f_str(t:str) : pass
def f_any(t: typing.Any) : pass
def f_sensor(t: SensorReading) : pass
def f_p(t: P) : pass
def f_s(t: S) : pass

def m_none(t) : pass
def m_int(t:int) -> str : pass
def m_str(t:str) -> SensorReading : pass
def m_any(t: typing.Any) -> str : pass
def m_sensor(t: SensorReading) : pass
def m_p(t: P) -> P : pass
def m_s(t: S) -> S : pass
def m_p2s(t: P) -> S : pass

def agg_none(t) : pass
def agg_any(t: typing.Any) -> str : pass
def agg_int(t:typing.List[int]) -> str : pass
def agg_str(t:typing.List[str]) -> SensorReading : pass
def agg_listany(t: typing.List[typing.Any]) -> str : pass
def agg_sensor(t: typing.List[SensorReading]) : pass
def agg_p(t: typing.List[P]) -> P : pass
def agg_s(t: typing.List[S]) -> S : pass
def agg_p2s(t: typing.List[P]) -> S : pass

def fm_none(t) : pass
def fm_int(t:int) : pass
def fm_str(t:str) -> typing.Iterable[SensorReading] : pass
def fm_any(t: typing.Any) -> typing.Iterable[str] : pass
def fm_sensor(t: SensorReading) : pass
def fm_p(t: P) -> typing.Iterable[P] : pass
def fm_s(t: S) -> typing.Iterable[S] : pass


def a_0() : pass
class A_0(object):
    def __call__(self): pass

def a_1(a) : pass
class A_1(object):
    def __call__(self, a): pass

def ao_1(a=None) : pass
class AO_1(object):
    def __call__(self, a=None): pass

def a_2(a,b) : pass
class A_2(object):
    def __call__(self, a, b): pass

def ao_2(a, b=None) : pass
class AO_2(object):
    def __call__(self, a, b=None): pass


class TestHints(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_no_type_checking(self):
        topo = Topology()
        topo.type_checking = False

        s = topo.source(s_int)
        s.filter(f_str)
        s.split(2, f_str)
        s.for_each(f_sensor)
        s.map(f_sensor)
        s.flat_map(f_sensor)

        s = topo.source(s_str)
        self.assertEqual(CommonSchema.Python, s.oport.schema)

    def test_source(self):
        topo = Topology()
  
        s = topo.source(s_none)
        self.assertEqual(CommonSchema.Python, s.oport.schema)

        s = topo.source(s_int)
        self.assertEqual(CommonSchema.Python, s.oport.schema)

        s = topo.source(s_str)
        self.assertEqual(CommonSchema.String, s.oport.schema)

        s = topo.source(s_any)
        self.assertEqual(CommonSchema.Python, s.oport.schema)

        s = topo.source(s_sensor)
        self.assertEqual(_normalize(SensorReading), s.oport.schema)

        s = topo.source(s_str_it)
        self.assertEqual(CommonSchema.String, s.oport.schema)

        s = topo.source(s_p)
        self.assertEqual(CommonSchema.Python, s.oport.schema)

        s = topo.source(s_s)
        self.assertEqual(CommonSchema.Python, s.oport.schema)

    def test_source_argcount(self):
        topo = Topology()
        topo.source(a_0)
        topo.source(A_0())
        self.assertRaises(TypeError, topo.source, a_1)
        self.assertRaises(TypeError, topo.source, A_1())
        topo.source(ao_1)
        topo.source(AO_1())

    def test_filter(self):
        topo = Topology()

        s = topo.source(s_none)
        s.filter(f_none)
        s.filter(f_int)
        s.filter(f_str)
        s.filter(f_any)
        s.filter(f_sensor)

        s = topo.source(s_int)
        s.filter(f_none)
        s.filter(f_int)
        self.assertRaises(TypeError, s.filter, f_str)
        s.filter(f_any)
        self.assertRaises(TypeError, s.filter, f_sensor)

        s = topo.source(s_str)
        s.filter(f_none)
        self.assertRaises(TypeError, s.filter, f_int)
        s.filter(f_str)
        s.filter(f_any)
        self.assertRaises(TypeError, s.filter, f_sensor)

        s = topo.source(s_any)
        s.filter(f_none)
        s.filter(f_int)
        s.filter(f_str)
        s.filter(f_any)
        s.filter(f_sensor)

        s = topo.source(s_sensor)
        s.filter(f_none)
        self.assertRaises(TypeError, s.filter, f_int)
        self.assertRaises(TypeError, s.filter, f_str)
        s.filter(f_any)
        s.filter(f_sensor)

        s = topo.source(s_p)
        s.filter(f_none)
        self.assertRaises(TypeError, s.filter, f_int)
        self.assertRaises(TypeError, s.filter, f_str)
        s.filter(f_any)
        self.assertRaises(TypeError, s.filter, f_sensor)
        s.filter(f_p)
        self.assertRaises(TypeError, s.filter, f_s)

        s = topo.source(s_s)
        s.filter(f_p)
        s.filter(f_s)

    def test_filter_argcount(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.filter, a_0)
        self.assertRaises(TypeError, s.filter, A_0())
        s.filter(a_1)
        s.filter(A_1())
        s.filter(ao_1)
        s.filter(AO_1())
        self.assertRaises(TypeError, s.filter, a_2)
        self.assertRaises(TypeError, s.filter, A_2())
        s.filter(ao_2)
        s.filter(AO_2())

    def test_split(self):
        topo = Topology()

        s = topo.source(s_none)
        s.split(2, f_none)
        s.split(2, f_int)
        s.split(2, f_str)
        s.split(2, f_any)
        s.split(2, f_sensor)

        s = topo.source(s_int)
        s.split(2, f_none)
        s.split(2, f_int)
        self.assertRaises(TypeError, s.split, 2, f_str)
        s.split(2, f_any)
        self.assertRaises(TypeError, s.split, 2, f_sensor)

        s = topo.source(s_str)
        s.split(2, f_none)
        self.assertRaises(TypeError, s.split, 2, f_int)
        s.split(2, f_str)
        s.split(2, f_any)
        self.assertRaises(TypeError, s.split, 2, f_sensor)

        s = topo.source(s_any)
        s.split(2, f_none)
        s.split(2, f_int)
        s.split(2, f_str)
        s.split(2, f_any)
        s.split(2, f_sensor)

        s = topo.source(s_sensor)
        s.split(2, f_none)
        self.assertRaises(TypeError, s.split, 2, f_int)
        self.assertRaises(TypeError, s.split, 2, f_str)
        s.split(2, f_any)
        s.split(2, f_sensor)

        s = topo.source(s_p)
        s.split(2, f_none)
        self.assertRaises(TypeError, s.split, 2, f_int)
        self.assertRaises(TypeError, s.split, 2, f_str)
        s.split(2, f_any)
        self.assertRaises(TypeError, s.split, 2, f_sensor)
        s.split(2, f_p)
        self.assertRaises(TypeError, s.split, 2, f_s)

        s = topo.source(s_s)
        s.split(2, f_p)
        s.split(2, f_s)

    def test_split_argcount(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.split, 2, a_0)
        self.assertRaises(TypeError, s.split, 2, A_0())
        s.split(2, a_1)
        s.split(2, A_1())
        s.split(2, ao_1)
        s.split(2, AO_1())
        self.assertRaises(TypeError, s.split, 2, a_2)
        self.assertRaises(TypeError, s.split, 2, A_2())
        s.split(2, ao_2)
        s.split(2, AO_2())

    def test_for_each(self):
        topo = Topology()

        s = topo.source(s_none)
        s.for_each(f_none)
        s.for_each(f_int)
        s.for_each(f_str)
        s.for_each(f_any)
        s.for_each(f_sensor)

        s = topo.source(s_int)
        s.for_each(f_none)
        s.for_each(f_int)
        self.assertRaises(TypeError, s.for_each, f_str)
        s.for_each(f_any)
        self.assertRaises(TypeError, s.for_each, f_sensor)

        s = topo.source(s_str)
        s.for_each(f_none)
        self.assertRaises(TypeError, s.for_each, f_int)
        s.for_each(f_str)
        s.for_each(f_any)
        self.assertRaises(TypeError, s.for_each, f_sensor)

        s = topo.source(s_any)
        s.for_each(f_none)
        s.for_each(f_int)
        s.for_each(f_str)
        s.for_each(f_any)
        s.for_each(f_sensor)

        s = topo.source(s_sensor)
        s.for_each(f_none)
        self.assertRaises(TypeError, s.for_each, f_int)
        self.assertRaises(TypeError, s.for_each, f_str)
        s.for_each(f_any)
        s.for_each(f_sensor)

        s = topo.source(s_p)
        s.for_each(f_none)
        self.assertRaises(TypeError, s.for_each, f_int)
        self.assertRaises(TypeError, s.for_each, f_str)
        s.for_each(f_any)
        self.assertRaises(TypeError, s.for_each, f_sensor)
        s.for_each(f_p)
        self.assertRaises(TypeError, s.for_each, f_s)

        s = topo.source(s_s)
        s.for_each(f_p)
        s.for_each(f_s)

    def test_for_each_argcount(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.for_each, a_0)
        self.assertRaises(TypeError, s.for_each, A_0())
        s.for_each(a_1)
        s.for_each(A_1())
        s.for_each(ao_1)
        s.for_each(AO_1())
        self.assertRaises(TypeError, s.for_each, a_2)
        self.assertRaises(TypeError, s.for_each, A_2())
        s.for_each(ao_2)
        s.for_each(AO_2())

    def test_map(self):
        topo = Topology()

        s = topo.source(s_none)
        s.map(m_none)
        sr = s.map(m_int)
        self.assertEqual(CommonSchema.String, sr.oport.schema)
        sr = s.map(m_str)
        self.assertEqual(_normalize(SensorReading), sr.oport.schema)
        s.map(m_any)
        s.map(m_sensor)

        s = topo.source(s_int)
        s.map(m_none)
        s.map(m_int)
        self.assertRaises(TypeError, s.map, m_str)
        s.map(m_any)
        self.assertRaises(TypeError, s.map, m_sensor)

        s = topo.source(s_str)
        s.map(m_none)
        self.assertRaises(TypeError, s.map, m_int)
        s.map(m_str)
        s.map(m_any)
        self.assertRaises(TypeError, s.map, m_sensor)

        s = topo.source(s_any)
        s.map(m_none)
        s.map(m_int)
        s.map(m_str)
        s.map(m_any)
        s.map(m_sensor)

        s = topo.source(s_sensor)
        s.map(m_none)
        self.assertRaises(TypeError, s.map, m_int)
        self.assertRaises(TypeError, s.map, m_str)
        s.map(m_any)
        s.map(m_sensor)

        s = topo.source(s_p)
        s.map(m_none)
        self.assertRaises(TypeError, s.map, m_int)
        self.assertRaises(TypeError, s.map, m_str)
        s.map(m_any)
        self.assertRaises(TypeError, s.map, m_sensor)
        sr = s.map(m_p)
        self.assertEqual(CommonSchema.Python, sr.oport.schema)
        self.assertRaises(TypeError, s.map, m_s)

        # Ensure we maintain the hint as well as the schema
        sr.map(m_p)
        self.assertRaises(TypeError, sr.map, m_s)
        sr.map(m_p2s).map(m_s)

        s = topo.source(s_s)
        s.map(m_p)
        s.map(m_s)

    def test_map_argcount(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.map, a_0)
        self.assertRaises(TypeError, s.map, A_0())
        s.map(a_1)
        s.map(A_1())
        s.map(ao_1)
        s.map(AO_1())
        self.assertRaises(TypeError, s.map, a_2)
        self.assertRaises(TypeError, s.map, A_2())
        s.map(ao_2)
        s.map(AO_2())

    def test_aggregate(self):
        topo = Topology()

        w = topo.source(s_none).last()
        w.aggregate(agg_none)
        sr = w.aggregate(agg_int)
        self.assertEqual(CommonSchema.String, sr.oport.schema)
        sr = w.aggregate(agg_str)
        self.assertEqual(_normalize(SensorReading), sr.oport.schema)
        w.aggregate(agg_any)
        w.aggregate(agg_sensor)

        w = topo.source(s_int).last()
        w.aggregate(agg_none)
        w.aggregate(agg_int)
        #self.assertRaises(TypeError, w.aggregate, agg_str)
        w.aggregate(agg_any)
        #self.assertRaises(TypeError, w.aggregate, agg_sensor)

        w = topo.source(s_str).last()
        w.aggregate(agg_none)
        #self.assertRaises(TypeError, w.aggregate, agg_int)
        w.aggregate(agg_str)
        w.aggregate(agg_any)
        #self.assertRaises(TypeError, w.aggregate, agg_sensor)

        w = topo.source(s_any).last()
        w.aggregate(agg_none)
        w.aggregate(agg_int)
        w.aggregate(agg_str)
        w.aggregate(agg_any)
        w.aggregate(agg_sensor)

        w = topo.source(s_sensor).last()
        w.aggregate(agg_none)
        #self.assertRaises(TypeError, w.aggregate, agg_int)
        #self.assertRaises(TypeError, w.aggregate, agg_str)
        w.aggregate(agg_any)
        w.aggregate(agg_sensor)

        w = topo.source(s_p).last()
        w.aggregate(agg_none)
        #self.assertRaises(TypeError, w.aggregate, agg_int)
        #self.assertRaises(TypeError, w.aggregate, agg_str)
        w.aggregate(agg_any)
        #self.assertRaises(TypeError, w.aggregate, agg_sensor)
        sr = w.aggregate(agg_p)
        self.assertEqual(CommonSchema.Python, sr.oport.schema)
        #self.assertRaises(TypeError, w.aggregate, agg_s)

        w = topo.source(s_s).last()
        w.aggregate(agg_p)
        w.aggregate(agg_s)

    def test_aggregate_argcount(self):
        topo = Topology()
        w = topo.source([]).last(1)
        self.assertRaises(TypeError, w.aggregate, a_0)
        self.assertRaises(TypeError, w.aggregate, A_0())
        w.aggregate(a_1)
        w.aggregate(A_1())
        w.aggregate(ao_1)
        w.aggregate(AO_1())
        self.assertRaises(TypeError, w.aggregate, a_2)
        self.assertRaises(TypeError, w.aggregate, A_2())
        w.aggregate(ao_2)
        w.aggregate(AO_2())

    def test_flat_map(self):
        topo = Topology()

        s = topo.source(s_none)
        s.flat_map(fm_none)
        s.flat_map(fm_int)
        sr = s.flat_map(fm_str)
        self.assertEqual(_normalize(SensorReading), sr.oport.schema)
        sr.flat_map(fm_sensor)
        s.flat_map(fm_any)
        s.flat_map(fm_sensor)

        s = topo.source(s_int)
        s.flat_map(fm_none)
        s.flat_map(fm_int)
        self.assertRaises(TypeError, s.flat_map, fm_str)
        s.flat_map(fm_any)
        self.assertRaises(TypeError, s.flat_map, fm_sensor)

        s = topo.source(s_str)
        s.flat_map(fm_none)
        self.assertRaises(TypeError, s.flat_map, fm_int)
        sr = s.flat_map(fm_str)
        self.assertEqual(_normalize(SensorReading), sr.oport.schema)
        sr.flat_map(fm_sensor)
        s.flat_map(fm_any)
        self.assertRaises(TypeError, s.flat_map, fm_sensor)

        s = topo.source(s_any)
        s.flat_map(fm_none)
        s.flat_map(fm_int)
        sr = s.flat_map(fm_str)
        self.assertEqual(_normalize(SensorReading), sr.oport.schema)
        sr.flat_map(fm_sensor)
        s.flat_map(fm_any)
        s.flat_map(fm_sensor)

        s = topo.source(s_sensor)
        s.flat_map(fm_none)
        self.assertRaises(TypeError, s.flat_map, fm_int)
        self.assertRaises(TypeError, s.flat_map, fm_str)
        s.flat_map(fm_any)
        s.flat_map(fm_sensor)

        s = topo.source(s_p)
        s.flat_map(fm_none)
        self.assertRaises(TypeError, s.flat_map, fm_int)
        self.assertRaises(TypeError, s.flat_map, fm_str)
        s.flat_map(fm_any)
        self.assertRaises(TypeError, s.flat_map, fm_sensor)
        s.flat_map(fm_p)
        self.assertRaises(TypeError, s.flat_map, fm_s)

        s = topo.source(s_s)
        s.flat_map(fm_p)
        s.flat_map(fm_s)

    def test_flat_map_argcount(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.flat_map, a_0)
        self.assertRaises(TypeError, s.flat_map, A_0())
        s.flat_map(a_1)
        s.flat_map(A_1())
        s.flat_map(ao_1)
        s.flat_map(AO_1())
        self.assertRaises(TypeError, s.flat_map, a_2)
        self.assertRaises(TypeError, s.flat_map, A_2())
        s.flat_map(ao_2)
        s.flat_map(AO_2())

    def test_as_string(self):
        topo = Topology()

        s = topo.source(s_none)
        s = s.as_string()

        s.map(m_none)
        self.assertRaises(TypeError, s.map, m_int)
        s.map(m_str)
        s.map(m_any)
        self.assertRaises(TypeError, s.map, m_sensor)

    def test_maintain_hints(self):
        topo = Topology()
        s = topo.source(s_str)
        s.map(m_str)
        self.assertRaises(TypeError, s.map, m_sensor)

        d = s.autonomous()
        d.map(m_str)
        self.assertRaises(TypeError, d.map, m_sensor)

        d = s.low_latency()
        d.map(m_str)
        self.assertRaises(TypeError, d.map, m_sensor)

        d = d.end_low_latency()
        d.map(m_str)
        self.assertRaises(TypeError, d.map, m_sensor)

        p = s.parallel(width=3)
        t = p.map(m_str).as_string()
        self.assertRaises(TypeError, p.map, m_sensor)

        e = t.end_parallel()
        e.map(m_str)
        self.assertRaises(TypeError, e.map, m_sensor)

class V_Sensor(object):
    def __init__(self):
        self.c = 0
  
    def __iter__(self) -> typing.Iterable[SensorReading] :
        return self

    def __next__(self) -> SensorReading:
        if self.c == 0:
             self.c += 1
             return SensorReading(101, 10, 3.0)
        if self.c == 1:
             self.c += 1
             return {'sensor_id':202, 'ts':20, 'reading': 7.0}
        raise StopIteration()

class V_Str(object):
    def __init__(self):
        self.c = 0
  
    def __iter__(self) -> typing.Iterable[str] :
        return self

    def __next__(self) -> SensorReading:
        if self.c == 0:
             self.c += 1
             return 'Hello'
        if self.c == 1:
             self.c += 1
             return 'World'
        raise StopIteration()

class TestRunningHints(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_structured(self):
        topo = Topology()
        s = topo.source(V_Sensor())
        d = s.map(lambda v : v._asdict())

        tester = Tester(topo)
        tester.tuple_count(s, 2)
        tester.tuple_check(s, lambda v : isinstance(v, tuple))
        tester.contents(d, [{'sensor_id':101, 'ts':10, 'reading': 3.0}, {'sensor_id':202, 'ts':20, 'reading': 7.0}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_string(self):
        topo = Topology()
        s = topo.source(V_Str())

        tester = Tester(topo)
        tester.tuple_count(s, 2)
        tester.tuple_check(s, lambda v : isinstance(v, str))
        tester.contents(s, ['Hello', 'World'])
        tester.test(self.test_ctxtype, self.test_config)
