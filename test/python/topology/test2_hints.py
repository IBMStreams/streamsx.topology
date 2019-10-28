# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import typing
import random
import time

from streamsx.topology.topology import Topology
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
