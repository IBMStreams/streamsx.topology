# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import typing
import decimal
from py36_types import NamedTupleBytesSchema, NamedTupleNumbersSchema, NamedTupleListOfTupleSchema, NamedTupleNestedTupleSchema

"""
Test that NamedTuples schemas can be passed from and into Python functions as tuples.
"""

def generate_data_for_bytes_schema() -> typing.Iterable[NamedTupleBytesSchema]:
    idx = 0
    while idx < 3:
        idx += 1
        output_event = NamedTupleBytesSchema(
            idx = str(idx),
            msg = bytes(b"python"),
            flag = True,
            oidx = str('optional'+str(idx)) if ((idx % 2) == 0) else None,
            omsg = bytes(b"python") if ((idx % 2) == 0) else None,
            oflag = False if ((idx % 2) == 0) else None
        )
        yield output_event

class check_bytes_tuple():
  def __call__(self, t):
    print(t)
    # attribute msg is of type memoryview (sequence of bytes)
    if isinstance(t.msg, memoryview):
        b = t.msg.tobytes()
        index = b.find(b"on")
        #print(index)
        if index < 0:
            raise ValueError("Incorrect bytes value")
    else:
        raise TypeError("Invalid type for bytes attribute")
    if isinstance(t.idx, str):
        pass
    else:
        raise TypeError("Invalid type for idx attribute")

def generate_data_for_numbers_schema() -> typing.Iterable[NamedTupleNumbersSchema]:
    idx = 0
    while idx < 3:
        idx += 1
        output_event = NamedTupleNumbersSchema(
            i64 = idx,
            f64 = 0.5,
            d128 = decimal.Decimal('0.79745902883'),
            c64 = complex(8.0, -32.0),
            oi64 = idx if ((idx % 2) == 0) else None,
            of64 = 0.123 if ((idx % 2) == 0) else None,
            od128 = decimal.Decimal('5.15566') if ((idx % 2) == 0) else None,
            oc64 = complex(2.0, -12.0) if ((idx % 2) == 0) else None,
            omi64li64 = {123: [1, 2, 3]} if ((idx % 2) == 0) else None
        )
        yield output_event

class check_numbers_tuple():
  def __call__(self, t):
    print(t) 
    if isinstance(t.i64, int) and isinstance(t.f64, float) and isinstance(t.d128, decimal.Decimal) and isinstance(t.c64, complex):
        pass
    else:
        raise TypeError("Invalid type")

def simple_map(tpl):
    print("simple_map: "+str(tpl))
    return tpl


def simple_map_to_named_tuple(tpl) -> NamedTupleNestedTupleSchema:
    print("simple_map_to_named_tuple: "+str(tpl))
    return tpl

def simple_map_to_named_tuple_list(tpl) -> NamedTupleListOfTupleSchema:
    print("simple_map_to_named_tuple_list: "+str(tpl))
    return tpl


class TestNamedTupleSource(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def test_bytes(self):
        # python source -> python sink (NamedTupleBytesSchema)
        topo = Topology()
        st = topo.source(generate_data_for_bytes_schema)
        st.for_each(check_bytes_tuple())

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_bytes_py_spl_py(self):
        # python source -> spl filter -> python sink (NamedTupleBytesSchema)
        topo = Topology()
        st = topo.source(generate_data_for_bytes_schema)
        fst = op.Map('spl.relational::Filter', st).stream
        fst.for_each(check_bytes_tuple())

        tester = Tester(topo)
        tester.tuple_count(fst, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_bytes_spl_py(self):
        # spl source -> python sink (NamedTupleBytesSchema)
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleBytesSchema,
            params = {'period': 0.1, 'iterations':3})
        b.idx = b.output('(rstring) IterationCount()')
        b.msg = b.output('convertToBlob(\"python\")')
        b.omsg = b.output('IterationCount() % 2ul == 0ul ?' +
           'convertToBlob(\"python\") : (optional<blob>) null')
        st = b.stream
        st.for_each(check_bytes_tuple())

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_numbers(self):
        # python source -> python sink (NamedTupleNumbersSchema)
        topo = Topology()
        st = topo.source(generate_data_for_numbers_schema)
        st.for_each(check_numbers_tuple())

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_numbers_py_spl_py(self):
        # python source -> spl filter -> python sink (NamedTupleNumbersSchema)
        topo = Topology()
        st = topo.source(generate_data_for_numbers_schema)
        fst = op.Map('spl.relational::Filter', st).stream
        fst.for_each(check_numbers_tuple())

        tester = Tester(topo)
        tester.tuple_count(fst, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_numbers_spl_py(self):
        # spl source -> python sink (NamedTupleNumbersSchema)
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleNumbersSchema,
            params = {'period': 0.1, 'iterations':3})
        b.i64 = b.output('(int64) IterationCount()')
        b.oi64 = b.output('IterationCount() % 2ul == 0ul ?' +
           '(int64) IterationCount() : (optional<int64>) null')
        b.of64 = b.output('IterationCount() % 2ul == 0ul ?' +
           '0.321 : (optional<float64>) null')
        b.od128 = b.output('IterationCount() % 2ul == 0ul ?' +
           '(decimal128) 10.1245345 : (optional<decimal128>) null')
        b.omi64li64 = b.output('IterationCount() % 2ul == 0ul ?' +
           '{321l: [1l, 2l, 3l]} : (optional<map<int64, list<int64>>>) null')
        st = b.stream
        st.for_each(check_numbers_tuple())

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_list_of_tuple_spl_py(self):
        # spl source -> python map (python object output) -> python sink (NamedTupleListOfTupleSchema)
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleListOfTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.spotted = b.output('[{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}]')

        st = b.stream
        sm1 = st.map(simple_map, name='MapSPL2PY')
        sm1.print()
 
        tester = Tester(topo)
        tester.tuple_count(sm1, 3)
        self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_list_of_tuple_spl_py_namedtuple(self):
        # spl source -> python map (named tuple output) -> python sink (NamedTupleListOfTupleSchema)
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleListOfTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.spotted = b.output('[{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}]')

        st = b.stream
        sm1 = st.map(simple_map_to_named_tuple_list, name='MapSPL2NamedTuple')
        sm1.print()
 
        tester = Tester(topo)
        tester.tuple_count(sm1, 3)
        self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)


    def test_nested_tuple_spl_py(self):
        # spl source -> python sink (NamedTupleNestedTupleSchema)
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleNestedTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.key = b.output('(rstring)IterationCount()')
        b.spotted = b.output('{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}')
        st = b.stream
        sm1 = st.map(simple_map_to_named_tuple, name='MapSPL2NamedTuple')
        sm1.print()
 
        tester = Tester(topo)
        tester.tuple_count(sm1, 3)
        self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_nested_tuple_streamschema_spl_py(self):
        # spl source -> python sink (StreamSchema)
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            schema='tuple<rstring key, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>',
            params = {'period': 0.1, 'iterations':3})
        b.key = b.output('(rstring)IterationCount()')
        b.spotted = b.output('{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}')

        st = b.stream
        sm1 = st.map(simple_map, name='MapSPL2PY')
        sm1.print()
 
        tester = Tester(topo)
        tester.tuple_count(sm1, 3)
        self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)




