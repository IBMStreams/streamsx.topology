# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import sys
import itertools
import os
from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import typing
import decimal
from py36_types import NamedTupleBytesSchema, NamedTupleNumbersSchema, NamedTupleListOfTupleSchema, NamedTupleNestedTupleSchema, PersonSchema, SpottedSchema, NamedTupleMapWithTupleSchema, NamedTupleMapWithListTupleSchema, NamedTupleSetOfListofTupleSchema

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
    #print(t)
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

class SourceDictOutWithNested(object):
    def __call__(self) -> typing.Iterable[NamedTupleNestedTupleSchema]:
        for num in itertools.count(1):
            if num == 4:
                break
            yield {"key": str(num), "spotted" : {"start_time": 0.2, "end_time": 0.4, "confidence": 0.4} }


class SourceTupleOutWithNested(object):
    def __call__(self) -> typing.Iterable[NamedTupleNestedTupleSchema]:
        for num in itertools.count(1):
            if num == 4:
                break
            spotted_event = SpottedSchema(
                start_time = 0.1,
                end_time = 0.2,
                confidence = 0.5
            ) 
            output_event = NamedTupleNestedTupleSchema(
                key = str(num),
                spotted = spotted_event
            )
            yield output_event


class SourceDictOutMapWithTuple(object):
    def __call__(self) -> typing.Iterable[NamedTupleMapWithTupleSchema]:
        for num in itertools.count(1):
            if num == 4:
                break
            yield {"keywords_spotted": {str(num) : {"start_time": 0.2, "end_time": 0.4, "confidence": 0.4}}}


class SourceDictOutMapWithListOfTuple(object):
    def __call__(self) -> typing.Iterable[NamedTupleMapWithListTupleSchema]:
        for num in itertools.count(1):
            if num == 4:
                break
            yield {"keywords_spotted": {str(num) : [{"start_time": 0.2, "end_time": 0.4, "confidence": 0.4},{"start_time": 0.3, "end_time": 0.6, "confidence": 0.8}]}}



class check_numbers_tuple():
  def __call__(self, t):
    #print(t) 
    if isinstance(t.i64, int) and isinstance(t.f64, float) and isinstance(t.d128, decimal.Decimal) and isinstance(t.c64, complex):
        pass
    else:
        raise TypeError("Invalid type")

def simple_map(tpl):
    print("simple_map: "+str(tpl))
    return tpl

def map_dict_to_dict(tpl) -> NamedTupleNestedTupleSchema:
    print("map_dict_to_dict: "+str(tpl))
    tpl['spotted']['confidence'] = tpl['spotted']['confidence'] + 0.4
    return tpl

def map_dict_list_dict(tpl) -> NamedTupleListOfTupleSchema:
    print("map_dict_list_dict: "+str(tpl))
    an_item = {"start_time": 0.3, "end_time": 0.6, "confidence": 0.8}
    tpl['spotted'].append(an_item)
    return tpl

def map_dict_to_namedtuple_list(tpl) -> NamedTupleListOfTupleSchema:
    an_item = {"start_time": 0.3, "end_time": 0.6, "confidence": 0.8}
    tpl['spotted'].append(an_item)

    out = NamedTupleListOfTupleSchema(
        spotted = tpl['spotted']
    )
    return out

def simple_map_to_person_schema(tpl) -> PersonSchema:
    print("simple_map_to_person_schema: "+str(tpl))
    return tpl


expected_contents_nested_beacon_source = """{key="0",spotted={start_time=0.1,end_time=0.2,confidence=0.7}}
{key="1",spotted={start_time=0.1,end_time=0.2,confidence=0.7}}
{key="2",spotted={start_time=0.1,end_time=0.2,confidence=0.7}}
Punctuation received: WindowMarker
Punctuation received: FinalMarker
"""

expected_contents_nested_py_source = """{key="1",spotted={start_time=0.1,end_time=0.2,confidence=0.9}}
{key="2",spotted={start_time=0.1,end_time=0.2,confidence=0.9}}
{key="3",spotted={start_time=0.1,end_time=0.2,confidence=0.9}}
Punctuation received: FinalMarker
"""

expected_contents_list1 = """{spotted=[{start_time=0.1,end_time=0.2,confidence=0.5},{start_time=0.3,end_time=0.6,confidence=0.8}]}
{spotted=[{start_time=0.1,end_time=0.2,confidence=0.5},{start_time=0.3,end_time=0.6,confidence=0.8}]}
{spotted=[{start_time=0.1,end_time=0.2,confidence=0.5},{start_time=0.3,end_time=0.6,confidence=0.8}]}
Punctuation received: WindowMarker
Punctuation received: FinalMarker
"""

expected_contents_nested_multi1 = """{name="Bacon0",age=20,address={street="Rue",city="Macon",contacts={mail="mymail@test.org",phone="+09875",nested_tuple={flag=true,i64=123456789}}}}
{name="Bacon1",age=21,address={street="Rue",city="Macon",contacts={mail="mymail@test.org",phone="+09875",nested_tuple={flag=true,i64=123456789}}}}
Punctuation received: WindowMarker
Punctuation received: FinalMarker
"""

################################
debug_named_tuple_output = True
################################

class TestNamedTupleSource(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def _test_spl_file(self, topo, s, test_name, expected_content, expected_tuple_count):
        if debug_named_tuple_output:
            s.print()
        op_params = {'file' : 'spl_file_'+test_name, 'format' : op.Expression.expression('txt'), 'writePunctuations' : True, 'flushOnPunctuation' : True}
        op.Sink("spl.adapter::FileSink", s, params = op_params)

        # Copy the config, since it's shared across all tests, and not every test needs a data
        # directory.
        cfg = self.test_config.copy()
        jc = JobConfig(data_directory=os.getcwd())
        jc.add(cfg)
        if debug_named_tuple_output:
            cfg['topology.keepArtifacts'] = True
         
        tester = Tester(topo)
        tester.tuple_count(s, expected_tuple_count)
        tester.test(self.test_ctxtype, cfg)

        path = os.path.join(os.getcwd(), 'spl_file_'+test_name)
        
        # Validate the contents of the file.
        with open(path, 'r') as f:
            file_contents = f.read()
            self.assertEqual(expected_content, file_contents)    
        os.remove(path)


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
        b.si64 = b.output('{(int64) IterationCount(),(int64) IterationCount()+100l}')
        st = b.stream
        st.for_each(check_numbers_tuple())
        if debug_named_tuple_output:
            st.print()

        tester = Tester(topo)
        tester.tuple_count(st, 3)
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)


    def test_unsupported_types_for_conversion_to_from_py(self):
        tc = 'test_unsupported_types_for_conversion_to_from_py'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema='tuple<set<list<int64>> sli64>',
            params = {'period': 0.1, 'iterations':3})
        b.sli64 = b.output('{[(int64) IterationCount()],[0l]}')
        s = b.stream
        print ('-A- EXPECT ERROR CDISP9164E for unsupported type: tuple<set<list<int64>>>')
        tester = Tester(topo)
        tester.tuple_count(s, 3) # causes test to fail: type is not supported for conversion to or from Python
        res = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
        assert(False == res) # expected result: test failed
        print ('-A- unsupported type check: PASSED')

        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema='tuple<set<tuple<rstring a, rstring b>> stuple>',
            params = {'period': 0.1, 'iterations':3})
        b.stuple = b.output('{{a="a", b="b"}}')
        s = b.stream
        print ('-B- EXPECT ERROR CDISP9164E for unsupported type: tuple<set<tuple<rstring a, rstring b>>>')
        tester = Tester(topo)
        tester.tuple_count(s, 3) # causes test to fail: type is not supported for conversion to or from Python
        res = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
        assert(False == res) # expected result: test failed
        print ('-B- unsupported type check: PASSED')

        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleSetOfListofTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.slt = b.output('{[{start_time=(float64)0.1, end_time=(float64)0.2, confidence=(float64)0.5}]}')
        s = b.stream
        print ('-C- EXPECT ERROR CDISP9164E for unsupported type: set<list<tuple<float64 start_time,float64 end_time,float64 confidence>>>')
        tester = Tester(topo)
        tester.tuple_count(s, 3) # causes test to fail: type is not supported for conversion to or from Python
        res = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
        assert(False == res) # expected result: test failed
        print ('-C- unsupported type check: PASSED')

    def test_spl_source_list_of_tuple_named_tuple_py_sink(self):
        # spl source -> python map (python object output) -> python sink
        tc = 'test_spl_source_list_of_tuple_named_tuple_py_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleListOfTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.spotted = b.output('[{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}]')
        bstream = b.stream
        s = bstream.map(map_dict_to_namedtuple_list, name='MapSPL2PYNamedTuple')
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        tester.contents(s, [{'spotted': [{'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}, {'start_time': 0.3, 'end_time': 0.6, 'confidence': 0.8}]}, {'spotted': [{'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}, {'start_time': 0.3, 'end_time': 0.6, 'confidence': 0.8}]}, {'spotted': [{'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}, {'start_time': 0.3, 'end_time': 0.6, 'confidence': 0.8}]}])
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_spl_source_list_of_tuple_py_sink(self):
        # spl source -> python map (python object output) -> python sink
        tc = 'test_spl_source_list_of_tuple_py_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleListOfTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.spotted = b.output('[{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}]')
        bstream = b.stream
        s = bstream.map(map_dict_list_dict, name='MapSPL2PY')
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        tester.contents(s, [{'spotted': [{'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}, {'start_time': 0.3, 'end_time': 0.6, 'confidence': 0.8}]}, {'spotted': [{'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}, {'start_time': 0.3, 'end_time': 0.6, 'confidence': 0.8}]}, {'spotted': [{'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}, {'start_time': 0.3, 'end_time': 0.6, 'confidence': 0.8}]}])
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_spl_source_list_of_tuple_spl_sink(self):
        # spl source -> python map (NamedTupleListOfTupleSchema) -> spl sink
        tc = 'test_spl_source_list_of_tuple_spl_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleListOfTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.spotted = b.output('[{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}]')
        bstream = b.stream
        s = bstream.map(map_dict_list_dict, name='MapSPL2NamedTuple')
        self._test_spl_file(topo, s, tc, expected_contents_list1, 3)

    def test_spl_source_namedtuple_schema_nested_spl_sink(self):
        # spl source -> python map (NamedTupleNestedTupleSchema) -> spl sink
        tc = 'test_spl_source_namedtuple_schema_nested_spl_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleNestedTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.key = b.output('(rstring)IterationCount()')
        b.spotted = b.output('{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.3}')
        bstream = b.stream
        s = bstream.map(map_dict_to_dict, name='MapSPL2NamedTuple')
        self._test_spl_file(topo, s, tc, expected_contents_nested_beacon_source, 3)

    def test_spl_source_streams_schema_nested_py_sink(self):
        # spl source -> python map (python object) -> python sink
        tc = 'test_spl_source_streams_schema_nested_py_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema='tuple<rstring key, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>',
            params = {'period': 0.1, 'iterations':3})
        b.key = b.output('(rstring)IterationCount()')
        b.spotted = b.output('{start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}')
        bstream = b.stream
        s = bstream.map(simple_map, name='MapSPL2PY')
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        tester.contents(s, [{'key':'0','spotted': {'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}}, {'key':'1','spotted': {'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}}, {'key':'2','spotted': {'start_time': 0.1, 'end_time': 0.2, 'confidence': 0.5}}])
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_spl_source_multi_nested_tuple_py_sink(self):
        # spl source -> python map (python object) -> python sink
        tc = 'test_spl_source_multi_nested_tuple_py_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema='tuple<rstring name, int32 age, tuple<rstring street, rstring city, tuple<rstring mail, rstring phone> contacts> address>',
            params = {'period': 0.1, 'iterations':2})
        b.name = b.output('"Bacon"+(rstring)IterationCount()')
        b.age = b.output('(int32)IterationCount()+20')
        b.address = b.output('{street="Rue", city="Macon", contacts={mail="mymail@test.org", phone="+09875"}}')
        bstream = b.stream
        s = bstream.map(simple_map, name='MapSPL2PY')
        tester = Tester(topo)
        tester.tuple_count(s, 2)
        tester.contents(s, [{'name': 'Bacon0', 'age': 20, 'address': {'street': 'Rue', 'city': 'Macon', 'contacts': {'mail': 'mymail@test.org', 'phone': '+09875'}}}, {'name': 'Bacon1', 'age': 21, 'address': {'street': 'Rue', 'city': 'Macon', 'contacts': {'mail': 'mymail@test.org', 'phone': '+09875'}}}])
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_spl_source_multi_nested_tuple_spl_sink(self):
        # spl source -> python map (PersonSchema) -> spl sink
        tc = 'test_spl_source_multi_nested_tuple_spl_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=PersonSchema,
            params = {'period': 0.1, 'iterations':2})
        b.name = b.output('"Bacon"+(rstring)IterationCount()')
        b.age = b.output('(int64)IterationCount()+20l')
        b.address = b.output('{street="Rue", city="Macon", contacts={mail="mymail@test.org", phone="+09875", nested_tuple={flag=true, i64=123456789l}}}')
        bstream = b.stream
        s = bstream.map(simple_map_to_person_schema, name='MapSPL2NamedTuple')
        self._test_spl_file(topo, s, tc, expected_contents_nested_multi1, 2)

    def test_py_source_dict_nested_py_sink(self):
        # python source -> python map -> python sink (NamedTupleNestedTupleSchema)
        tc = 'test_py_source_dict_nested_py_sink'
        topo = Topology(tc)
        s = topo.source(SourceDictOutWithNested())
        s = s.map(map_dict_to_dict, name='MapDict2Dict')
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        tester.contents(s, [{'key':'1','spotted': {'start_time': 0.2, 'end_time': 0.4, 'confidence': 0.8}}, {'key':'2','spotted': {'start_time': 0.2, 'end_time': 0.4, 'confidence': 0.8}}, {'key':'3','spotted': {'start_time': 0.2, 'end_time': 0.4, 'confidence': 0.8}}])
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)

    def test_py_source_tuple_nested_spl_sink(self):
        # python source -> python map -> spl sink (NamedTupleNestedTupleSchema)
        tc = 'test_py_source_tuple_nested_spl_sink'
        topo = Topology(tc)
        s = topo.source(SourceTupleOutWithNested())
        s = s.map(map_dict_to_dict, name='MapDict2SPL')
        self._test_spl_file(topo, s, tc, expected_contents_nested_py_source, 3)


    def test_spl_source_map_of_tuple_py_sink(self):
        # spl source -> python map (python object output) -> python sink
        tc = 'test_spl_source_map_of_tuple_py_sink'
        topo = Topology(tc)
        b = op.Source(topo, "spl.utility::Beacon",
            schema=NamedTupleMapWithTupleSchema,
            params = {'period': 0.1, 'iterations':3})
        b.keywords_spotted = b.output('{(rstring) IterationCount(): {start_time=(float64)0.1, end_time=(float64)0.2 , confidence=(float64)0.5}}')
        bstream = b.stream
        s = bstream.map(simple_map, name='MapSPL2PY')
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        #tester.contents(s, TODO check content)
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)


    def test_py_source_map_of_tuple_py_sink(self):
        # python source -> python map -> python sink (NamedTupleMapWithTupleSchema)
        tc = 'test_py_source_map_of_tuple_py_sink'
        topo = Topology(tc)
        s = topo.source(SourceDictOutMapWithTuple())
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        #tester.contents(s, TODO check content)
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)


    def test_py_source_map_with_list_of_tuple_py_sink(self):
        # python source -> python map -> python sink (NamedTupleMapWithListTupleSchema)
        tc = 'test_py_source_map_with_list_of_tuple_py_sink'
        topo = Topology(tc)
        s = topo.source(SourceDictOutMapWithListOfTuple())
        if debug_named_tuple_output:
            s.print()
        tester = Tester(topo)
        tester.tuple_count(s, 3)
        #tester.contents(s, TODO check content)
        if debug_named_tuple_output:
            self.test_config['topology.keepArtifacts'] = True
        tester.test(self.test_ctxtype, self.test_config)


