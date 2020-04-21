# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import time
import random
import itertools

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
import streamsx.spl.op as op
import typing
from typing import NamedTuple

"""
Test aggregate function with various input and output types.
"""

class Average:
    def __call__(self, tuples_in_window):
        values = [tpl["value"] for tpl in tuples_in_window]
        mn = min(values)
        mx = max(values)
        num_of_tuples = len(tuples_in_window)
        average = sum(values) / len(tuples_in_window)
        return {"count": num_of_tuples,
                "avg": average,
                "min": mn,
                "max": mx}

class AggregateSchema(NamedTuple):
    count: int = 0
    avg: float = 0.0
    min: int = 0
    max: int = 0
    mytext: str = ""

class AverageTypeHints:
    def __call__(self, tuples_in_window) -> AggregateSchema:
        values = [tpl["value"] for tpl in tuples_in_window]
        mn = min(values)
        mx = max(values)
        num_of_tuples = len(tuples_in_window)
        average = sum(values) / len(tuples_in_window)
        return {"count": num_of_tuples,
                "avg": average,
                "min": mn,
                "max": mx,
                "mytext": 'test'}

class AverageStrTypeHints:
    def __call__(self, tuples_in_window) -> AggregateSchema:
        # input is string
        values = [int(tpl) for tpl in tuples_in_window]
        mn = min(values)
        mx = max(values)
        num_of_tuples = len(tuples_in_window)
        average = sum(values) / len(tuples_in_window)
        return {"count": num_of_tuples,
                "avg": average,
                "min": mn,
                "max": mx,
                "mytext": 'test'}

class AverageNamedTuple:
    def __call__(self, tuples_in_window) -> AggregateSchema:
        values = [tpl.value for tpl in tuples_in_window]
        mn = min(values)
        mx = max(values)
        num_of_tuples = len(tuples_in_window)
        average = sum(values) / len(tuples_in_window)
        output_event = AggregateSchema(
            count = num_of_tuples,
            avg = average,
            min = mn,
            max = mx,
            mytext = 'test'
        )
        return output_event

class AverageObjectToNamedTuple:
    def __call__(self, tuples_in_window) -> AggregateSchema:
        values = [tpl["value"] for tpl in tuples_in_window]
        mn = min(values)
        mx = max(values)
        num_of_tuples = len(tuples_in_window)
        average = sum(values) / len(tuples_in_window)
        output_event = AggregateSchema(
            count = num_of_tuples,
            avg = average,
            min = mn,
            max = mx,
            mytext = 'test'
        )
        return output_event

class Numbers(object):
    def __call__(self):
        for num in itertools.count(1):
            if num == 11:
                break
            time.sleep(0.2)
            yield {"value": num, "id": "id_" + str(random.randint(0, 10))}


class NumbersSourceSchema(NamedTuple):
    value: int = 0

def generate_numbers_for_named_tuple_schema() -> typing.Iterable[NumbersSourceSchema]:
    idx = 0
    while idx < 10:
        idx += 1
        output_event = NumbersSourceSchema(
            value = idx
        )
        yield output_event


class TestAggregate(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def _runTest(self, topo, s):
        #s.print()

        #self.test_config = {
        #    ConfigParams.SSL_VERIFY: False,
        #    'topology.keepArtifacts': True
        #}

        tester = Tester(topo)
        tester.tuple_count(s, 10)
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_in_object_out(self):
        # Python object as input and output
        # stream<blob __spl_po> Numbers = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<blob __spl_po> Average = com.ibm.streamsx.topology.functional.python::Aggregate  ( Numbers)
        topo = Topology("test_object_in_object_out")
        src = topo.source(Numbers())
        window = src.last(size=10)
        rolling_average = window.aggregate(Average())
        self._runTest(topo, rolling_average)

    def test_object_in_dict_out_type_hints(self):
        # Python object as input and named tuple as output
        # stream<blob __spl_po> Numbers = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<int64 count, float64 avg, int64 min, int64 max, rstring mytext> AverageTypeHints = com.ibm.streamsx.topology.functional.python::Aggregate  ( Numbers)
        topo = Topology("test_object_in_dict_out_type_hints")
        src = topo.source(Numbers())
        window = src.last(size=10)
        rolling_average = window.aggregate(AverageTypeHints())
        self._runTest(topo, rolling_average)

    def test_object_in_named_tuple_out(self):
        # Python object as input and named tuple as output
        # stream<blob __spl_po> Numbers = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<int64 count, float64 avg, int64 min, int64 max, rstring mytext> AverageObjectToNamedTuple = com.ibm.streamsx.topology.functional.python::Aggregate  ( Numbers)
        topo = Topology("test_object_in_named_tuple_out")
        src = topo.source(Numbers())
        window = src.last(size=10)
        rolling_average = window.aggregate(AverageObjectToNamedTuple())
        self._runTest(topo, rolling_average)

    def test_json_in_dict_out_type_hints(self):
        # JSON as input and named tuple as output
        # stream<rstring jsonString> as_json = com.ibm.streamsx.topology.functional.python::Map  ( map_lambda)
        # stream<int64 count, float64 avg, int64 min, int64 max, rstring mytext> AverageTypeHints = com.ibm.streamsx.topology.functional.python::Aggregate
        topo = Topology("test_json_in_dict_out_type_hints")
        s = topo.source(range(10))
        src = s.map(lambda x : {'value': x}).as_json()
        window = src.last(size=10)
        rolling_average = window.aggregate(AverageTypeHints())
        self._runTest(topo, rolling_average)

    def test_str_in_dict_out_type_hints(self):
        # String as input and named tuple as output
        # stream<rstring string> as_string = com.ibm.streamsx.topology.functional.python::Map  ( range)
        # stream<int64 count, float64 avg, int64 min, int64 max, rstring mytext> AverageTypeHints = com.ibm.streamsx.topology.functional.python::Aggregate
        topo = Topology("test_str_in_dict_out_type_hints")
        src = topo.source(range(10)).as_string()
        window = src.last(size=10)
        rolling_average = window.aggregate(AverageStrTypeHints())
        self._runTest(topo, rolling_average)

    def test_named_tuple_in_named_tuple_out(self):
        # named tuple as input and named tuple as output
        # stream<int64 value> generate_data_for_named_tuple_schema = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<int64 count, float64 avg, int64 min, int64 max, rstring mytext> AverageNamedTuple = com.ibm.streamsx.topology.functional.python::Aggregate  ( generate_data_for_named_tuple_schema)
        topo = Topology("test_named_tuple_in_named_tuple_out")
        src = topo.source(generate_numbers_for_named_tuple_schema)
        window = src.last(size=10)
        rolling_average = window.aggregate(AverageNamedTuple())
        self._runTest(topo, rolling_average)


class TestDistributedAggregate(TestAggregate):
    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False

class TestSasAggregate(TestAggregate):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


