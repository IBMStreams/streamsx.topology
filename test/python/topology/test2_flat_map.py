# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import itertools
import os
from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
import streamsx.spl.op as op
import typing
from typing import Iterable, List, NamedTuple

"""
Test flat_map function with various input and output types.
"""

class NumbersSchema(NamedTuple):
    num: int

class NumbersListSchema(NamedTuple):
    numlist: List[int]

class GenerateNumbersNamedTuple(object):

    def __init__(self, iterations, range):
        self.iterations = iterations
        self.range = range

    def __call__(self) -> Iterable[NumbersListSchema]:
        for num in itertools.count(1):
            if num >  self.iterations:
                break
            l = []
            l.extend(range(0, self.range))
            yield {"numlist": l}

class GenerateNumbers(object):

    def __init__(self, iterations, range):
        self.iterations = iterations
        self.range = range

    def __call__(self):
        for num in itertools.count(1):
            if num >  self.iterations:
                break
            l = []
            l.extend(range(0, self.range))
            yield {"numlist": l}

class GenerateListOfDict(object):

    def __init__(self, iterations, range):
        self.iterations = iterations
        self.range = range

    def __call__(self):
        for num in itertools.count(1):
            if num >  self.iterations:
                break
            l = []
            count = 0
            while count < self.range:
                d = {'num': count, "a":'a'+str(count), 'b':True}
                l.append(d.copy())
                count += 1
            yield l

def flatten_flat_map(tpl):
    return tpl.numlist

def flatten_flat_map_with_type_hints(tpl) -> Iterable[NumbersSchema]:
    return tpl.numlist

def flatten_flat_map_obj(tpl):
    return tpl["numlist"]

def flatten_flat_map_obj_with_type_hints(tpl) -> Iterable[NumbersSchema]:
    return tpl["numlist"]

class SampleSchema(NamedTuple):
    num: int
    a: str
    b: bool

def flatten_flat_map_dict_with_type_hints(tpl) -> Iterable[SampleSchema]:
    return tpl

#############################
debug_flat_map_output = False
#############################

class TestFlatMap(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def _runTest(self, topo, s):
        if debug_flat_map_output:
            import streamsx.standard.files as files
            from streamsx.standard import CloseMode, Format
            s.print()
            fsink_config = {
              'format': Format.txt.name,
              'write_punctuations': True
            }
            script_dir = os.path.dirname(os.path.realpath(__file__))
            debug_out_file = os.path.join(script_dir, 'flat_map.txt')
            fsink = files.FileSink(file=debug_out_file, **fsink_config)
            s.for_each(fsink)
            self.test_config['topology.keepArtifacts'] = True

        tester = Tester(topo)
        tester.tuple_count(s, 10)
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_in_object_out(self):
        # Python object as input and output
        # stream<blob __spl_po> GenerateNumbers = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<blob __spl_po> flatten_flat_map_obj = com.ibm.streamsx.topology.functional.python::FlatMap  ( GenerateNumbers)
        topo = Topology("test_object_in_object_out")
        s = topo.source(GenerateNumbers(2,5))
        s = s.flat_map(flatten_flat_map_obj)
        self._runTest(topo, s)

    def test_object_in_namedtuple_out(self):
        # Python object as input and named tuple as output
        # stream<blob __spl_po> GenerateNumbers = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<int64 num> flatten_flat_map_obj_with_type_hints = com.ibm.streamsx.topology.functional.python::FlatMap  ( GenerateNumbers)
        topo = Topology("test_object_in_namedtuple_out")
        s = topo.source(GenerateNumbers(2,5))
        s = s.flat_map(flatten_flat_map_obj_with_type_hints)
        self._runTest(topo, s)

    def test_dict_in_namedtuple_out(self):
        # Python dict as input and named tuple as output
        # stream<blob __spl_po> GenerateListOfDict = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<int64 num, rstring a, boolean b> flatten_flat_map_dict_with_type_hints = com.ibm.streamsx.topology.functional.python::FlatMap  ( GenerateListOfDict)
        topo = Topology("test_dict_in_namedtuple_out")
        s = topo.source(GenerateListOfDict(2,5))
        s = s.flat_map(flatten_flat_map_dict_with_type_hints)
        self._runTest(topo, s)

    def test_namedtuple_in_namedtuple_out(self):
        # named tuple as input and named tuple as output
        # stream<list<int64> numlist> GenerateNumbersNamedTuple = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<int64 num> flatten_flat_map_with_type_hints = com.ibm.streamsx.topology.functional.python::FlatMap  ( GenerateNumbersNamedTuple)
        topo = Topology("test_namedtuple_in_namedtuple_out")
        s = topo.source(GenerateNumbersNamedTuple(2,5))
        s = s.flat_map(flatten_flat_map_with_type_hints)
        self._runTest(topo, s)

    def test_namedtuple_in_object_out(self):
        # named tuple as input and Python object as output
        # stream<list<int64> numlist> GenerateNumbersNamedTuple = com.ibm.streamsx.topology.functional.python::Source  ( )
        # stream<blob __spl_po> flatten_flat_map = com.ibm.streamsx.topology.functional.python::FlatMap  ( GenerateNumbersNamedTuple)
        topo = Topology("test_namedtuple_in_object_out")
        s = topo.source(GenerateNumbersNamedTuple(2,5))
        s = s.flat_map(flatten_flat_map)
        self._runTest(topo, s)


class TestDistributedFlatMap(TestFlatMap):
    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False

class TestSasFlatMap(TestFlatMap):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


