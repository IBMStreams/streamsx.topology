# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import os
import shutil

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams
import streamsx.spl.op as op
import test_functions

class CheckForEach(object):
    def __init__(self):
        self.expected = ['ByRef', 3, 'a', 42]
        self.count = 0

    def __call__(self, v):
        if self.expected[self.count] != v:
            raise AssertionError()
        self.count += 1

class TestJson(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_as_json(self):
        topo = Topology()
        s = topo.source(['JSON!', 3, list(('a', 42))])
        s = s.map(lambda x : {'abc': x})
        s = s.as_json()
        s2 = s.as_json()
        self.assertIs(s, s2)
        f = op.Map('spl.relational::Functor', s, schema='tuple<rstring string>')
        f.string = f.output(f.attribute('jsonString'))
        s = f.stream.map(lambda x : json.loads(x))
        s = s.map(lambda x : x['abc'])

        tester = Tester(topo)
        tester.contents(s, ['JSON!', 3, ['a', 42]])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_string(self):
        topo = Topology()
        s = topo.source(['String!', 3, 42.0])
        s = s.as_string()
        s2 = s.as_string()
        self.assertIs(s, s2)

        tester = Tester(topo)
        tester.contents(s, ['String!', '3', '42.0'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_json_as_string(self):
        topo = Topology()
        s = topo.source(['JSON_STRING!', 89, list(('b', 93))])
        s = s.map(lambda x : {'abc': x})
        s = s.as_json()
        s = s.as_string()
        s = s.map(lambda x : eval(x))
        s = s.map(lambda x : x['abc'])

        tester = Tester(topo)
        tester.contents(s, ['JSON_STRING!', 89, ['b', 93]])
        tester.test(self.test_ctxtype, self.test_config)

    def test_as_string_as_json(self):
        topo = Topology()
        s = topo.source(['STRING_JSON!', 235, 93.6])
        s = s.as_string()
        s = s.as_json()

        tester = Tester(topo)
        tester.contents(s, [{'payload': 'STRING_JSON!'}, {'payload': '235'}, {'payload': '93.6'}])
        tester.test(self.test_ctxtype, self.test_config)
