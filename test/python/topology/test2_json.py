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

import test_vers

class CheckForEach(object):
    def __init__(self):
        self.expected = ['ByRef', 3, 'a', 42]
        self.count = 0

    def __call__(self, v):
        if self.expected[self.count] != v:
            raise AssertionError()
        self.count += 1

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestJson(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def test_as_json(self):
        topo = Topology()
        s = topo.source(['JSON!', 3, list(('a', 42))])
        s = s.map(lambda x : {'abc': x})
        s = s.as_json()
        s = s.as_json()
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
        s = s.as_string()

        tester = Tester(topo)
        tester.contents(s, ['String!', '3', '42.0'])
        tester.test(self.test_ctxtype, self.test_config)
