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

class TestByRef(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_ByRef(self):
        topo = Topology()
        s = topo.source(['ByRef', 3, list(('a', 42))])
        s = s.map(lambda x : x)
        s = s.flat_map(lambda x : x if isinstance(x, list) else [x])
        s.for_each(CheckForEach())

        tester = Tester(topo)
        tester.contents(s, ['ByRef', 3, 'a', 42])
        tester.test(self.test_ctxtype, self.test_config)

    def test_NotByRef(self):
        topo = Topology()
        s = topo.source(['ByRef', 3, list(('a', 42))])
        f = op.Map('spl.relational::Filter', s)
        s = f.stream.map(lambda x : x)
        f = op.Map('spl.relational::Filter', s)
        s = f.stream.flat_map(lambda x : x if isinstance(x, list) else [x])
        f = op.Map('spl.relational::Filter', s)
        f.stream.for_each(CheckForEach())

        tester = Tester(topo)
        tester.contents(s, ['ByRef', 3, 'a', 42])
        tester.test(self.test_ctxtype, self.test_config)
