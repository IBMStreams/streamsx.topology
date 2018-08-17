# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import os
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.scripts.extract

import spl_tests_utils as stu

class TestSources(unittest.TestCase):
    """ 
    Test @spl.source decorated operators
    """

    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    def test_class_source(self):
        count = 43
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pysamples.sources::Range", schema.StreamSchema('tuple<int64 c>').as_tuple(), params={'count':count})
        r = bop.stream
        self.tester = Tester(topo)
        self.tester.tuple_count(r, count)
        self.tester.contents(r, list(zip(range(count))))
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_fn_source(self):
        count = 37
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pysamples.sources::Range37", schema.StreamSchema('tuple<int64 c>').as_tuple())
        r = bop.stream
        self.tester = Tester(topo)
        self.tester.tuple_count(r, count)
        self.tester.contents(r, list(zip(range(count))))
        self.tester.test(self.test_ctxtype, self.test_config)
