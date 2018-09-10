# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
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

class TestVersion(unittest.TestCase):
    """ 
    """
    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    def test_single_toolkit(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "spl.utility::Beacon",
            'tuple<rstring a>',
            {'iterations': 1})
        bop.a = bop.output('"A"')

        sv = op.Map(
            "com.ibm.streamsx.topology.pytest.pyvers::StreamsxVersion",
            bop.stream,
            'tuple<rstring a, rstring v1, rstring v2>')

        tester = Tester(topo)
        tester.contents(sv.stream, [{'a':'A', 'v1':'aggregate', 'v2':'True'}])
        tester.test(self.test_ctxtype, self.test_config)

    @unittest.expectedFailure
    def test_mixed_toolkits(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('tk17'))
        data = ['A']
        bop = op.Source(topo, "spl.utility::Beacon",
            'tuple<rstring a>',
            {'iterations': 1})
        bop.a = bop.output('"A"')

        sv = op.Map(
            "com.ibm.streamsx.topology.pytest.pyvers::StreamsxVersion",
            bop.stream,
            'tuple<rstring a, rstring v1, rstring v2>')

        m17f = op.Map(
            "com.ibm.streamsx.topology.pytest.tk17::M17F",
            sv.stream,
            'tuple<rstring a, rstring v1, rstring v2, rstring f1, rstring f2>')

        m17c = op.Map(
            "com.ibm.streamsx.topology.pytest.tk17::M17C",
            m17f.stream,
            'tuple<rstring a, rstring v1, rstring v2, rstring f1, rstring f2, rstring c1, rstring c2, int32 x>',
            {'x': 27})

        tester = Tester(topo)
        tester.contents(m17c.stream, [{'a':'A', 'f1':'1.7', 'f2':'F', 'v1':'aggregate', 'v2':'True', 'c1':'1.7', 'c2':'C', 'x':27}])
        tester.test(self.test_ctxtype, self.test_config)
