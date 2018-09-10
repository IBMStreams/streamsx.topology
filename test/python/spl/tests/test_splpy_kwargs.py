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
import streamsx.spl.runtime as ssr
import streamsx.scripts.extract

import spl_tests_utils as stu

class TestKWArgs(unittest.TestCase):
    _multiprocess_can_split_ = True

    """ 
    Test decorated operators with **kwargs
    """
    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    def test_filer_for_each(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        s = topo.source([(1,2,3), (1,2,201), (2,2,301), (3,4,401), (5,6,78), (8,6,501), (803, 9324, 901)])
        sch = 'tuple<int32 a, int32 b, int32 c>'
        s = s.map(lambda x : x, schema=sch)
        bop = op.Map("com.ibm.streamsx.topology.pytest.pykwargs::KWFilter", s)

        op.Sink("com.ibm.streamsx.topology.pytest.pykwargs::KWForEach", bop.stream)
        
        tester = Tester(topo)
        tester.contents(bop.stream, [{'a':1, 'b':2, 'c':201}, {'a':3, 'b':4, 'c':401}, {'a':803, 'b':9324, 'c':901}])
        tester.test(self.test_ctxtype, self.test_config)

class TestConvertFn(unittest.TestCase):
    def test_fn_gen(self):
        fn = ssr._splpy_convert_tuple(['a', 'b', 'c'])

        self.assertIs(None, fn(None))

        v = tuple()
        self.assertIs(v, fn(v))
        v = (1,2)
        self.assertIs(v, fn(v))

        v = list()
        self.assertEqual(0, len(fn(v)))

        v = [(), (1,2,3), None]
        r = fn(v)
        self.assertEqual(3, len(r))
        self.assertIs(v[0], r[0])
        self.assertIs(v[1], r[1])
        self.assertIs(v[2], r[2])

        v = dict()
        self.assertEqual((None, None, None), fn(v))
        v = {'a':45, 'b':'BB'}
        self.assertEqual((45, 'BB', None), fn(v))
        v = {'c':92, 'b':'BBB'}
        self.assertEqual((None, 'BBB', 92), fn(v))
        v = {'c':99, 'b':'BBBB', 'a':98}
        self.assertEqual((98, 'BBBB', 99), fn(v))

        v = {'d':99, 'b':'BBBB', 'a':98}
        self.assertEqual((98, 'BBBB', None), fn(v))

        lv = [v]
        r = fn(lv)
        self.assertEqual(1, len(r))
        self.assertEqual((98, 'BBBB', None), r[0])

        lv.append(None)
        lv.append((8,9,10))
        r = fn(lv)
        self.assertEqual(3, len(r))
        self.assertEqual((98, 'BBBB', None), r[0])
        self.assertEqual(None, r[1])
        self.assertEqual((8, 9, 10), r[2])
        
      
