# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
import tempfile
import os

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import streamsx.scripts.extract
import streamsx.spl.toolkit
import streamsx.spl.op as op

def _create_tf():
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write("CREATE\n".encode('utf-8'))
        fp.flush()
        return fp.name


class TestBaseExceptions(unittest.TestCase):
    """ Test exceptions in callables
    """
    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        streamsx.scripts.extract.main(['-i', '../testtkpy', '--make-toolkit'])

    def setUp(self):
        self.tf = _create_tf()
        Tester.setup_standalone(self)

    def tearDown(self):
        if self.tf:
            os.remove(self.tf)

    def _result(self, n):
        with open(self.tf) as fp:
            content = fp.readlines()
        self.assertTrue(len(content) >=3)
        self.assertEqual('CREATE\n', content[0])
        self.assertEqual('__init__\n', content[1])
        self.assertEqual('__enter__\n', content[2])
        self.assertEqual(n, len(content), msg=str(content))
        return content

class TestExceptions(TestBaseExceptions):

    def _run_app(self, kind):
        schema = 'tuple<rstring a, int32 b>'
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, '../testtkpy')
        s = topo.source(range(234))
        data = [1,2,3]
        se = topo.source(data)

        se = se.map(lambda x : {'a':'hello', 'b':x} , schema=schema)

        prim = op.Map(
            "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
            se, params={'tf':self.tf})

        res = prim.stream

        tester = Tester(topo)
        tester.tuple_count(s, 234)
        tester.tuple_count(res, 0)
        ok = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
        self.assertFalse(ok)

    def test_exc_enter_filter(self):
        self._run_app('ExcEnterFilter')
        self._result(3)

    def test_exc_call_filter(self):
        self._run_app('ExcCallFilter')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

class TestSuppressExceptions(TestBaseExceptions):

    def _run_app(self, kind, e):
        schema = 'tuple<rstring a, int32 b>'
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, '../testtkpy')
        data = [1,2,3]
        se = topo.source(data)

        se = se.map(lambda x : {'a':'hello', 'b':x} , schema=schema)

        prim = op.Map(
            "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
            se, params={'tf':self.tf})

        res = prim.stream

        tester = Tester(topo)
        tester.tuple_count(res, len(e))
        tester.contents(res, e)
        tester.test(self.test_ctxtype, self.test_config)

    def test_suppress_filter(self):

        self._run_app('SuppressFilter',
            [{'a':'hello', 'b':1}, {'a':'hello', 'b':3}])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])
