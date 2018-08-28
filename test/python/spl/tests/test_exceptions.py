# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
import tempfile
import os
import uuid

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import streamsx.scripts.extract
import streamsx.spl.toolkit
import streamsx.spl.op as op

import spl_tests_utils as stu

def _create_tf():
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write("CREATE\n".encode('utf-8'))
        fp.flush()
        return fp.name


class TestBaseExceptions(unittest.TestCase):
    """ Test exceptions in callables
    """
    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

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
    _multiprocess_can_split_ = False

    def _run_app(self, kind, opi='M'):
        schema = 'tuple<rstring a, int32 b>'
        topo = Topology('TESPL' + str(uuid.uuid4().hex))
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        if opi == 'M':
            data = [1,2,3]
            se = topo.source(data)
            se = se.map(lambda x : {'a':'hello', 'b':x} , schema=schema)
            prim = op.Map(
                "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
                se, params={'tf':self.tf})

            res = prim.stream
        elif opi == 'S':
            prim = op.Source(
                topo,
                "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
                schema=schema, params={'tf':self.tf})
            res = prim.stream
        elif opi == 'E':
            data = [1,2,3]
            se = topo.source(data)
            se = se.map(lambda x : {'a':'hello', 'b':x} , schema=schema)
            prim = op.Sink(
                "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
                se, params={'tf':self.tf})
            res = None

        tester = Tester(topo)
        tester.run_for(3)
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

    def test_exc_enter_map(self):
        self._run_app('ExcEnterMap')
        self._result(3)

    def test_exc_call_map(self):
        self._run_app('ExcCallMap')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_enter_for_each(self):
        self._run_app('ExcEnterForEach', opi='E')
        self._result(3)

    def test_exc_call_for_each(self):
        self._run_app('ExcCallForEach', opi='E')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_enter_source(self):
        self._run_app('ExcEnterSource', opi='S')
        self._result(3)

    def test_exc_iter_source(self):
        self._run_app('ExcIterSource', opi='S')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_next_source(self):
        self._run_app('ExcNextSource', opi='S')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_enter_primitive(self):
        self._run_app('ExcEnterPrimitive', opi='S')
        self._result(3)

    def test_exc_all_ports_ready_primitive(self):
        self._run_app('ExcAllPortsReadyPrimitive', opi='S')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_input_primitive(self):
        self._run_app('ExcInputPrimitive')
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

class TestSuppressExceptions(TestBaseExceptions):
    _multiprocess_can_split_ = False

    def _run_app(self, kind, e, opi='M'):
        schema = 'tuple<rstring a, int32 b>'
        topo = Topology('TSESPL' + str(uuid.uuid4().hex))
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        if opi == 'M':
            data = [1,2,3]
            se = topo.source(data)
            se = se.map(lambda x : {'a':'hello', 'b':x} , schema=schema)
            prim = op.Map(
                "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
                se, params={'tf':self.tf})

            res = prim.stream
        elif opi == 'S':
            prim = op.Source(
                topo,
                "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
                schema=schema, params={'tf':self.tf})
            res = prim.stream
        elif opi == 'E':
            data = [1,2,3]
            se = topo.source(data)
            se = se.map(lambda x : {'a':'hello', 'b':x} , schema=schema)
            prim = op.Sink(
                "com.ibm.streamsx.topology.pytest.pyexceptions::" + kind,
                se, params={'tf':self.tf})
            res = None
    
        tester = Tester(topo)
        if res is not None:
            tester.tuple_count(res, len(e))
            if e:
                tester.contents(res, e)
        else:
            tester.run_for(5)
        tester.test(self.test_ctxtype, self.test_config)

    def test_suppress_filter(self):
        self._run_app('SuppressFilter',
            [{'a':'hello', 'b':1}, {'a':'hello', 'b':3}])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_suppress_map(self):
        self._run_app('SuppressMap',
            [{'a':'helloSM', 'b':8}, {'a':'helloSM', 'b':10}])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_suppress_for_each(self):
        self._run_app('SuppressForEach', None, opi='E')
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_suppress_iter_source(self):
        self._run_app('SuppressIterSource', [], opi='S')
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_suppress_next_source(self):
        self._run_app('SuppressNextSource',
            [{'a':'helloSS', 'b':1}, {'a':'helloSS', 'b':3}], opi='S')
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_suppress_all_ports_ready_primitive(self):
        self._run_app('SuppressAllPortsReadyPrimitive',
            [{'a':'helloAPR', 'b':5}, {'a':'helloAPR', 'b':6},  {'a':'helloAPR', 'b':7}])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_suppress_input_primitive(self):
        self._run_app('SuppressInputPrimitive',
            [{'a':'helloAPR', 'b':5}, {'a':'helloAPR', 'b':7}])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

class TestSuppressMetric(TestBaseExceptions):
    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        self.tf = None
        Tester.setup_distributed(self)

    def test_suppress_metric(self):
        schema = 'tuple<int32 a, int32 b, int32 c, int32 d>'
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        # no metric
        st = op.Source(topo,
            kind='com.ibm.streamsx.topology.pytest.pysource::SparseTuple',
            schema=schema,
            name='NOMETRIC_ST')
        sf = op.Source(topo,
            kind='com.ibm.streamsx.topology.pysamples.sources::Range37',
            schema=schema,
            name='NOMETRIC_SF')
        s = st.stream.union({sf.stream})

        sm = op.Map('com.ibm.streamsx.topology.pysamples.positional::Noop',
            s, name='NOMETRIC_MF')
        sm = op.Map('com.ibm.streamsx.topology.pysamples.positional::AddSeq',
            sm.stream, name='NOMETRIC_MC')

        # With metric
        schema = 'tuple<rstring a, int32 b>'
        ms = op.Source(topo,
            kind='com.ibm.streamsx.topology.pytest.pyexceptions::SuppressNextSource',
            schema=schema,
            name='HASMETRIC_S_1')

        mm = op.Map(
            kind='com.ibm.streamsx.topology.pytest.pyexceptions::SuppressMap',
            stream=ms.stream,
            name='HASMETRIC_M_0')
        mf = op.Map(
            kind='com.ibm.streamsx.topology.pytest.pyexceptions::SuppressFilter',
            stream=ms.stream,
            name='HASMETRIC_F_0')

        self.tester = Tester(topo)
        self.tester.local_check = self.check_suppress_metric
        self.tester.tuple_count(sm.stream, 38)
        self.tester.tuple_count(ms.stream, 2)
        self.tester.tuple_count(mm.stream, 2)
        self.tester.tuple_count(mf.stream, 2)
        self.tester.test(self.test_ctxtype, self.test_config)
            
    def check_suppress_metric(self):
        job = self.tester.submission_result.job
        hms = []
        for op in job.get_operators():
            seen_suppress_metric = False
            for m in op.get_metrics():
                if m.name == 'nExceptionsSuppressed':
                    seen_suppress_metric = True
            if 'NOMETRIC_' in op.name:
                self.assertFalse(seen_suppress_metric, msg=op.name)
            elif 'HASMETRIC_' in op.name:
                hms.append(op)
                self.assertTrue(seen_suppress_metric, msg=op.name)

        self.assertEqual(3, len(hms))
        for _ in range(10):
            ok = True
            for op in hms:
                exp = int(op.name.split('_')[2])
                m = op.get_metrics(name='nExceptionsSuppressed')[0]
                if m.value != exp:
                    ok = False
            if ok:
                break
            time.sleep(2)
        for op in hms:
            exp = int(op.name.split('_')[2])
            m = op.get_metrics(name='nExceptionsSuppressed')[0]
            self.assertEqual(exp, m.value, msg=op.name)

class TestSuppressMetricService(TestSuppressMetric):
    def setUp(self):
        self.tf = None
        Tester.setup_streaming_analytics(self, force_remote_build=True)
