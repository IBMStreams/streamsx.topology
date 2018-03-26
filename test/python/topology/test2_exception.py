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

def _create_tf():
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write("CREATE\n".encode('utf-8'))
        fp.flush()
        return fp.name


class EnterExit(object):
    def __init__(self, tf):
        self.tf = tf
        self._report('__init__')
    def __enter__(self):
        self._report('__enter__')
    def __exit__(self, exc_type, exc_value, traceback):
        self._report('__exit__')
        if exc_type:
            self._report(exc_type.__name__)
    def __call__(self, t):
        return t
    def _report(self, txt):
        with open(self.tf, 'a') as fp:
            fp.write(txt)
            fp.write('\n')
            fp.flush()

class ExcOnEnter(EnterExit):
    def __enter__(self):
        super(ExcOnEnter,self).__enter__()
        raise ValueError('__enter__ has failed!')


class BadData(EnterExit):
    def __call__(self, t):
        return {'a':'A' + str(t)}

class BadHash(EnterExit):
    def __call__(self, t):
        return 'A'

class BadDataFlatMap(EnterExit):
    def __call__(self, t):
        return [{'a':'A' + str(t)}]

class BadCall(EnterExit):
    def __call__(self, t):
        d = {}
        return d['notthere']

class TestExceptions(unittest.TestCase):
    """ Test exceptions in callables
    """
    def setUp(self):
        self.tf = None
        Tester.setup_standalone(self)
        self.tf = _create_tf()

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

    def test_context_mgr_ok(self):
        try:
            with EnterExit(self.tf) as x:
                pass
        except ValueError:
            pass
        content = self._result(4)
        self.assertEqual('__exit__\n', content[3])

    def test_context_mgr_enter_raise(self):
        try:
            with ExcOnEnter(self.tf) as x:
                pass
        except ValueError:
            pass
        self._result(3)

    def test_context_mgr_body_raise(self):
        try:
            with EnterExit(self.tf) as x:
                raise TypeError
        except TypeError:
            pass
        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('TypeError\n', content[4])

    def _run_app(self, fn):
        topo = Topology()
        s = topo.source(range(57))
        se = topo.source([1,2,3])

        se = fn(se)

        tester = Tester(topo)
        tester.tuple_count(s, 57)
        if isinstance(se, Stream):
            tester.tuple_count(se, 0)
        ok = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
        self.assertFalse(ok)

    def test_exc_on_enter_map(self):
        """Test exception on enter.
        """
        self._run_app(lambda se : se.map(ExcOnEnter(self.tf)))

        self._result(3)

    def test_exc_on_data_conversion_map(self):
        """Test exception on enter.
        """
        self._run_app(lambda se :
            se.map(BadData(self.tf), schema='tuple<int32 a>'))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('TypeError\n', content[4])

    def test_exc_on_bad_call_map(self):
        """Test exception in __call__
        """
        self._run_app(lambda se :
            se.map(BadCall(self.tf), schema='tuple<int32 a>'))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_on_enter_flat_map(self):
        """Test exception on enter.
        """
        self._run_app(lambda se : se.flat_map(ExcOnEnter(self.tf)))

        self._result(3)

    def test_exc_on_bad_call_flat_map(self):
        """Test exception in __call__
        """
        self._run_app(lambda se :
            se.flat_map(BadCall(self.tf)))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_on_enter_filter(self):
        """Test exception on enter.
        """
        self._run_app(lambda se :
            se.filter(ExcOnEnter(self.tf)))

        self._result(3)

    def test_exc_on_bad_call_filter(self):
        """Test exception in __call__
        """
        self._run_app(lambda se :
            se.filter(BadCall(self.tf)))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_on_enter_for_each(self):
        """Test exception on enter.
        """
        self._run_app(lambda se : se.for_each(ExcOnEnter(self.tf)))

        self._result(3)

    def test_exc_on_bad_call_for_each(self):
        """Test exception in __call__
        """
        self._run_app(lambda se :
            se.for_each(BadCall(self.tf)))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_on_enter_hash(self):
        """Test exception on enter.
        """
        self._run_app(lambda se :
            se.parallel(1, routing=Routing.HASH_PARTITIONED, func=ExcOnEnter(self.tf)))

        self._result(3)

    def test_exc_on_bad_call_hash(self):
        """Test exception in __call__
        """
        self._run_app(lambda se :
            se.parallel(1, routing=Routing.HASH_PARTITIONED, func=BadCall(self.tf)))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_on_data_conversion_hash(self):
        """Test exception on enter.
        """
        self._run_app(lambda se :
            se.parallel(1, routing=Routing.HASH_PARTITIONED, func=BadHash(self.tf)))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('TypeError\n', content[4])
