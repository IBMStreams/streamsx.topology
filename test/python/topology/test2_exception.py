# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2018
from future.builtins import *
import unittest
import sys
import itertools
import tempfile
import os
import uuid

if sys.version_info.major == 3:
    unicode = str

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
            fp.write(unicode(txt))
            fp.write(unicode('\n'))
            fp.flush()

class ExcOnEnter(EnterExit):
    def __enter__(self):
        super(ExcOnEnter,self).__enter__()
        raise ValueError('INTENTIONAL ERROR: __enter__ has failed!')


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
        return d['INTENTIONAL ERROR: notthere']

class BadSource(EnterExit):
    def __call__(self):
        d = {}
        return d['INTENTIONAL ERROR: notthere']

class BadSourceIter(EnterExit):
    def __call__(self):
        return self

    def __iter__(self):
        raise UnicodeError("INTENTIONAL ERROR: Bad source __iter__")

class BadSourceNext(EnterExit):
    def __call__(self):
        return self

    def __iter__(self):
        return self

    def __next__(self):
        raise IndexError("INTENTIONAL ERROR: Bad source __next__")

class TestBaseExceptions(unittest.TestCase):
    """ Test exceptions in callables
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        self.tf = _create_tf()
        Tester.setup_standalone(self)

    def tearDown(self):
        if self.tf:
            os.remove(self.tf)

    def _result(self, n):
        with open(self.tf) as fp:
            content = fp.readlines()
        self.assertTrue(len(content) >=3, msg=str(content))
        self.assertEqual('CREATE\n', content[0])
        self.assertEqual('__init__\n', content[1])
        self.assertEqual('__enter__\n', content[2])
        self.assertEqual(n, len(content), msg=str(content))
        return content

class TestExceptions(TestBaseExceptions):

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

    def _run_app(self, fn=None, data=None):
        topo = Topology('TE' + str(uuid.uuid4().hex))
        if data is None:
            data = [1,2,3]
        se = topo.source(data)

        if fn is not None:
            se = fn(se)

        tester = Tester(topo)
        tester.run_for(3)
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

    def test_exc_on_enter_source(self):
        """Test exception on enter.
        """
        self._run_app(data=ExcOnEnter(self.tf))

        self._result(3)

    def test_exc_on_bad_call_source(self):
        """Test exception in __call__
           This is the __call__ that sets up the iterator
        """
        self._run_app(data=BadSource(self.tf))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

    def test_exc_on_bad_iter_source(self):
        """Test exception in __iter__
        """
        self._run_app(data=BadSourceIter(self.tf))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('UnicodeError\n', content[4])

    def test_exc_on_bad_next_source(self):
        """Test exception in __iter__
        """
        self._run_app(data=BadSourceNext(self.tf))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('IndexError\n', content[4])

    def test_exc_on_enter_aggregate(self):
        """Test exception on enter.
        """
        self._run_app(lambda se : se.last(10).aggregate(ExcOnEnter(self.tf)))

        self._result(3)

    def test_exc_on_bad_call_aggregate(self):
        """Test exception in __call__
        """
        self._run_app(lambda se :
            se.last(10).aggregate(BadCall(self.tf)))

        content = self._result(5)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('KeyError\n', content[4])

class SuppressSourceCall(EnterExit):
    def __call__(self):
        raise ValueError("INTENTIONAL ERROR: Error setting up iterable")

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressSourceCall, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError

class SuppressSourceIter(EnterExit):
    def __call__(self):
        return self
    def __iter__(self):
        raise ValueError("INTENTIONAL ERROR: Error setting up iterable")

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressSourceIter, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError

class SuppressSourceNext(EnterExit):
    def __call__(self):
        return self
    def __iter__(self):
        self.count = 3
        return self
    def __next__(self):
        self.count += 1
        if self.count == 5:
            raise ValueError("INTENTIONAL ERROR: Skip 5!")
        if self.count == 7:
            raise StopIteration()
        return self.count

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressSourceNext, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError

class SuppressMapCall(EnterExit):
    def __call__(self, t):
        if t == 2:
            raise ValueError("INTENTIONAL ERROR: Skip 2")
        return t

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressMapCall, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError

class SuppressFlatMapCall(EnterExit):
    def __call__(self, t):
        if t == 2:
            raise ValueError("INTENTIONAL ERROR: Skip 2")
        return [t, t]

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressFlatMapCall, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError


class SuppressFilterCall(EnterExit):
    def __call__(self, t):
        if t != 2:
            raise ValueError("INTENTIONAL ERROR: Skip everything but 2")
        return t

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressFilterCall, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError


class SuppressForEach(EnterExit):
    def __call__(self, t):
        if t == 1:
            raise ValueError("INTENTIONAL ERROR: Skip 1")
        return t

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressForEach, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError

class SuppressHash(EnterExit):
    def __call__(self, t):
        if t == 3:
            raise ValueError("INTENTIONAL ERROR: Skip 3")
        return hash(t)

    def __exit__(self, exc_type, exc_value, traceback):
        super(SuppressHash, self).__exit__(exc_type, exc_value, traceback)
        return exc_type == ValueError

class TestSuppressExceptions(TestBaseExceptions):
    """ Test exception suppression in callables
    """
    def _run_app(self, fn=None, data=None, n=None, e=None):
        topo = Topology('TSE' + str(uuid.uuid4().hex))
        if data is None:
            data = [1,2,3]
        se = topo.source(data)

        if fn is not None:
            se = fn(se)

        tester = Tester(topo)
        if n is not None:
            tester.tuple_count(se, n)
        if e is not None:
            tester.contents(se, e)
        tester.run_for(3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_exc_on_call_source(self):
        """Ignore exception on __call__.
        Effectively since we've been told to ignore the __call__
        exception we have no data source so we create an empty stream.
        """
        self._run_app(data=SuppressSourceCall(self.tf), n=0)
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_exc_on_iter_source(self):
        """Ignore exception on __iter__.
        Effectively since we've been told to ignore the __iter__
        exception we have no data source so we create an empty stream.
        """
        self._run_app(data=SuppressSourceIter(self.tf), n=0)
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_exc_on_next_source(self):
        """Ignore exception on __next__.
        Ignore that step of the iteration.
        """
        self._run_app(data=SuppressSourceNext(self.tf), n=2, e=[4,6])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_exc_on_call_map(self):
        """Ignore exception on __call__.
        Ignore the tuple.
        """
        self._run_app(fn= lambda se : se.map(SuppressMapCall(self.tf)), n=2, e=[1,3])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_exc_on_call_filter(self):
        """Ignore exception on __call__.
        Ignore the tuple.
        """
        self._run_app(fn= lambda se : se.map(SuppressFilterCall(self.tf)), n=1, e=[2])
        content = self._result(8)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])
        self.assertEqual('ValueError\n', content[6])
        self.assertEqual('__exit__\n', content[7])

    def test_exc_on_call_flat_map(self):
        """Ignore exception on __call__.
        Ignore the tuple.
        """
        self._run_app(fn= lambda se : se.flat_map(SuppressFlatMapCall(self.tf)), n=4, e=[1,1,3,3])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_exc_on_call_for_each(self):
        """Ignore exception on __call__.
        Ignore the tuple.
        """
        self._run_app(lambda se : se.for_each(SuppressForEach(self.tf)))
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

    def test_exc_on_call_hash(self):
        """Ignore exception on __call__.
        Ignore the tuple.
        """
        self._run_app(lambda se :
            se.parallel(1, routing=Routing.HASH_PARTITIONED, func=SuppressHash(self.tf)).filter(lambda x : True).end_parallel(), n=2, e=[1,2])
        content = self._result(6)
        self.assertEqual('__exit__\n', content[3])
        self.assertEqual('ValueError\n', content[4])
        self.assertEqual('__exit__\n', content[5])

