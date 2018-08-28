# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
import tempfile
import os

# Import the SPL decorators
from streamsx.spl import spl
import sys

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pyexceptions"

class EnterExit(object):
    def __init__(self, tf=None):
        self.tf = tf
        self._report('__init__')
    def __enter__(self):
        self._report('__enter__')
    def __exit__(self, exc_type, exc_value, traceback):
        self._report('__exit__')
        if exc_type:
            self._report(exc_type.__name__)
    def __call__(self, *t):
        return t
    def _report(self, txt):
        if not self.tf:
            return
        with open(self.tf, 'a') as fp:
            fp.write(txt)
            fp.write('\n')
            fp.flush()

class ExcOnEnter(EnterExit):
    def __enter__(self):
        super(ExcOnEnter,self).__enter__()
        raise ValueError('INTENTIONAL_ERROR: __enter__ has failed!')

class ExcOnCall(EnterExit):
    def __call__(self, *t):
        d = {}
        return d['INTENTIONAL_ERROR: notthere']


@spl.filter()
class ExcEnterFilter(ExcOnEnter):
    pass


@spl.filter()
class ExcCallFilter(ExcOnCall):
    pass

@spl.filter()
class SuppressFilter(EnterExit):
    def __call__(self, *t):
        if t[1] == 2:
            raise ValueError('INTENTIONAL_ERROR: Skip 2')
        return True

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == ValueError

@spl.map()
class ExcEnterMap(ExcOnEnter):
    pass


@spl.map()
class ExcCallMap(ExcOnCall):
    pass

@spl.map()
class SuppressMap(EnterExit):
    def __call__(self, *t):
        if t[1] == 2:
            raise ValueError('INTENTIONAL_ERROR: Skip 2')
        return t[0]+'SM', t[1]+7

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == ValueError

@spl.for_each()
class ExcEnterForEach(ExcOnEnter):
    def __call__(self, *t):
        pass


@spl.for_each()
class ExcCallForEach(ExcOnCall):
    pass

@spl.for_each()
class SuppressForEach(EnterExit):
    def __call__(self, *t):
        if t[1] == 2:
            raise ValueError('INTENTIONAL_ERROR: Skip 2')
        return None

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == ValueError

@spl.source()
class ExcEnterSource(ExcOnEnter):
    pass


@spl.source()
class ExcIterSource(EnterExit):
    def __iter__(self):
        raise KeyError()


@spl.source()
class ExcNextSource(EnterExit):
    def __iter__(self):
        return self

    def __next__(self):
        raise KeyError('INTENTIONAL_ERROR:')

    # For Python 2
    def next(self):
        return self.__next__()

@spl.source()
class SuppressNextSource(EnterExit):
    def __iter__(self):
        self.count = 0
        return self

    def __next__(self):
        self.count += 1
        if self.count == 2:
           raise ValueError('INTENTIONAL_ERROR: Skip 2!')
        if self.count == 4:
           raise StopIteration()
        return 'helloSS', self.count

    # For Python 2
    def next(self):
        return self.__next__()

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == ValueError

@spl.source()
class SuppressIterSource(EnterExit):
    def __iter__(self):
        raise KeyError('INTENTIONAL_ERROR: Bad Iter')

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == KeyError

@spl.primitive_operator(output_ports=['A'])
class ExcEnterPrimitive(ExcOnEnter):
    pass

@spl.primitive_operator(output_ports=['A'])
class ExcAllPortsReadyPrimitive(spl.PrimitiveOperator, EnterExit):
    def all_ports_ready(self):
        raise KeyError('INTENTIONAL_ERROR: not so ready!!')


@spl.primitive_operator(output_ports=['A'])
class ExcInputPrimitive(spl.PrimitiveOperator, EnterExit):
    @spl.input_port()
    def p1e(self):
        raise KeyError('INTENTIONAL_ERROR: Can not process tuple')


@spl.primitive_operator(output_ports=['A'])
class SuppressAllPortsReadyPrimitive(spl.PrimitiveOperator, EnterExit):
    def all_ports_ready(self):
        raise KeyError('INTENTIONAL_ERROR: not so ready!!')

    @spl.input_port()
    def p1(self, *t):
        self.submit('A', (t[0] + 'APR', t[1] + 4))

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == KeyError

@spl.primitive_operator(output_ports=['A'])
class SuppressInputPrimitive(spl.PrimitiveOperator, EnterExit):
    @spl.input_port()
    def p1(self, *t):
        if t[1] == 2:
            raise ValueError('INTENTIONAL_ERROR: Skip 2!!')
        self.submit('A', (t[0] + 'APR', t[1] + 4))

    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == ValueError
