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
    def __call__(self, *t):
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

class ExcOnCall(EnterExit):
    def __call__(self, *t):
        d = {}
        return d['notthere']


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
            raise ValueError('Skip 2')
        return True

    def __enter__(self):
        EnterExit.__enter__(self)
    
    def __exit__(self, exc_type, exc_value, traceback):
        EnterExit.__exit__(self, exc_type, exc_value, traceback)
        return exc_type == ValueError

