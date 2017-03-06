# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import os
import pickle
from past.builtins import basestring

import streamsx.ec as ec

try:
    import dill
    dill.settings['recurse'] = True
except ImportError:
    dill = pickle

import base64
import sys
import json

def __splpy_addDirToPath(dir):
    if os.path.isdir(dir):
        if dir not in sys.path:
            sys.path.insert(0, dir)
                
def setupOperator(dir):
    pydir = os.path.join(dir, 'opt', 'python')
    __splpy_addDirToPath(os.path.join(pydir, 'modules'))
    __splpy_addDirToPath(os.path.join(pydir, 'packages'))
    #print("sys.path", sys.path)


# Get the callable from the value
# passed into the SPL PyFunction operator.
#
# It is either something that is callable
# and is used directly or is string
# that is a encoded pickled class instance
#
def _get_callable(f):
    if callable(f):
        return f
    if isinstance(f, basestring):
        ci = dill.loads(base64.b64decode(f))
        if callable(ci):
            return ci
    raise TypeError("Class is not callable" + type(ci))

import inspect
class _FunctionalCallable(object):
    def __init__(self, callable):
        self._callable = _get_callable(callable)
        self._cls = False

        if callable is not self._callable:
            is_cls = not inspect.isfunction(self._callable)
            is_cls = is_cls and ( not inspect.isbuiltin(self._callable) )
            is_cls = is_cls and (not inspect.isclass(self._callable))
            
            if is_cls:
                if ec._supported:
                    self._callable._streamsx_ec_op = ec._get_opc(self._callable)
                self._cls = True
                ec._callable_enter(self._callable)

        ec._clear_opc()

    def __call__(self, tuple):
        """Default callable implementation
        Just calls the callable directly.
        """
        return self._callable(tuple)

    def _shutdown(self):
        if self._cls:
            ec._callable_exit_clean(self._callable)

class _PickleInObjectOut(_FunctionalCallable):
    def __call__(self, tuple):
        return self._callable(pickle.loads(tuple))

class _PickleInPickleOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(pickle.loads(tuple))
        if rv is None:
            return None
        return pickle.dumps(rv)

class _PickleInJSONOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(pickle.loads(tuple))
        if rv is None:
            return None
        return json.dumps(rv, ensure_ascii=False)

class _PickleInStringOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(pickle.loads(tuple))
        if rv is None:
            return None
        return str(rv)

class _ObjectInPickleOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return pickle.dumps(rv)

class _JSONInObjectOut(_FunctionalCallable):
    def __call__(self, tuple):
        return self._callable(json.loads(tuple))

class _JSONInPickleOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(json.loads(tuple))
        if rv is None:
            return None
        return pickle.dumps(rv)

# Given a callable 'callable', return a function
# that depickles the input and then calls 'callable'
# returning the callable's return
# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in(callable) :
    return _PickleInObjectOut(callable)

# Given a callable 'callable', return a function
# that loads an object from the serialized JSON input
# and then calls 'callable' returning the callable's return
def json_in(callable) :
    return _JSONInObjectOut(callable)

def string_in(callable) :
    return _FunctionalCallable(callable)

# Given a callable 'callable', return a function
# that calls 'callable' with a python dictionary object 
# form of an spltuple returning the callable's return
def dict_in(callable) :
    return _FunctionalCallable(callable)

##
## Set of functions that wrap the application's Python callable
## with a function that correctly handles the input and output
## (return) value. The input is from the SPL operator, i.e.
## a value obtained from a tuple attribute as a Python object.
## The output is the value (as a Python object) to be returned
## to the SPL operator to be set as a tuple attribute.
##
## The style is one of:
##
## pickle - Object is a Python byte string representing a picked object.
##          The object is depicked/pickled before being passed to/return from
##          the application function.
##
## json - Object is a Python unicode string representing a serialized
##          Json object. The object is deserialized/serialized before
##          being passed to/return from the application function.
##
## string - Object is a Python unicode string representing a string
##          to be passed directly to the Python application function.
##          For output the function return is converted to a unicode
##          string using str(value).
##
## dict - Object is a Python dictionary object
##          to be passed directly to the Python application function.
##          For output the function return is converted to a unicode
##          string using str(value).
##
## object - Object is a Python object passed directly into the function
##          Only used within this file 
##

###
# Currently there are only these cases to handle for transform
#
#  {pickle,json,string} -> {pickle}
#  {pickle} ->  {json,string}
#
#  Typically the use case is {pickle} -> {pickle}
#  The other cases are only introduced with publish or subscribe.
#
###

## 
## {pickle,json,string} -> {pickle}
##

# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in__pickle_out(callable):
    return _PickleInPickleOut(callable)

def json_in__pickle_out(callable):
    return _JSONInPickleOut(callable)

def string_in__pickle_out(callable):
    return _ObjectInPickleOut(callable)

def dict_in__pickle_out(callable):
    return _ObjectInPickleOut(callable)

##################################################

##
##  {pickle} ->  {json,string}
##

# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in__json_out(callable):
    return _PickleInJSONOut(callable)

def pickle_in__string_out(callable):
    return _PickleInStringOut(callable)


class _IterablePickleOut(_FunctionalCallable):
    def __init__(self, callable):
        super(_IterablePickleOut, self).__init__(callable)
        self._it = iter(self._callable())

    def __call__(self):
        try:
            while True:
                tuple = next(self._it)
                if not tuple is None:
                    return pickle.dumps(tuple)
        except StopIteration:
            return None


# Given a function that returns an iterable
# return a function that can be called
# repeatably by a source operator returning
# the next tuple in its pickled form
def iterableSource(callable) :
    return _IterablePickleOut(callable)

# Iterator that wraps another iterator
# to discard any values that are None
# and pickle any returned value.
class _PickleIterator:
   def __init__(self, it):
       self.it = iter(it)
   def __iter__(self):
       return self
   def __next__(self):
       nv = next(self.it)
       while nv is None:
          nv = next(self.it)
       return pickle.dumps(nv)
# python 2.7 uses the next function whereas 
# python 3.x uses __next__ 
   def next(self):
       return self.__next__()

# Return a function that depickles
# the input tuple calls callable
# that is expected to return
# an Iterable. If callable returns
# None then the function will return
# None, otherwise it returns
# an instance of _PickleIterator
# wrapping an iterator from the iterable
# Used by PyFunctionMultiTransform

class _ObjectInPickleIter(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return _PickleIterator(rv)

class _PickleInPickleIter(_ObjectInPickleIter):
    def __call__(self, tuple):
        return super(_PickleInPickleIter, self).__call__(pickle.loads(tuple))

class _JSONInPickleIter(_ObjectInPickleIter):
    def __call__(self, tuple):
        return super(_JSONInPickleIter, self).__call__(json.loads(tuple))

# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in__pickle_iter(callable):
    return _PickleInPickleIter(callable)

def json_in__pickle_iter(callable):
    return _JSONInPickleIter(callable)

def string_in__pickle_iter(callable):
    return _ObjectInPickleIter(callable)

def dict_in__pickle_iter(callable):
    return _ObjectInPickleIter(callable)

