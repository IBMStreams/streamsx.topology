# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import os
import sys
import pickle
from past.builtins import basestring

import streamsx.ec as ec

try:
    import dill
    # Importing cloudpickle break dill's deserialization.
    # Workaround is to make dill aware of the ClassType type.
    if sys.version_info.major == 3:
        dill.dill._reverse_typemap['ClassType'] = type
    dill.settings['recurse'] = True
except ImportError:
    dill = pickle

import base64
import json
from pkgutil import extend_path
import streamsx

def __splpy_addDirToPath(dir):
    if os.path.isdir(dir):
        if dir not in sys.path:
            sys.path.insert(0, dir)
            # In case a streamsx module (e.g. streamsx.bm) 
            # is included in the additional code
            if os.path.isdir(os.path.join(dir, 'streamsx')):
                streamsx.__path__ = extend_path(streamsx.__path__, streamsx.__name__)
                
def setupOperator(dir):
    pydir = os.path.join(dir, 'opt', 'python')
    __splpy_addDirToPath(os.path.join(pydir, 'modules'))
    __splpy_addDirToPath(os.path.join(pydir, 'packages'))
    #print("sys.path", sys.path)

def _json_object_out(v):
    """Return a serialized JSON object for a value."""
    if v is None:
        return None
    return json.dumps(v, ensure_ascii=False)

def _json_force_object(v):
    """Force a non-dictionary object to be a JSON dict object"""
    if not isinstance(v, dict):
        v = {'payload': v}
    return v
    
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
                if ec._is_supported():
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
    def __call__(self, tuple, pm=None):
        if pm is not None:
            tuple = pickle.loads(tuple)
        return self._callable(tuple)

class _PickleInPickleOut(_FunctionalCallable):
    def __call__(self, tuple, pm=None):
        if pm is not None:
            tuple = pickle.loads(tuple)
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return pickle.dumps(rv)

class _PickleInJSONOut(_FunctionalCallable):
    def __call__(self, tuple, pm=None):
        if pm is not None:
            tuple = pickle.loads(tuple)
        rv =  self._callable(tuple)
        return _json_object_out(rv)

class _PickleInStringOut(_FunctionalCallable):
    def __call__(self, tuple, pm=None):
        if pm is not None:
            tuple = pickle.loads(tuple)
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return str(rv)

class _ObjectInPickleOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return pickle.dumps(rv)

class _ObjectInStringOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return str(rv)

class _ObjectInJSONOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        return _json_object_out(rv)

class _JSONInObjectOut(_FunctionalCallable):
    def __call__(self, tuple):
        return self._callable(json.loads(tuple))

class _JSONInPickleOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(json.loads(tuple))
        if rv is None:
            return None
        return pickle.dumps(rv)

class _JSONInStringOut(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(json.loads(tuple))
        if rv is None:
            return None
        return str(rv)

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

def json_in__string_out(callable):
    return _JSONInStringOut(callable)

def json_in__object_out(callable):
    return _JSONInObjectOut(callable)

def string_in__pickle_out(callable):
    return _ObjectInPickleOut(callable)

def string_in__object_out(callable):
    return _FunctionalCallable(callable)

def string_in__json_out(callable):
    return _ObjectInJSONOut(callable)

def dict_in__pickle_out(callable):
    return _ObjectInPickleOut(callable)

def dict_in__object_out(callable):
    return _FunctionalCallable(callable)

def dict_in__json_out(callable):
    return _ObjectInJSONOut(callable)

def dict_in__string_out(callable):
    return _ObjectInStringOut(callable)


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

def pickle_in__object_out(callable):
    return _PickleInObjectOut(callable)


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

class _IterableObjectOut(_FunctionalCallable):
    def __init__(self, callable):
        super(_IterableObjectOut, self).__init__(callable)
        self._it = iter(self._callable())

    def __call__(self):
        try:
            while True:
                tuple = next(self._it)
                if not tuple is None:
                    return tuple
        except StopIteration:
            return None

# Given a function that returns an iterable
# return a function that can be called
# repeatably by a source operator returning
# the next tuple in its pickled form
def source_pickle(callable) :
    return _IterablePickleOut(callable)

# Source iterator that returns objects
# when passing by ref
def source_object(callable) :
    return _IterableObjectOut(callable)

# Iterator that wraps another iterator
# to discard any values that are None
class _ObjectIterator(object):
   def __init__(self, it):
       self.it = iter(it)
   def __iter__(self):
       return self
   def __next__(self):
       nv = next(self.it)
       while nv is None:
          nv = next(self.it)
       return nv
# python 2.7 uses the next function whereas 
# python 3.x uses __next__ 
   def next(self):
       return self.__next__()

# and pickle any returned value.
class _PickleIterator(_ObjectIterator):
   def __next__(self):
       return pickle.dumps(super(_PickleIterator, self).__next__())

# Return a function that depickles
# the input tuple calls callable
# that is expected to return
# an Iterable. If callable returns
# None then the function will return
# None, otherwise it returns
# an instance of _PickleIterator
# wrapping an iterator from the iterable
# Used by FlatMap (flat_map)

class _ObjectInPickleIter(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return _PickleIterator(rv)

class _ObjectInObjectIter(_FunctionalCallable):
    def __call__(self, tuple):
        rv =  self._callable(tuple)
        if rv is None:
            return None
        return _ObjectIterator(rv)

class _PickleInPickleIter(_ObjectInPickleIter):
    def __call__(self, tuple, pm=None):
        if pm is not None:
            tuple = pickle.loads(tuple)
        return super(_PickleInPickleIter, self).__call__(tuple)

class _PickleInObjectIter(_ObjectInObjectIter):
    def __call__(self, tuple, pm=None):
        if pm is not None:
            tuple = pickle.loads(tuple)
        return super(_PickleInObjectIter, self).__call__(tuple)

class _JSONInPickleIter(_ObjectInPickleIter):
    def __call__(self, tuple):
        return super(_JSONInPickleIter, self).__call__(json.loads(tuple))

class _JSONInObjectIter(_ObjectInObjectIter):
    def __call__(self, tuple):
        return super(_JSONInObjectIter, self).__call__(json.loads(tuple))

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

# By reference versions
def pickle_in__object_iter(callable):
    return _PickleInObjectIter(callable)

def json_in__object_iter(callable):
    return _JSONInObjectIter(callable)

def string_in__object_iter(callable):
    return _ObjectInObjectIter(callable)

def dict_in__object_iter(callable):
    return _ObjectInObjectIter(callable)
