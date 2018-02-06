# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import unicode_literals
from future.builtins import *

import os
import sys
import pickle
from past.builtins import basestring

import streamsx.ec as ec
from streamsx.topology.schema import StreamSchema

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
                
def add_output_packages(out_dir):
    py_dir = os.path.join(out_dir, 'etc', 'streamsx.topology', 'python')
    vdir = 'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)
    site_pkg = os.path.join(py_dir, 'lib', vdir, 'site-packages')
    __splpy_addDirToPath(site_pkg)

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
    raise TypeError("Class is not callable" + str(type(ci)))

def _verify_tuple(tuple_, attributes):
    if isinstance(tuple_, tuple) or tuple_ is None:
        return tuple_

    if isinstance(tuple_, dict):
        return tuple(tuple_.get(name, None) for name in attributes)
   
    raise TypeError("Function must return a tuple, dict or None:" + str(type(tuple_)))

import inspect
class _FunctionalCallable(object):
    def __init__(self, callable_, attributes=None):
        self._callable = _get_callable(callable_)
        self._cls = False
        self._attributes = attributes

        if callable_ is not self._callable:
            is_cls = not inspect.isfunction(self._callable)
            is_cls = is_cls and ( not inspect.isbuiltin(self._callable) )
            is_cls = is_cls and (not inspect.isclass(self._callable))
            
            if is_cls:
                if ec._is_supported():
                    self._callable._streamsx_ec_op = ec._get_opc(self._callable)
                self._cls = True
                ec._callable_enter(self._callable)

        ec._clear_opc()

    def __call__(self, tuple_):
        """Default callable implementation
        Just calls the callable directly.
        """
        return self._callable(tuple_)

    def _splpy_shutdown(self):
        if self._cls:
            ec._callable_exit_clean(self._callable)

class _PickleInObjectOut(_FunctionalCallable):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        return self._callable(tuple_)

class _PickleInPickleOut(_FunctionalCallable):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        rv =  self._callable(tuple_)
        if rv is None:
            return None
        return pickle.dumps(rv)

class _PickleInJSONOut(_FunctionalCallable):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        rv =  self._callable(tuple_)
        return _json_object_out(rv)

class _PickleInStringOut(_FunctionalCallable):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        rv =  self._callable(tuple_)
        if rv is None:
            return None
        return str(rv)

class _PickleInTupleOut(_FunctionalCallable):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        rv =  self._callable(tuple_)
        return _verify_tuple(rv, self._attributes)

class _ObjectInTupleOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(tuple_)
        return _verify_tuple(rv, self._attributes)

class _ObjectInPickleOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(tuple_)
        if rv is None:
            return None
        return pickle.dumps(rv)


class _ObjectInStringOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(tuple_)
        if rv is None:
            return None
        return str(rv)


class _ObjectInJSONOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(tuple_)
        return _json_object_out(rv)


class _JSONInObjectOut(_FunctionalCallable):
    def __call__(self, tuple_):
        return self._callable(json.loads(tuple_))


class _JSONInPickleOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(json.loads(tuple_))
        if rv is None:
            return None
        return pickle.dumps(rv)


class _JSONInStringOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(json.loads(tuple_))
        if rv is None:
            return None
        return str(rv)


class _JSONInTupleOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv = self._callable(json.loads(tuple_))
        return _verify_tuple(rv, self._attributes)


class _JSONInJSONOut(_FunctionalCallable):
    def __call__(self, tuple_):
        rv = self._callable(json.loads(tuple_))
        return _json_object_out(rv)

##
## Set of functions that wrap the application's Python callable
## with a function that correctly handles the input and output
## (return) value. The input is from the SPL operator, i.e.
## a value obtained from a tuple (attribute) as a Python object.
## The output is the value (as a Python object) to be returned
## to the SPL operator to be set as a tuple (attribute).
##
## The style is one of:
##
## pickle - Object is a Python byte string representing a picked object.
##          The object is depicked/pickled before being passed to/return from
##          the application callable.
##          he returned function must not maintain a reference
##          to the passed in value as it will be a memory view
##          object with memory that will become invalid after the call.
##
## json - Object is a Python unicode string representing a serialized
##          Json object. The object is deserialized/serialized before
##          being passed to/return from the application callable.
##
## string - Object is a Python unicode string representing a string
##          to be passed directly to the Python application callable.
##          For output the function return is converted to a unicode
##          string using str(value).
##
## dict - Object is a Python dictionary object
##          to be passed directly to the Python application function.
##          For output the function return is expecting a Python
##          tuple with the values in the correct order for the
##          the SPL schema or a dict that will be mapped to a tuple.
##          Missing values (not enough fields in the Python tuple
##          or set to None are set the the SPL attribute type default.
##          Really for output 'dict' means structured schema and the
##          classes use 'TupleOut' as they return a Python tuple to
##          the primitive operators.
##
## object - Object is a Python object passed directly into/ from the callable
##          Used when passing by ref. In addition since from the Python
##          point of view string and dict need no transformations
##          they are mapped to the object versions, e.g.
##          string_in == dict_in == object_in
##
## tuple - Object is an SPL schema passed as a regular Python tuple. The
##         order of the Python tuple values matches the order of
##         attributes in the schema. Upon return a tuple is expected
##         like the dict style.
##

## The wrapper functions also ensure the correct context is set up for streamsx.ec
## and the __enter__/__exit__ methods are called.

## The core functionality of the wrapper functions are implemented as classes
## with the input_style__output_style (e.g. string_in__json_out) are fields
## set to the correct class objcet. The class object is called with the application
## callable and a function the SPL operator will call is returned.


# Given a callable that returns an iterable
# return a function that can be called
# repeatably by a source operator returning
# the next tuple in its pickled form
class _IterablePickleOut(_FunctionalCallable):
    def __init__(self, callable, attributes=None):
        super(_IterablePickleOut, self).__init__(callable, attributes)
        self._it = iter(self._callable())

    def __call__(self):
        try:
            while True:
                tuple_ = next(self._it)
                if not tuple_ is None:
                    return pickle.dumps(tuple_)
        except StopIteration:
            return None

class _IterableObjectOut(_FunctionalCallable):
    def __init__(self, callable, attributes=None):
        super(_IterableObjectOut, self).__init__(callable, attributes)
        self._it = iter(self._callable())

    def __call__(self):
        try:
            while True:
                tuple_ = next(self._it)
                if not tuple_ is None:
                    return tuple_
        except StopIteration:
            return None

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
    def __call__(self, tuple_):
        rv =  self._callable(tuple_)
        if rv is None:
            return None
        return _PickleIterator(rv)


class _ObjectInObjectIter(_FunctionalCallable):
    def __call__(self, tuple_):
        rv =  self._callable(tuple_)
        if rv is None:
            return None
        return _ObjectIterator(rv)


class _PickleInPickleIter(_ObjectInPickleIter):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        return super(_PickleInPickleIter, self).__call__(tuple_)


class _PickleInObjectIter(_ObjectInObjectIter):
    def __call__(self, tuple_, pm=None):
        if pm is not None:
            tuple_ = pickle.loads(tuple_)
        return super(_PickleInObjectIter, self).__call__(tuple_)


class _JSONInPickleIter(_ObjectInPickleIter):
    def __call__(self, tuple_):
        return super(_JSONInPickleIter, self).__call__(json.loads(tuple_))


class _JSONInObjectIter(_ObjectInObjectIter):
    def __call__(self, tuple_):
        return super(_JSONInObjectIter, self).__call__(json.loads(tuple_))


# Variables used by SPL Python operators to create specific wrapper function.
#
# Source: source_style
# Filter: style_in__style_out (output style is same as input) - (any input style supported)
# Map: style_in__style_out (any input/output style supported)
# FlatMap: style_in__style_iter: (any input style supported, pickle/object on output)
# ForEach: style_in (any style)

source_object = _IterableObjectOut
object_in__object_out = _FunctionalCallable
object_in__object_iter = _ObjectInObjectIter
object_in__pickle_out = _ObjectInPickleOut
object_in__pickle_iter = _ObjectInPickleIter
object_in__json_out = _ObjectInJSONOut
object_in__dict_out = _ObjectInTupleOut
object_in = _FunctionalCallable

source_pickle = _IterablePickleOut
pickle_in__object_out = _PickleInObjectOut
pickle_in__object_iter = _PickleInObjectIter
pickle_in__pickle_out = _PickleInPickleOut
pickle_in__pickle_iter = _PickleInPickleIter
pickle_in__string_out = _PickleInStringOut
pickle_in__json_out = _PickleInJSONOut
pickle_in__dict_out = _PickleInTupleOut
pickle_in = _PickleInObjectOut

string_in__object_out = object_in__object_out
string_in__object_iter = object_in__object_iter
string_in__pickle_out = object_in__pickle_out
string_in__pickle_iter = object_in__pickle_iter
string_in__string_out = object_in__object_out
string_in__json_out = object_in__json_out
string_in__dict_out = object_in__dict_out
string_in = object_in

json_in__object_out = _JSONInObjectOut
json_in__object_iter = _JSONInObjectIter
json_in__pickle_out = _JSONInPickleOut
json_in__pickle_iter = _JSONInPickleIter
json_in__string_out = _JSONInStringOut
json_in__json_out = _JSONInJSONOut
json_in__dict_out = _JSONInTupleOut
json_in = _JSONInObjectOut

dict_in__object_out = object_in__object_out
dict_in__object_iter = object_in__object_iter
dict_in__pickle_out = object_in__pickle_out
dict_in__pickle_iter = object_in__pickle_iter
dict_in__string_out = object_in__object_out
dict_in__json_out = object_in__json_out
dict_in__dict_out = object_in__dict_out
dict_in = object_in

tuple_in__object_out = object_in__object_out
tuple_in__object_iter = object_in__object_iter
tuple_in__pickle_out = object_in__pickle_out
tuple_in__pickle_iter = object_in__pickle_iter
tuple_in__string_out = object_in__object_out
tuple_in__json_out = object_in__json_out
tuple_in__dict_out = object_in__dict_out
tuple_in = object_in

# Get the named tuple class for a schema.
# used by functional operators.
def _get_namedtuple_cls(schema, name):
    return StreamSchema(schema).as_tuple(named=name).style

class _WrappedInstance(object):
    def __init__(self, callable_):
        self._callable = callable_

    def _hasee(self):
        return hasattr(self._callable, '__enter__') and hasattr(self._callable, '__exit__')

    def __enter__(self):
        if self._hasee():
            self._callable.__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._hasee():
            self._callable.__exit__(exc_type, exc_value, traceback)

# Wraps an iterable instance returning
# it when called. Allows an iterable
# instance to be passed directly to Topology.source
class _IterableInstance(_WrappedInstance):
    def __call__(self):
        return self._callable

# Wraps an callable instance 
# When this is called, the callable is called.
# Used to wrap a lambda object or a function/class
# defined in __main__
class _Callable(_WrappedInstance):
    def __call__(self, *args, **kwargs):
        return self._callable.__call__(*args, **kwargs)

