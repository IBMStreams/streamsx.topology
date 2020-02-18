# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

import os
import sys
import pickle
import inspect
import logging
import importlib
import types

import streamsx.ec as ec
from streamsx.topology.schema import StreamSchema
import streamsx._streams._runtime

import dill
# Importing cloudpickle break dill's deserialization.
# Workaround is to make dill aware of the ClassType type.
if not 'dill._dill' in sys.modules:
    sys.modules['dill._dill'] = dill.dill
    dill._dill = dill.dill
dill._dill._reverse_typemap['ClassType'] = type
    
import base64
import json
from pkgutil import extend_path
import streamsx

# Simple identity function used by map, flat_map as default function.
def _identity(tuple_):
    return tuple_

def __splpy_addDirToPath(dir_):
    if os.path.isdir(dir_):
        if dir_ not in sys.path:
            sys.path.insert(0, dir_)
            # In case a streamsx module (e.g. streamsx.bm) 
            # is included in the additional code
            if os.path.isdir(os.path.join(dir_, 'streamsx')):
                streamsx.__path__ = extend_path(streamsx.__path__, streamsx.__name__)
                

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
    if isinstance(f, str):
        ci = dill.loads(base64.b64decode(f))
        if callable(ci):
            return ci
    raise TypeError("Class is not callable" + str(type(ci)))

# Base class used at runtime by all functional operators
# to manage the application's callable.
# For functional operators all of the logic is seen as a callable.
# Specific sub-classes perform conversion on the input and or output values.
#
class _FunctionalCallable(streamsx._streams._runtime._WrapOpLogic):
    def __init__(self, callable_, attributes=None):
        callable_ = _get_callable(callable_)
        super(_FunctionalCallable,  self).__init__(callable_)
        self._attributes = attributes

    def __call__(self, tuple_):
        """Default callable implementation
        Just calls the callable directly.
        """
        return self._callable(tuple_)

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
        return self._callable(tuple_)

class _ObjectInTupleOut(_FunctionalCallable):
    def __call__(self, tuple_):
        return self._callable(tuple_)

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
        return self._callable(json.loads(tuple_))


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
## pickle - Object is a Python byte string representing a pickled object.
##          The object is depickled/pickled before being passed to/return from
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
#
# the next tuple in its pickled form
# Set up iterator from the callable.
# If an error occurs and __exit__ asks for it to be
# ignored then an empty source is created.
class _IterableAnyOut(_FunctionalCallable):
    def __init__(self, callable_, attributes=None):
        super(_IterableAnyOut, self).__init__(callable_, attributes)
        self._it = None

    def _start(self):
        try:
            self._it = iter(self._callable())
        except:
            ei = sys.exc_info()
            ignore = streamsx._streams._runtime._call_exit(self, ei)
            if not ignore:
                raise ei[1]
            # Ignored by nothing to do so use empty iterator
            self._it = iter([])

    def __call__(self):
        if self._it is None:
            self._start()
        return next(self._it)

class _IterablePickleOut(_IterableAnyOut):
    def __init__(self, callable_, attributes=None):
        super(_IterablePickleOut, self).__init__(callable_, attributes)
        self.pdfn = pickle.dumps

    def __call__(self):
        tuple_ = super(_IterablePickleOut, self).__call__()
        if tuple_ is not None:
            return self.pdfn(tuple_)
        return tuple_

class _IterableObjectOut(_IterableAnyOut):
     pass

# Iterator that wraps another iterator
# to discard any values that are None
# This is created at runtime to wrap
# iterators returned by an iterable.
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

source_string = source_object
source_dict = source_object

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

def _inline_modules(fn, modules, constants, inlines):
    cvs = inspect.getclosurevars(fn)
    for mk in cvs.globals.keys():
        gv = cvs.globals[mk]
        if isinstance(gv, types.ModuleType):
            modules[mk] = gv.__name__
            continue
        elif hasattr(gv, '__module__') and gv.__module__ != '__main__' and hasattr(gv, '__name__'):
            modules[mk] = gv.__name__, gv.__module__
            continue
        elif type(gv) == str or type(gv) == int or type(gv) == float or type(gv) == bool or type(gv) == bytes or type(gv) == complex:
            constants[mk] = gv
            continue
        elif hasattr(gv, '__module__') and gv.__module__ == '__main__':
            if inspect.isroutine(gv):
                if mk not in inlines:
                    inlines[mk] = gv
                    _inline_modules(gv, modules, constants, inlines)
                continue
            elif type(gv) == type:
                if mk not in inlines:
                    inlines[mk] = gv
                    for xm in inspect.getmembers(gv, inspect.ismethod):
                        _inline_modules(xm[1], modules, constants, inlines)
                continue
 
        raise TypeError("Unsupported global closure {} type {} in {}".format(mk, gv, fn))
          

# Wraps an callable instance 
# When this is called, the callable is called.
# Used to wrap a lambda object or a function/class
# defined in __main__
# Also used through _IterableInstance to handle
# any instance passed to source() including those
# defined in __main__ and those not defined in __main__
# Instance of _WrapInstance so used at declaration time
class _ModulesCallable(streamsx._streams._runtime._WrapOpLogic):
    def __init__(self, callable_, no_context=None):
        super(_ModulesCallable, self).__init__(callable_, no_context)
        check_cmm = False
        modules = {}
        constants = {}
        inlines = {}
        if inspect.isroutine(callable_):
            _inline_modules(callable_, modules, constants, inlines)
        elif callable(callable_):
            _inline_modules(callable_.__call__, modules, constants, inlines)
            check_cmm = True
        elif type(callable_).__module__ == '__main__': 
            _inline_modules(callable_.__iter__, modules, constants, inlines)
            # Handle common case the iterable is also the iterator.
            if hasattr(callable_, '__next__'):
                _inline_modules(callable_.__next__, modules, constants, inlines)
            check_cmm = True

        if check_cmm and self._streamsx_ec_context:
            _inline_modules(callable_.__enter__, modules, constants, inlines)
            _inline_modules(callable_.__exit__, modules, constants, inlines)

        self._modules = modules if modules else None
        self._constants = constants if constants else None
        self._inlines = inlines if inlines else None

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

        # Patch the lambda/in-line function's globals
        # to include any modules it references.
        if self._modules or self._constants or self._inlines:
            if inspect.isroutine(self._callable):
                gbls = self._callable.__globals__
            elif callable(self._callable):
                gbls = self._callable.__call__.__globals__
            else:
                gbls = self._callable.__iter__.__globals__

        if self._modules:
            for vn,mn in self._modules.items():
                if vn not in gbls:
                    if isinstance(mn, tuple):
                        cm = importlib.import_module(mn[1])
                        gbls[vn] = getattr(cm, mn[0])
                    else:
                        gbls[vn] = importlib.import_module(mn)

        if self._constants:
            for vn,mn in self._constants.items():
                if vn not in gbls:
                    gbls[vn] = self._constants[vn]

        if self._inlines:
            for vn,d in self._inlines.items():
                if vn not in gbls:
                    gbls[vn] = self._inlines[vn]
            


class _Callable0(_ModulesCallable):
    def __call__(self):
        return self._callable()

class _Callable1(_ModulesCallable):
    def __call__(self, tuple_):
        return self._callable(tuple_)

# Wraps an iterable instance returning
# it when called. Allows an iterable
# instance to be passed directly to Topology.source
# (such as a list)
# Instance of _WrapOpLogic so used at declaration time
class _IterableInstance(_ModulesCallable):
    def __call__(self):
        return self._callable

def _spl_boolean_to_bool(v):
    return not v == 'false'

class _SubmissionParam(object):
    def __init__(self, name, default, type_):
        self._name = name

        if default is not None:
            type_ = type(default)

        if default is None and type_ is None:
            type_ = None
            self._spl_type = 'RSTRING'
        elif isinstance(default, str):
            type_ = None
            self._spl_type = 'RSTRING'
        elif isinstance(default, bool):
            self._spl_type = 'BOOLEAN'
        elif isinstance(default, int):
            if default >= -2147483648 and default <= 2147483647:
                self._spl_type = 'INT32'
            else:
                self._spl_type = 'INT64'
        elif isinstance(default, float):
            self._spl_type = 'FLOAT64'
        elif type_ is str:
            self._spl_type = 'RSTRING'
        elif type_ is int:
            self._spl_type = 'INT32'
        elif type_ is float:
            self._spl_type = 'FLOAT64'
        elif type_ is bool:
            self._spl_type = 'BOOLEAN'
        else:
            raise TypeError("Type {} not supported for submission parameter default value.".format(type_))

        self._type = type_
        self._default = default

    def __call__(self):
        sv =  ec._SUBMIT_PARAMS.get(self._name)
        if sv is not None and self._type is not None:
            if self._type is bool:
                return _spl_boolean_to_bool(sv)
            return self._type(sv)
        return sv

    def spl_json(self):
        o = {'type': 'submissionParameter'}
        v = {'name': self._name}
        o['value'] = v
        v['metaType'] = self._spl_type
        if self._default is not None:
            v['defaultValue'] = self._default
        return o
