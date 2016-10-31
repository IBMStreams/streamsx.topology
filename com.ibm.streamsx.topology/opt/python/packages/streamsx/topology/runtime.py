# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import os
import pickle

try:
    import dill
except ImportError:
    dill = pickle

import base64
import sys
import json

def __splpy_addDirToPath(dir):
    if os.path.isdir(dir):
        if dir not in sys.path:
            #print ("Adding dir to sys.path", dir)
            sys.path.append(dir)
                
def setupOperator(dir):
    pydir = os.path.join(dir, 'opt', 'python')
    __splpy_addDirToPath(os.path.join(pydir, 'modules'))
    __splpy_addDirToPath(os.path.join(pydir, 'packages'))
    #print("sys.path", sys.path)

def pickleReturn(function) :
    def _pickleReturn(v):
        return pickle.dumps(function(v))
    return _pickleReturn

# Given a callable 'callable', return a function
# that depickles the input and then calls 'callable'
# returning the callable's return
# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in(callable) :
    ac = _getCallable(callable)
    def _wf(v):
        return ac(pickle.loads(v))
    return _wf

# Given a callable 'callable', return a function
# that loads an object from the serialized JSON input
# and then calls 'callable' returning the callable's return
def json_in(callable) :
    ac = _getCallable(callable)
    def _wf(v):
        return ac(json.loads(v))
    return _wf

def string_in(callable) :
    ac = _getCallable(callable)
    def _wf(v):
        return ac(v)
    return _wf


# Given a callable 'callable', return a function
# that calls 'callable' with a python dictionary object 
# form of an spltuple returning the callable's return
def dict_in(callable) :
    ac = _getCallable(callable)
    def _wf(v):
        return ac(v)
    return _wf

# Get the callable from the value
# passed into the SPL PyFunction operator.
#
# It is either something that is callable
# and is used directly or is string
# that is a encoded pickled class instance
#
# TODO throw exception
def _getCallable(f):
    if callable(f):
        return f
    if isinstance(f, str):
        ci = dill.loads(base64.b64decode(f))
        if callable(ci):
            return ci
    return None

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
    ac = _getCallable(callable)
    def _wf(v):
        rv = ac(pickle.loads(v))
        if rv is None:
            return None
        return pickle.dumps(rv)
    return _wf

def json_in__pickle_out(callable):
    ac = _getCallable(callable)
    def _wf(v):
        rv = ac(json.loads(v))
        if rv is None:
            return None
        return pickle.dumps(rv)
    return _wf

def string_in__pickle_out(callable):
    return object_in__pickle_out(callable)

def dict_in__pickle_out(callable):
    return object_in__pickle_out(callable)

def dict_in__pickle_out(callable):
    return object_in__pickle_out(callable)

def object_in__pickle_out(callable):
    ac = _getCallable(callable)
    def _wf(v):
        rv = ac(v)
        if rv is None:
            return None
        return pickle.dumps(rv)
    return _wf

##################################################

##
##  {pickle} ->  {json,string}
##

# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in__json_out(callable):
    ac = _getCallable(callable)
    def _wf(v):
        rv = ac(pickle.loads(v))
        if rv is None:
            return None
        jrv = json.dumps(rv, ensure_ascii=False)
        return jrv
    return _wf

def pickle_in__string_out(callable):
    ac = _getCallable(callable)
    def _wf(v):
        rv = ac(pickle.loads(v))
        if rv is None:
            return None
        return str(rv)
    return _wf

# Given a function that returns an iterable
# return a function that can be called
# repeatably by a source operator returning
# the next tuple in its pickled form
def iterableSource(callable) :
  ac = _getCallable(callable)
  iterator = iter(ac())
  def _wf():
     try:
        while True:
            tuple = next(iterator)
            if not tuple is None:
                return pickle.dumps(tuple)
     except StopIteration:
       return None
  return _wf

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

# The returned function must not maintain a reference
# to the passed in value as it will be a memory view
# object with memory that will become invalid after the call.
def pickle_in__pickle_iter(callable):
    ac =_getCallable(callable)
    def _wf(v):
        irv = ac(pickle.loads(v))
        if irv is None:
            return None
        return _PickleIterator(irv)
    return _wf

def json_in__pickle_iter(callable):
    ac =_getCallable(callable)
    def _wf(v):
        irv = ac(json.loads(v))
        if irv is None:
            return None
        return _PickleIterator(irv)
    return _wf

def string_in__pickle_iter(callable):
    ac =_getCallable(callable)
    def _wf(v):
        irv = ac(v)
        if irv is None:
            return None
        return _PickleIterator(irv)
    return _wf

def dict_in__pickle_iter(callable):
    ac =_getCallable(callable)
    def _wf(v):
        irv = ac(v)
        if irv is None:
            return None
        return _PickleIterator(irv)
    return _wf

