import os
import pickle
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

def pickleObject(v):
    return pickle.dumps(v)
     
def pickleReturn(function) :
    def _pickleReturn(v):
        return pickleObject(function(v))
    return _pickleReturn

# Given a callable 'callable', return a function
# that depickles the input and then calls 'callable'
# returning the callable's return
def pickle_in(callable) :
    ac = _getCallable(callable)
    def _pickle_in(v):
        return ac(pickle.loads(v))
    return _pickle_in

# Given a callable 'callable', return a function
# that loads an object from the serialized JSON input
# and then calls 'callable' returning the callable's return
def json_in(callable) :
    ac = _getCallable(callable)
    def _json_in(v):
        return ac(json.loads(v))
    return _json_in

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
        ci = pickle.loads(base64.b64decode(f))
        if callable(ci):
            return ci
    return None

# Given a callable 'callable', return a function
# that depickles the input and then calls 'callable'
# returning the callable's return already pickled.
# If the return is None then it is not pickled.
def pickle_in__pickle_out(callable):
    ac = _getCallable(callable)
    def _pickle_in__pickle_out(v):
        rv = ac(pickle.loads(v))
        if rv is None:
            return None
        return pickle.dumps(rv)
    return _pickle_in__pickle_out

# Given a callable 'callable', return a function
# that loads an object from the serialized JSON input
# and then calls 'callable'
# returning the callable's return already pickled.
# If the return is None then it is not pickled.
def json_in__pickle_out(callable):
    ac = _getCallable(callable)
    def _json_in__pickle_out(v):
        rv = ac(json.loads(v))
        if rv is None:
            return None
        return pickle.dumps(rv)
    return _json_in__pickle_out

#
# Pickle to JSON for transform
#
def pickle_in__json_out(callable):
    ac = _getCallable(callable)
    def _pickle_in__json_out(v):
        rv = ac(pickle.loads(v))
        if rv is None:
            return None
        jrv = json.dumps(rv, ensure_ascii=False)
        return jrv
    return _pickle_in__json_out

#
# JSON to JSON for transform
#
def json_in__json_out(callable):
    ac = _getCallable(callable)
    def _json_in__json_out(v):
        rv = ac(json.loads(v))
        if rv is None:
            return None
        jrv = json.dumps(rv, ensure_ascii=False)
        return jrv
    return _json_in__json_out

# Given a function that returns an iterable
# return a function that can be called
# repeatably by a source operator returning
# the next tuple in its pickled form
def iterableSource(callable) :
  ac = _getCallable(callable)
  iterator = iter(ac())
  def _sourceIterator():
     try:
        while True:
            tuple = next(iterator)
            if not tuple is None:
                return pickleObject(tuple)
     except StopIteration:
       return None
  return _sourceIterator

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

# Return a function that depickles
# the input tuple calls callable
# that is expected to return
# an Iterable. If callable returns
# None then the function will return
# None, otherwise it returns
# an instance of _PickleIterator
# wrapping an iterator from the iterable
# Used by PyFunctionMultiTransform
def pickle_in__pickle_iter(callable):
    ac =_getCallable(callable)
    def _pickle_in__pickle_iter(v):
        irv = ac(pickle.loads(v))
        if irv is None:
            return None
        return _PickleIterator(irv)
    return _pickle_in__pickle_iter

def json_in__pickle_iter(callable):
    ac =_getCallable(callable)
    def _json_in__pickle_iter(v):
        irv = ac(json.loads(v))
        if irv is None:
            return None
        return _PickleIterator(irv)
    return _json_in__pickle_iter
