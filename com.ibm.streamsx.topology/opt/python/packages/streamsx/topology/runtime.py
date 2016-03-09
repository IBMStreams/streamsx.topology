import os
import pickle
import base64
import sys

def __splpy_addDirToPath(dir):
    if os.path.isdir(dir):
        if dir not in sys.path:
            sys.path.append(dir)

def setupOperator(dir):
    pydir = os.path.join(dir, 'opt', 'python')
    __splpy_addDirToPath(os.path.join(pydir, 'modules'))

def pickleObject(v):
    return pickle.dumps(v)
     
def pickleReturn(function) :
    def _pickleReturn(v):
        return pickleObject(function(v))
    return _pickleReturn

# Given a function F return a function
# that depickles the input and then calls F
def depickleInput(function) :
    def _depickleInput(v):
        return function(pickle.loads(v))
    return _depickleInput

# Given a serialized callable instance, 
# deserialize and return the callable
def depickleCallableInstance(serializedCallable):
    return pickle.loads(base64.b64decode(serializedCallable))
 
# Given a serialized callable instance, 
# deserialize the callable into F, return 
# a function that depickles the input and then calls F
def depickleInputForCallableInstance(serializedCallable) :
    callableObject = depickleCallableInstance(serializedCallable)
    def _depickleInputForCallableInstance(v):
        return callableObject(pickle.loads(v))
    return _depickleInputForCallableInstance
 
# Given a function that returns an iterable
# return a function that can be called
# repeatably by a source operator returning
# the next tuple in its pickled form
def iterableSource(function) :
  iterator = iter(function())
  def _sourceIterator():
     try:
        while True:
            tuple = next(iterator)
            if not tuple is None:
                return pickleObject(tuple)
     except StopIteration:
       return None
  return _sourceIterator

# Given a function and tuple argument
# that returns an iterable,
# return a function that can be called
# repeatedly by an operator returning
# the next tuple in its pickled form
def iterableObject(function, v) :
   appRetVal = function(pickle.loads(v))
   if appRetVal is None:
      appRetVal = []
   iterator = iter(appRetVal)
   def _iterableObject():
      try:
         while True:
            tuple = next(iterator)
            if not tuple is None:
               return pickleObject(tuple)
      except StopIteration:
         return None
   return _iterableObject
