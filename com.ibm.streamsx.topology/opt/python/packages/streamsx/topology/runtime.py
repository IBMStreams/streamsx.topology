import os
import pickle
import base64
import sys

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
def depickleInput(callable) :
    def _depickleInput(v):
        return callable(pickle.loads(v))
    return _depickleInput

# Given a serialized callable instance, 
# deserialize and return the callable
def depickleCallableInstance(serializedCallable):
    return pickle.loads(base64.b64decode(serializedCallable))
 
# Given a serialized callable instance, 
# deserialize the callable into 'callable', return 
# a function that depickles the input and then calls 'callable'
def depickleInputForCallableInstance(serializedCallable) :
    callable = depickleCallableInstance(serializedCallable)
    return depickleInput(callable)
 
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

# Given a serialized callable instance, 
# deserialize the callable into 'callable',
# then call 'iterableSource'
def iterableSourceForCallableInstance(serializedCallable) :
  callable = depickleCallableInstance(serializedCallable)
  return iterableSource(callable)

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

