# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
#
# Wrap the operator's iterable in a function
# that when called returns each value from
# the iteration returned by iter(callable).
# It the iteration returns None then that
# value is skipped (i.e. no tuple will be
# generated). When the iteration stops
# the wrapper function returns None.
#
# There are two possibilities:
# a) iterable is a decorated class and is iterable
# b) iterable is a decorated function that returns an iterator
#

from future.builtins import *

def _splpy_iter_source(iterable) :
  try:
      it = iter(iterable)
  except TypeError:
      it = iterable()
  def _wf():
     try:
        while True:
            tv = next(it)
            if tv is not None:
                return tv
     except StopIteration:
       return None
  
  _add_shutdown_hook(iterable, _wf)
  return _wf

def _add_shutdown_hook(fn, wrapper):
    if hasattr(fn, '_splpy_shutdown'):
        def _splpy_shutdown():
            fn._splpy_shutdown()
        wrapper._splpy_shutdown = _splpy_shutdown

# The decorated operators only support converting
# Python tuples or a list of Python tuples to
# an SPL output tuple. To simplify the generated code
# we handle any other type by using a wrapper function
# and converting to a Python tuple or list of Python
# tuples.
#
# A Python tuple returned by the wrapped function
# may be sparse, values not set by the dictionary
# (etc.) are set to None in the Python tuple.

def _splpy_convert_tuple(attributes):
    """Create a function that converts tuples to
    be submitted as dict objects into Python tuples
    with the value by position.
    Return function handles tuple,dict,list[tuple|dict|None],None
    """

    def _to_tuples(tuple_):
        if isinstance(tuple_, tuple):
            return tuple_
        if isinstance(tuple_, dict):
            return tuple(tuple_.get(name, None) for name in attributes)
        if isinstance(tuple_, list):
            lt = list()
            for ev in tuple_:
                if isinstance(ev, dict):
                    ev = tuple(ev.get(name, None) for name in attributes)
                lt.append(ev)
            return lt
        return tuple_
    return _to_tuples

def _splpy_to_tuples(fn, attributes):
   conv_fn = _splpy_convert_tuple(attributes)

   def _to_tuples(*args, **kwargs):
      value = fn(*args, **kwargs)
      return conv_fn(value)

   _add_shutdown_hook(fn, _to_tuples)
   return _to_tuples

def _splpy_release_memoryviews(*args):
    for o in args:
        if isinstance(o, memoryview):
            o.release()
        elif isinstance(o, list):
            for e in o:
                _splpy_release_memoryviews(e)
        elif isinstance(o, dict):
            for e in o.values():
                _splpy_release_memoryviews(e)

def _splpy_primitive_input_fns(obj):
    """Convert the list of class input functions to be
        instance functions against obj.
        Used by @spl.primitive_operator SPL cpp template.
    """
    ofns = list()
    for fn in obj._splpy_input_ports:
        ofns.append(getattr(obj, fn.__name__))
    return ofns
    

def _splpy_primitive_output_attrs(callable_, port_attributes):
    """Sets output conversion functions in the callable."""
    conv_fns = []
    for attributes in port_attributes:
        conv_fns.append(_splpy_convert_tuple(attributes))
        
    callable_._splpy_conv_fns = conv_fns


def _splpy_all_ports_ready(callable_):
    """Call all_ports_ready for a primitive operator."""
    if hasattr(type(callable_), 'all_ports_ready'):
        return callable_.all_ports_ready()
    return None
