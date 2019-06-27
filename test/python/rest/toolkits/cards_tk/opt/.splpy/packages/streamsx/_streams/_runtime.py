# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
from future.builtins import *

import inspect
import logging
import os
import pkg_resources
import sys
import streamsx
from pkgutil import extend_path

_TRACE = logging.getLogger('streamsx.runtime')

def _add_to_sys_path(dir_):
    if _TRACE.isEnabledFor(logging.DEBUG):
        _TRACE.debug('Potential addition to sys.path: %s EXISTS %s', dir_, str(os.path.isdir(dir_)))
    if os.path.isdir(dir_) and dir_ not in sys.path and os.listdir(dir_):
        _TRACE.debug('Inserting as first entry to sys.path: %s', dir_)
        sys.path.insert(0, dir_)
        pkg_resources.working_set.add_entry(dir_)
        # In case a streamsx module (e.g. streamsx.bm) 
        # is included in the additional code
        if os.path.isdir(os.path.join(dir_, 'streamsx')):
            streamsx.__path__ = extend_path(streamsx.__path__, streamsx.__name__)
        return True
    return False

def _setup_operator(tk_dir):
    pydir = os.path.join(tk_dir, 'opt', 'python')
    changed = _add_to_sys_path(os.path.join(pydir, 'modules'))
    changed = _add_to_sys_path(os.path.join(pydir, 'packages')) or changed

    if changed and _TRACE.isEnabledFor(logging.INFO):
        _TRACE.info('Updated sys.path: %s', str(sys.path))

                
class _Setup(object):
    _DONE = False

    @staticmethod
    def _setup(out_dir):
        if _Setup._DONE:
            return
        _Setup._DONE = True
        bundle_site_dir = _Setup._add_output_packages(out_dir)
        _Setup._trace_packages(bundle_site_dir)

    @staticmethod
    def _add_output_packages(out_dir):
        py_dir = _Setup._pip_base_dir(out_dir)
        vdir = 'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)
        site_pkg = os.path.join(py_dir, 'lib', vdir, 'site-packages')
        _add_to_sys_path(site_pkg)
        return site_pkg

    @staticmethod
    def _pip_base_dir(out_dir):
        """Base Python directory for pip within the output directory"""
        return os.path.join(out_dir, 'etc', 'streamsx.topology', 'python')

    @staticmethod
    def _trace_packages(bundle_site_dir):
        if not _TRACE.isEnabledFor(logging.INFO):
            return

        _TRACE.info('sys.path: %s', str(sys.path))
        dists = list(pkg_resources.working_set)
        dists.sort(key=lambda d : d.key)
        _TRACE.info('*** Python packages ***')
        for pkg_ in dists:
            _TRACE.info(repr(pkg_))
        _TRACE.info('*** End Python packages ***')


def _setup(out_dir):
    _Setup._setup(out_dir)

# Application logic runs within an operator within a
# statement context, effectively a with statement.
# This means that for logic that has __enter__ and __exit__  methods
# (technically its type has the methods)
#
# This allows the logic to create and dispose of objects that
# cannot be pickled such as open files, custom metrics etc.
# 
# __enter__ is called:
#   a) when the operator starts up before tuple processing
#   b) when the operator resets, to inital state or from a checkpoint
#      enter is called on the new instance.
#
# __exit__ is called:
#   a) when the operator shuts down
#   b) when an exception occurs in tuple processing
#   c) when the operator resets (on the current instance)
#
#  Note: __exit__ is only called if __enter__ was called previously 
#  Note: in b) if __exit__ returns a true value then the exception is suppressed
#
# Two attributes are set in the object being wrapped to manage context:
#
# _streamsx_ec_context : Boolean indicating if the object has context methods
# _streamsx_ec_entered  : Has __enter__ been called on this instance.
#
# Note that the top-level Python object seen by the C++ primitive operator
# maintains these attributes and is responsible for calling __enter__
# on any wrapped logic.
#
# For topology:
#     The top-level object is an instance of
#     streamsx.topology.runtime._FunctionCallable wrapping the
#     application logic.
#
# For SPL primitives:
#     Source operator uses streamsx.spl.runtime._SourceIterator
#     that wraps the iterable maintains a reference to the iterator.
#
#     Otherwise the top-level object is an instance of sub-class
#     of the application logic.
#
# The C++ operator calls
#
#   _call_enter() - to enter the object into the context
#   _call_exit() - to exit the object from the context.
#
#   These methods are responsible for seeing if the underlying
#   application logic's methods are called.

def _has_context_methods(cls):
    return hasattr(cls, '__enter__') and hasattr(cls, '__exit__')

def _call_enter(obj, opc):
    if obj._streamsx_ec_context or obj._streamsx_ec_cls:
        obj._streamsx_ec_opc = opc
        if obj._streamsx_ec_context:
            obj.__enter__()
            obj._streamsx_ec_entered = True

def _call_exit(obj, exc_info=None):
    if obj._streamsx_ec_context and obj._streamsx_ec_entered:
        try:
            if exc_info is None:
                ev = obj.__exit__(None,None,None)
            else:
                exc_type = exc_info[0]
                exc_value = exc_info[1] if len(exc_info) >=2 else None
                traceback = exc_info[2] if len(exc_info) >=3 else None

                ev = obj.__exit__(exc_type, exc_value, traceback)
                if ev and exc_type is not None:
                    # Remain in the context
                    return ev
            obj._streamsx_ec_entered = False
            obj._streamsx_ec_opc = None
            return ev
        except:
            obj._streamsx_ec_entered = False
            obj._streamsx_ec_opc = None
            raise
    obj._streamsx_ec_opc = None

# A _WrappedInstance is used to wrap the functional logic
# passed into a function like map when declaring the graph.
# The wrapping occurs at topology declaration time and the
# instance of _WrappedInstance becomes the "users" logic
# that is passed in as the functional operator's parameter.
# 
# If no_context is true then it's guaranteed that
# callable_ does not have __enter__, __exit__ methods
class _WrapOpLogic(object):
    def __init__(self, callable_, no_context=None):
        self._callable = callable_

        is_cls = not inspect.isfunction(callable_)
        is_cls = is_cls and not inspect.isbuiltin(callable_)
        is_cls = is_cls and not inspect.isclass(callable_)
        self._streamsx_ec_cls = is_cls

        if is_cls and not no_context:
            if hasattr(callable_, '_streamsx_ec_context'):
                self._streamsx_ec_context = callable_._streamsx_ec_context
            else:
                self._streamsx_ec_context = streamsx._streams._runtime._has_context_methods(type(callable_))
        else:
            self._streamsx_ec_context = False

        self._streamsx_ec_entered = False

    def __enter__(self):
        if self._streamsx_ec_context or self._streamsx_ec_cls:
            self._callable._streamsx_ec_opc = self._streamsx_ec_opc
            if self._streamsx_ec_context:
                self._callable.__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._streamsx_ec_context:
            ev = self._callable.__exit__(exc_type, exc_value, traceback)
            if not ev:
                self._streamsx_ec_opc = None
            return ev
