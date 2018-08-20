# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
from future.builtins import *

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
