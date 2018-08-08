# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import os
import streamsx.scripts.extract

def _spl_dir():
    print('XXX-File', __file__)
    tests_dir = os.path.dirname(__file__)
    print('XXX-test_dir', tests_dir)
    r =  os.path.abspath(os.path.dirname(tests_dir))
    print('XXX-SPL_TEST_dir', r)
    return r

def _tk_dir(tk):
    return os.path.join(_spl_dir(), tk)

def _extract_tk(tk):
    streamsx.scripts.extract.main(['-i', _tk_dir(tk), '--make-toolkit'])
