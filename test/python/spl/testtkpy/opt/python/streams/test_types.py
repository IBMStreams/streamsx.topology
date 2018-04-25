# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import unicode_literals
from builtins import *

# Import the SPL decorators
from streamsx.spl import spl
import sys

#------------------------------------------------------------------
# Test passing in SPL types functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pytypes"

@spl.map()
def ToBlob(*s):
    return (s[0].encode('utf-8'),)

@spl.map()
def ToListBlob(*s):
    return ([s[0].encode('utf-8')],)

@spl.map()
def ToMapBlob(*s):
    return ({"BLOB": s[0].encode('utf-8')},)

def validate_mv_blob(v):
    if not isinstance(v, memoryview):
        return ("Expected memory view is" + str(type(v)),)
    bs = v.tobytes()

    if not v.readonly:
        return "Expected readonly memory view",

    if v.itemsize != 1:
        return "Expected readonly memory view",

    return None

def validate_mv_blob_release(l):
    # No release in Python 2
    if sys.version_info.major == 2:
        return None
    for b in l:
        try:
            bs = b.tobytes()
            return "Expected released memory view",
        except ValueError as ve:
            pass
    return None

@spl.map()
class BlobTest(object):
    """
    Expect blob tuples, need to verify that after
    the call the previous value cannot be accessed.
    """
    def __init__(self, keep):
        self.last = list()
        self.keep = keep

    def __call__(self, *tuple):
        v = tuple[0]
        mvc = validate_mv_blob(v)
        if mvc:
            return mvc

        mvc = validate_mv_blob_release(self.last)
        if mvc:
            return mvc

        if self.keep:
             self.last.append(v)
        if sys.version_info.major == 2:
            v = v.tobytes()
        return str(v, encoding='utf-8'),

@spl.map()
class ListBlobTest(object):
    """
    Expect list<blob> tuples, need to verify that after
    the call the previous value cannot be accessed.
    """
    def __init__(self):
        self.last = list()

    def __call__(self, *tuple):
        v = tuple[0][0]
        mvc = validate_mv_blob(v)
        if mvc:
            return mvc

        mvc = validate_mv_blob_release(self.last)
        if mvc:
            return mvc

        self.last.append(v)
        if sys.version_info.major == 2:
            v = v.tobytes()
        return str(v, encoding='utf-8'),

@spl.map()
class MapBlobTest(object):
    """
    Expect map<rstring,blob> tuples, need to verify that after
    the call the previous value cannot be accessed.
    """
    def __init__(self):
        self.last = list()

    def __call__(self, *tuple):
        v = tuple[0]["BLOB"]
        mvc = validate_mv_blob(v)
        if mvc:
            return mvc

        mvc = validate_mv_blob_release(self.last)
        if mvc:
            return mvc

        self.last.append(v)
        if sys.version_info.major == 2:
            v = v.tobytes()
        return str(v, encoding='utf-8'),

MRV = [
    None,
    ('astuple', 823),
    ('aspartialtuple', None),
    (),
    {'how': 'asdict', 'val':234},
    {'how': 'aspartialdict1', 'val':None},
    {'how': 'aspartialdict2'},
    {},
    [],
    [None,None,None],
    [(), None, {}, {}],
    [('listtuple', 494), {'how':'listdict', 'val':863}],
    [('listpartialtuple',None), {'how':'listpartialdict'}],
    ]

@spl.map()
def MapReturnValues(*t):
    """Simple test of returning values from a map."""
    idx = t[0]
    return MRV[idx] if idx < len(MRV) else None

@spl.source()
def SourceReturnValues():
    return iter(MRV)

import threading
@spl.primitive_operator(output_ports=['A'])
class PrimitiveReturnValues(spl.PrimitiveOperator):
    def __init__(self):
        pass

    def all_ports_ready(self):
        self.submitter = threading.Thread(target=self.submit_some)
        self.submitter.start()
        return self.submitter.join

    def submit_some(self):
        for tuple_ in MRV:
            self.submit('A', tuple_)
