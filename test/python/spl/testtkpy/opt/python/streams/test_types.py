# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

# Import the SPL decorators
from streamsx.spl import spl

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
    for b in l:
        try:
            bs = b.tobytes()
            return "Expected released memory view",
        except ValueError as ve:
            pass
    return None

@spl.map()
class BlobTest:
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
        return str(v, 'utf-8'),

@spl.map()
class ListBlobTest:
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
        return str(v, 'utf-8'),

@spl.map()
class MapBlobTest:
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
        return str(v, 'utf-8'),

