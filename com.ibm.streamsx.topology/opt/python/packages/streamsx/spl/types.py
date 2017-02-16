# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
"""
SPL type definitions.

SPL is strictly typed, thus when invoking SPL operators
using classes from ``streamsx.spl.op`` then any parameters
must use the SPL type required by the operator.

"""

class _TypedV:
    def __init__(self, type, value):
        self.type = type
        self.value = value

    def spl_json(self):
        _splj = {}
        _splj["type"] = self.type 
        _splj["value"] = self.value
        return _splj

    def __str__(self):
        return str(self.value)

def int8(value):
    """
    Create an SPL ``int8`` value.
    """
    return _TypedV('INT8', int(value))

def int16(value):
    """
    Create an SPL ``int16`` value.
    """
    return _TypedV('INT16', int(value))

def int32(value):
    """
    Create an SPL ``int32`` value.
    """
    return _TypedV('INT32', int(value))

def int64(value):
    """
    Create an SPL ``int64`` value.
    """
    return _TypedV('INT64', int(value))

def uint8(value):
    """
    Create an SPL ``uint8`` value.
    """
    return _TypedV('UINT8', int(value))

def uint16(value):
    """
    Create an SPL ``uint16`` value.
    """
    return _TypedV('UINT16', int(value))

def uint32(value):
    """
    Create an SPL ``uint32`` value.
    """
    return _TypedV('UINT32', int(value))

def uint64(value):
    """
    Create an SPL ``uint64`` value.
    """
    return _TypedV('UINT64', int(value))

def float32(value):
    """
    Create an SPL ``float32`` value.
    """
    return _TypedV('FLOAT32', int(value))

def float64(value):
    """
    Create an SPL ``float64`` value.
    """
    return _TypedV('FLOAT64', int(value))
