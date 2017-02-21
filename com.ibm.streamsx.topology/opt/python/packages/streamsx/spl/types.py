# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
"""
SPL type definitions.

SPL is strictly typed, thus when invoking SPL operators
using classes from ``streamsx.spl.op`` then any parameters
must use the SPL type required by the operator.

"""

from streamsx.spl.op import Expression

def int8(value):
    """
    Create an SPL ``int8`` value.
    """
    return Expression('INT8', int(value))

def int16(value):
    """
    Create an SPL ``int16`` value.
    """
    return Expression('INT16', int(value))

def int32(value):
    """
    Create an SPL ``int32`` value.
    """
    return Expression('INT32', int(value))

def int64(value):
    """
    Create an SPL ``int64`` value.
    """
    return Expression('INT64', int(value))

def uint8(value):
    """
    Create an SPL ``uint8`` value.
    """
    return Expression('UINT8', int(value))

def uint16(value):
    """
    Create an SPL ``uint16`` value.
    """
    return Expression('UINT16', int(value))

def uint32(value):
    """
    Create an SPL ``uint32`` value.
    """
    return Expression('UINT32', int(value))

def uint64(value):
    """
    Create an SPL ``uint64`` value.
    """
    return Expression('UINT64', int(value))

def float32(value):
    """
    Create an SPL ``float32`` value.
    """
    return Expression('FLOAT32', int(value))

def float64(value):
    """
    Create an SPL ``float64`` value.
    """
    return Expression('FLOAT64', int(value))
