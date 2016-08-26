# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

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
    Create an SPL int8 value.
    """
    return _TypedV('INT8', int(value))

def int16(value):
    """
    Create an SPL int16 value.
    """
    return _TypedV('INT16', int(value))

def int32(value):
    """
    Create an SPL int32 value.
    """
    return _TypedV('INT32', int(value))

def float32(value):
    """
    Create an SPL float32 value.
    """
    return _TypedV('FLOAT32', int(value))
