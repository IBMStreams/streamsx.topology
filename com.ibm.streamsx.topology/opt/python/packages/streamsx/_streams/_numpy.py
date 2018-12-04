# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018


try:
    import numpy as np
    import streamsx.spl.types
    _SPL_CONVERSIONS = {
        np.bool_:bool,
        np.uint8:streamsx.spl.types.uint8,
        np.uint16:streamsx.spl.types.uint16,
        np.uint32:streamsx.spl.types.uint32,
        np.uint64:streamsx.spl.types.uint64,
        np.int8:streamsx.spl.types.int8,
        np.int16:streamsx.spl.types.int16,
        np.int32:streamsx.spl.types.int32,
        np.int64:streamsx.spl.types.int64,
        np.float32:streamsx.spl.types.float32,
        np.float64:streamsx.spl.types.float64,
    }
except ImportError:
    np = None


# Conversion from a numpy value
# to an SPL expression for code generation.
def as_spl_expr(value):
    if np is None:
        return None
    vt = type(value)
    cfn = _SPL_CONVERSIONS.get(vt)
    if cfn:
        return cfn(value)
    return None
 
