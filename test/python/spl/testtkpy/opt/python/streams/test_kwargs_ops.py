# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test Map functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pykwargs"

@spl.filter()
def KWFilter(**t):
    a = t['a']
    b = t['b']
    c = t['c']

    return a < b and c > 100


@spl.for_each()
def KWForEach(**t):
    a = t['a']
    b = t['b']
    c = t['c']
    if a >= b:
        raise ValueError("A>=B" + str(t))
    if c <= 100:
        raise ValueError("C<=100" + str(t))
