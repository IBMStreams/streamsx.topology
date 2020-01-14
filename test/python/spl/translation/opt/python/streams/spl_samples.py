# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2016

# Import the SPL decorators
from streamsx.spl import spl

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def splNamespace():
    return "com.ibm.streamsx.topology.pysamples.positional"

@spl.map()
def Noop(*tuple):
    "Pass the tuple along without any change."
    return tuple
