# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

#
# SPL Operators that pass the SPL tuple in
# as a Python dictionary containing all the
# attributes in the SPL tuple. The attribute
# name is the key value.
#
# This requires that the callable signature
# has a single **kwargs parameter.
#

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Example functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def spl_namespace():
    return "com.ibm.streamsx.topology.pysamples.kwargs"

@spl.map()
class DeltaFilter(object):
    "Drops any tuple that is within a delta of the last tuple for the attribute named `value`."
    def __init__(self, delta):
        self.delta = delta
        self.empty = ()
        self.last_value = None

    # Signature has **kwargs parameter which fixes the
    # SPL tuple parameter passing style to be dictionary
    def __call__(self, **tuple_):
        value = tuple_['value']
        if self.last_value is not None:
            if abs(value - self.last_value) <= self.delta:
                self.last_value = value
                return None
        self.last_value = value
        # Empty tuple will cause any matching input attributes
        # to be carried across to the output tuple
        return self.empty

@spl.filter()
class ContainsFilter(object):
    """
    Looks for a string term in any attribute in the tuple.
    """
    def __init__(self, term):
        self.term = term;

    def __call__(self, **tuple_):
        for s in tuple_.values():
            if self.term in str(s):
                return True
        return False
