# coding:utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015, 2016

from __future__ import absolute_import, division, print_function
import sys

# Simple inclusion of Python logic within an SPL application
# as a SPL "Function" operator. A "Function" operator has
# a single input port and single output port, a function
# is called for every input tuple, and results in
# no submission or a single tuple being submitted.

# Import the SPL decorators
from streamsx.spl import spl

# Any function in a Python module (.py file) within the
# toolkit's opt/python/streams directory is converted to a primitive operator
# with a single input and output port. The primitive operator
# is a C++ primitive that embeds the Python runtime.
#
# The function must be decorated with one of these
#
# @spl.pipe - Function is a pipe operator
# @spl.sink - Function is a sink operator
# @spl.ignore - Function is ignored

# Attributes of the input SPL tuple are passed
# as a Python Tuple and thus are available as positional arguments.
# (see examples below)

# Any returned value from a function must be a Tuple.
#
# If nothing is returned then no tuple is submitted
# by the operator for the input tuple.
#
# When a Tuple is returned, its values are assigned
# to the first N attributes of the output tuple,
# that is by position.
# The returned values in the Tuple must be assignable
# to the output tuple attribute types.
#
# If the output port has more than N attributes
# then any remaining attributes are set from the
# input tuple if there is a matching input attribute by
# name and type, otherwise the attribute remains at
# its default value.
#
# If the output port has fewer attributes than N
# then any additional values are ignored.

# Any function whose name starts with spl is not created
# as an operator, such functions are reserved as a mechanism
# to pass information back to the primitive operator generator.

# The description of the function becomes the description
# of the primitive operator model in its operator model.

#------------------------------------------------------------------
# Example functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def spl_namespace():
    return "com.ibm.streamsx.topology.pysamples.sources"

@spl.source()
class Range(object):
    def __init__(self, count):
        self.count = count

    def __iter__(self):
        # Use zip to convert the single returned value
        # into a tuple to allow it to be returned to SPL
        if sys.version_info.major == 2:
            # zip behaviour differs on Python 2.7
            return iter(zip(range(self.count)))
        return zip(range(self.count))

@spl.source()
def Range37():
    """Sample of a function as a source operator."""
    if sys.version_info.major == 2:
        # zip behaviour differs on Python 2.7
        return iter(zip(range(37)))
    return zip(range(37))
