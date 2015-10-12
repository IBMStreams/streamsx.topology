# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

# Simple inclusion of Python logic within an SPL application
# as a SPL "Function" operator. A "Function" operator has
# a single input port and single output port, a function
# is called for every input tuple, and results in
# no submission or a single tuple being submitted.

# Import the SPL utilities
from com.ibm.streamsx.topology.python import spl

# Any function in a Python module (.py file) within the
# toolkit's opt/python/streams directory is converted to a primitive operator
# with a single input and output port. The primitive operator
# is a C++ primitive that embeds the Python runtime.
#
# The function must be decorated with one of these
#
# @spl.operator - Function is a function operator
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
def splNamespace():
    return "com.ibm.streamsx.topology.pysamples.positional"

# Example where the function has acccess to
# all attributes of the input tuple using
# Python variable arguments.
#
# See Test01.spl

@spl.operator
def noop(*tuple):
    "Pass the tuple along without any change"
    return tuple


# Filters tuples by only returning a value if
# the first attribute is less than the second
# The returned value is a Tuple containing
# a single value being the sum of the first
# two input attributes.
# Since this is Python the type of a and b
# need not be specified, this will work with
# a schema of tuple<int32 a, int32b> and
# tuple<int32 a, float64 b>.
#
# Note the comma at the end of the return to make
# the single value a Python Tuple
#
# Remember input attributes are passed in from SPL by
# position, any additional attributes beyond the
# first two are ignored. Attribute names in SPL are irrelevant.
#
# See Test02.spl

@spl.operator
def simplefilter(a,b):
   "Filter tuples only allowing output if the first attribute is less than the second. Returns the sum of the first two attributes."
   if (a < b):
       return a+b,

# Adds the first two attributes and the
# second two attributes and returns
# the results as the first two attribute in the output tuple 
#
#
# See Test03.spl - Note that the from SPL we pass in integers
# and rstring values, and the Python code handles the '+'
# as arithmetic addition for the integers and string
# concatenation for the string values.

@spl.operator
def addFirstTwoSecondTwo(a,b,c,d):
    "Add first two and second two attributes"
    return a+b,c+d

# Example where the first attribute is passed by position
# as the first parameter (threshold) and all the remaining
# attributes are available as a variable argument list
#
@spl.operator
def lowest(threshold, *values):
    "Find the lowest value above a threshold in all the remaining attributes"
    lm = None
    for v in values:
      if v >= threshold:
          if lm == None:
             lm = v
          elif v < lm:
             lm = v
    if lm != None:
         return lm,


    
# Returns four copies of the tuple passed in. This demonstrates the
# ability for a python tuple to submit multiple tuples as output.
#
@spl.operator
def returnList(a,b,c):
    "Demonstrate returning a list of values, each value is submitted as a tuple" 
    return [(a+1,b+1,c+1),(a+2,b+2,c+2),(a+3,b+3,c+3),(a+4,b+4,c+4)]

