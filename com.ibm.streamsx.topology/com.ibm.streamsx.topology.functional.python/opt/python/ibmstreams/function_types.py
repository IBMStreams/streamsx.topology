__author__ = 'wcmarsha'

class functionTypes:
    """Contains string values for the different kinds of stream Python functions that
        can be returned from a functionFactory. A stream python function is a function
        that is executed from an SPL operator to provide python support. Currently,
        there are only sourceFunctions, sinkFunctions, and tupleFunctions."""

    TUPLE_FUNCTION = "TUPLE_FUNCTION"
    """A transformation function"""

    SOURCE_FUNCTION = "SOURCE_FUNCTION"
    """A python source operator"""

    SINK_FUNCTION = "SINK_FUNCTION"
    """A python sink operator"""
