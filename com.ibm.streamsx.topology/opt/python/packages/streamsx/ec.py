# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
"""
Overview
--------
Access to the IBM Streams execution context.

A Streams application runs distributed or standalone.

Distributed
-----------
Distributed is used when an application is submitted
to the Streaming Analytics service on IBM Bluemix cloud platform
or a IBM Streams distributed instance.

With distributed a running application is a `job` that
contains one or more processing elements (PEs). A PE
corresponds to a Linux operating system process.
The PEs in a job may be distributed across the
resources (hosts) in the Streams instance.

Standalone
----------
Standalone is a mode where the complete application is run
as a single PE (process) outside of a Streams instance.

Standalone is typically used for ad-hoc testing of an application.

Execution Context
-----------------
This module (`streamsx.exec`) provides access to the execution
context when Python code is running in a Streams application.

Access is only supported when running:
 * Python 3.5
 * Streams 4.2 or later

This module may be used by Python functions or classes used
in a `Topology` or decorated SPL operators.

Most functionality is only available when a Python class is
being invoked in a Streams application.

"""

import enum
import pickle
import threading

try:
    import _streamsx_ec as _ec
    _supported = True
except ImportError:
    _supported = False

def _check():
    if not _supported:
        raise NotImplementedError("Access to the execution context requires Python 3.5 and Streams 4.2 or later")

def job_id():
    """
    Return the job identifier.
    """
    _check()
    return _ec.job_id()

def pe_id():
    """
    Return the PE identifier.
    """
    _check()
    return _ec.pe_id()

def is_standalone():
    """Is the execution context standalone.

    Returns:
        boolean: True if the execution context is standalone, False if it is distributed.

    """
    _check()
    return _ec.is_standalone()

def get_application_configuration(name):
    """Get a named application configuration.

    An application configuration is a named set of securely stored properties
    where each key and its value in the property set is a string.

    An application configuration object is used to store information that
    IBM Streams applications require, such as:

    * Database connection data
    * Credentials that your applications need to use to access external systems
    * Other data, such as the port numbers or URLs of external systems

    Arguments:
        name(str): Name of the application configuration.

    Returns:
        dict: Dictionary containing the property names and values for the application configuration.

    Raises:
        ValueError: Application configuration does not exist.
    """
    _check()
    rc = _ec.get_application_configuration(name)
    if rc is False:
         raise ValueError("Application configuration {0} not found.".format(name))
    return rc

def channel(obj):
    """
    Return the parallel region global channel number `obj` is executing in.

    The channel number is in the range of 0 to ``max_channel(obj)``.

    When the parallel region is not nested this is the same value
    as ``local_channel(obj)``. 

    If the parallel region is nested the value will be between
    zero and ``(width*N - 1)`` where N is the number of times the
    parallel region has been replicated due to nesting.
    
    Args:
        obj: Instance of a class executing within Streams.

    Returns:
        int: Parallel region global channel number or -1 if not located in a parallel region.
    """
    return _ec.channel(_get_opc(obj))

def local_channel(obj):
    """
    Return the parallel region local channel number `obj` is executing in.

    The channel number is in the range of zero to ``local_max_channel(obj)``.
    
    Args:
        obj: Instance of a class executing within Streams.

    Returns:
        int: Parallel region local channel number or -1 if not located in a parallel region.
    """
    return _ec.local_channel(_get_opc(obj))

def max_channels(obj):
    """
    Return the global maximum number of channels for the parallel
    region `obj` is executing in.

    When the parallel region is not nested this is the same value
    as ``local_max_channels(obj)``. 

    If the parallel region is nested the value will be
    ``(width*N)`` where N is the number of times the
    parallel region has been replicated due to nesting.
    
    Args:
        obj: Instance of a class executing within Streams.
    Returns:
        int: Parallel region global maximum number of channels or 0 if not located in a parallel region.
    """
    return _ec.max_channels(_get_opc(obj))

def local_max_channels(obj):
    """
    Return the local maximum number of channels for the parallel
    region `obj` is executing in.

    The maximum number of channels corresponds to the width of the region.

    Args:
        obj: Instance of a class executing within Streams.
    Returns:
        int: Parallel region local maximum number of channels or 0 if not located in a parallel region.
    """
    return _ec.local_max_channels(_get_opc(obj))

@enum.unique
class MetricKind(enum.Enum):
    """
    Enumeration for the kind of a metric.

    The kind of the metric only indicates the behavior of value,
    it does not impose any semantics on the value.
    The kind is typically used by tooling applications.

    """
    Gauge = 0
    """
    A gauge metric observes a value that is continuously variable with time.
    """
    Counter = 1
    """
    A counter metric observes a value that represents a count of an occurrence.
    """
    Time = 2
    """
    A time metric represents a point in time or duration.
    The recommended unit of time is milliseconds, using the standard
    epoch of 00:00:00 Coordinated Universal Time (UTC),
    Thursday, 1 January 1970 to represent a point in time.
    """

class CustomMetric(object):
    """
    Create a custom metric.

    A custom metric holds a 64 bit signed integer value that represents
    a `Counter`, `Gauge` or `Time` metric.

    Custom metrics are exposed through the IBM Streams monitoring APIs.

    Args:
        obj: Instance of a class executing within Streams.
        name(str): Name of the custom metric.
        kind(MetricKind): Kind of the metric.
        description(str): Description of the metric.
        initialValue: Initial value of the metric.

    Examples:

    Simple example used as an instance to ``Stream.filter``::
    
	class MyF:
	    def __init__(self, substring):
                self.substring = substring
                pass

            def __call__(self, tuple):
                if self.substring in str(tuple)
                    self.my_metric += 1
                return True

            # Create the metric when the it is running
            # in the Streams execution context
            def __enter__(self):
                self.my_metric = ec.CustomMetric(self, "count_" + self.substring)

            # must supply __exit__ if using __enter__
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

            def __getstate__(self):
                # Remove metric from saved state.
                state = self.__dict__.copy()
                if 'my_metric' in state:
                    del state['my_metric']
                return state

            def __setstate__(self, state):
                self.__dict__.update(state)

    """
    def __init__(self, obj, name, description=None, kind=MetricKind.Counter, initialValue=0):
        if kind is None:
            kind = MetricKind.Counter
        elif kind in MetricKind.__members__:
            kind = MetricKind.__members__[kind]
        elif kind not in MetricKind.__members__.values():
            raise ValueError("kind is required to be MetricKind:" + kind)
        if description is None:
            description=name + ":" + kind.name
        self.name = str(name)
        self.kind = kind
        self.description = str(description)
        args = (_get_opc(obj), self.name, self.description, self.kind.value, int(initialValue))
        self.__ptr = _ec.create_custom_metric(args)

    @property
    def value(self):
        """
        Current value of the metric.
        """
        return _ec.metric_get(self.__ptr)

    @value.setter
    def value(self, value):
        """
        Set the current value of the metric.
        """
        args = (self.__ptr, int(value))
        _ec.metric_set(args)

    def __str__(self):
        return "{0}({1}):{2}".format(self.name, self.kind.name,self.value)

    def __iadd__(self, other):
        """
        Increment the current value of the metric.
        """
        args = (self.__ptr, int(other))
        _ec.metric_inc(args)
        return self

    def __int__(self):
        return self.value

    def __getstate__(self):
        raise pickle.PicklingError(CustomMetric.__name__)

####################
# internal functions
####################

# Thread local of operator pointers during
# operator class initialization
if _supported:
    _opptrs = threading.local()

# Sets the operator pointer as a thread
# local to allow access from an operator's
# class __init__ method.
def _set_opc(opc):
    _opptrs._opc = opc

# Clear the operator pointer from the
# thread local
def _clear_opc():
    if _supported:
        _opptrs._opc = None

# Save the opc in the operator class
# (getting it from the thread local)
def _save_opc(obj):
    _opptrs.obj = obj
    if hasattr(_opptrs, '_opc'):
       opc = _opptrs._opc
       if opc is not None:
           obj._streamsx_ec_op = opc

def _get_opc(obj):
    _check()
    try:
        return obj._streamsx_ec_op
    except AttributeError:
        try:
            opc = _opptrs._opc
            if opc is not None:
                return opc
        except AttributeError:
             pass
        raise AssertionError("InternalError")

def _shutdown_op(callable):
    if hasattr(callable, '_shutdown'):
        callable._shutdown()

def _callable_enter(callable):
    """Called at initialization time.
    """
    if hasattr(callable, '__enter__') and hasattr(callable, '__exit__'):
        callable.__enter__()

def _callable_exit_clean(callable):
    """Called at shutdown time.
    """
    if hasattr(callable, '__enter__') and hasattr(callable, '__exit__'):
        callable.__exit__(None, None, None)

