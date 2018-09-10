# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
"""
SPL type definitions.

********
Overview
********

SPL is strictly typed, thus when invoking SPL operators
using classes from ``streamsx.spl.op`` then any parameters
must use the SPL type required by the operator.

"""
from future.builtins import *

import datetime
import time
import streamsx.spl.op
import streamsx.spl.runtime

class Timestamp(streamsx.spl.runtime._Timestamp):
    """
    SPL native timestamp type with nanosecond resolution.

    Common usage is to store the seconds and nanoseconds since the Unix Epoch (Jan 1, 1970),
    but this is not enforced by the `Timestamp` class.

    Machine identifier is an optional application defined identifier for the machine the timestamp
    was created on. It is the responsibility of the application to set the machine identifier
    if required. The machine identifier may be used to detect if two timestamps were created on the same machine,
    as there may be variations in the clocks on different machines.

    A instance can be created by passing seconds, nanoseconds and
    optionally machine identifier::

        # Timestamp with the current time in seconds
        # discarding any fractional seconds.
        ts = Timestamp(time.time(), 0)

        # Timestamp set to a specific time with a machine identifier
        ts = Timestamp(1516500542, 9511447, 4)

    A `Timestamp` is a namedtuple with three fields `seconds`, `nanoseconds`
    and `machine_id`.

    Attributes:
        seconds (int) : Seconds since epoch.
        nanoseconds (int) : Nanosecond component.
        machine_id (int) : Optional machine identifier, defaults to zero.

    .. warning::
        Implementation of `Timestamp` changed with 1.8.3 to be a `namedtuple`
        maintaining the existing class API.
    """

    _EPOCH = datetime.datetime.utcfromtimestamp(0)
    _NS = 1000000000.0


    @staticmethod
    def from_datetime(dt, machine_id=0):
        """
        Convert a datetime to an SPL `Timestamp`.
        
        Args:
           dt(datetime.datetime): Datetime to be converted.
           machine_id(int): Machine identifier.

        Returns:
             Timestamp: Datetime converted to Timestamp.
        """
        td = dt - Timestamp._EPOCH
        seconds = td.days * 3600 * 24
        seconds += td.seconds
        return Timestamp(seconds, td.microseconds*1000, machine_id)

    @staticmethod
    def from_time(t, machine_id=0):
        """Convert seconds since epoch to a Timestamp.

        The time argument matches the return from ``time.time()``.

        Args:
           t(float): Time to be converted.
           machine_id(int): Machine identifier.

        Returns:
             Timestamp: Time converted to Timestamp.

        .. versionadded:: 1.8.3
        """
        return Timestamp(t, (t % 1) * Timestamp._NS, machine_id)

    @staticmethod
    def now(machine_id=0):
        """Timestamp representing the current time.

        Args:
           machine_id(int): Machine identifier.

        Returns:
             Timestamp: Current time.

        .. versionadded:: 1.8.3
        """
        return Timestamp.from_time(time.time(), machine_id)

    @staticmethod
    def _check_nanos(ns):
         ns = int(ns)
         if ns < 0 or ns >= Timestamp._NS:
              raise ValueError("nanoseconds must in the range 0-999999999")
         return ns

    def __new__(cls, seconds, nanoseconds, machine_id=0):
        return streamsx.spl.runtime._Timestamp.__new__(cls, int(seconds), Timestamp._check_nanos(nanoseconds), int(machine_id))

    def time(self):
        """
        Get the time in seconds since the epoch.

        Returns:
            float: time in seconds since the epoch.
        """
        return self.seconds + (self.nanoseconds / Timestamp._NS)

    def datetime(self):
        """
        Return the UTC datetime corresponding to the POSIX timestamp.

        This is identical to ``datetime.datetime.utcfromtimestamp(self.time())``.
        Nanosecond resolution may be lost.

        Returns:
             datetime.datetime: Timestamp converted to a `datetime.datetime`.
        """
        return datetime.datetime.utcfromtimestamp(self.time())

    def tuple(self):
        """Return this timestamp as a tuple.

        Returns:
            tuple: Returns a tuple of ``(seconds, nanoseconds, machine_id)``

        .. deprecated:: 1.8.3
            Timestamp is a `tuple` now.
        """
        return self

    def __reduce__(self):
        return streamsx.spl.runtime._stored_ts, tuple(self)

def _get_timestamp_tuple(ts):
    """
    Internal method to get a timestamp tuple from a value.
    Handles input being a datetime or a Timestamp.
    """
    if isinstance(ts, datetime.datetime):    
        return Timestamp.from_datetime(ts).tuple()
    elif isinstance(ts, Timestamp):    
        return ts
    raise TypeError('Timestamp or datetime.datetime required')
    

def int8(value):
    """
    Create an SPL ``int8`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('INT8', int(value))

def int16(value):
    """
    Create an SPL ``int16`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('INT16', int(value))

def int32(value):
    """
    Create an SPL ``int32`` value.

    Returns:
        Expression: Expression representing the value.

    Args:
        value(int): Value to be types as ``int32``.
    """
    return streamsx.spl.op.Expression('INT32', int(value))

def int64(value):
    """
    Create an SPL ``int64`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('INT64', int(value))

def uint8(value):
    """
    Create an SPL ``uint8`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('UINT8', int(value))

def uint16(value):
    """
    Create an SPL ``uint16`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('UINT16', int(value))

def uint32(value):
    """
    Create an SPL ``uint32`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('UINT32', int(value))

def uint64(value):
    """
    Create an SPL ``uint64`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('UINT64', int(value))

def float32(value):
    """
    Create an SPL ``float32`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('FLOAT32', float(value))

def float64(value):
    """
    Create an SPL ``float64`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('FLOAT64', float(value))

def rstring(value):
    """
    Create an SPL ``rstring`` value.

    Returns:
        Expression: Expression representing the value.
    """
    return streamsx.spl.op.Expression('RSTRING', str(value))

def null():
    """
    Return an SPL ``null``.

    Returns:
        Expression: Expression representing an SPL null value.

    .. versionadded:: 1.10
    """
    return streamsx.spl.op.Expression.expression("null")
