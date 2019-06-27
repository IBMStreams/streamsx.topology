# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
"""
Types used within IBM Streams and Streaming Analytics service.
"""

from enum import Enum

import streamsx.spl.op

class _SysEnum(Enum):
    """Super class for an Enum that maps to a enum in the
    spl.Sys composite from the standard toolkit.
    """
    def spl_json(self):
        return streamsx.spl.op.Expression.expression('Sys.' + self.name).spl_json()

class CongestionPolicy(_SysEnum):
    """Congestion policy for a threaded port's queue.

    .. versionadded:: 1.9
    """
    Wait = 0
    """When a tuple is to be added into a full queue the thread
    waits until space exists in the queue.
    """
    DropFirst = 1
    """When a tuple is to be added into a full queue the oldest tuple
    (first inserted) is dropped (discarded) and the newly arrived
    tuple inserted into the queue.
    """
    DropLast = 2
    """When a tuple is to be added into a queue and it is full
    the tuple is dropped (discarded).
    """

