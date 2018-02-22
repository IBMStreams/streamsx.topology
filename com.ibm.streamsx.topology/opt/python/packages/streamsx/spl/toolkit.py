# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

"""
SPL toolkit integration.

********
Overview
********

SPL operators are defined by an SPL toolkit. When a ``Topology`` 
contains invocations of SPL operators, their defining toolkit must
be made known using :py:func:`add_toolkit`.

Toolkits shipped with the IBM Streams product under
``$STREAMS_INSTALL/toolkits`` are implictly known and
must not be added through ``add_toolkit``.

"""
from future.builtins import *

import os
import streamsx.topology.topology

def add_toolkit(topology, location):
    """Add an SPL toolkit to a topology.

    Args:
        topology(Topology): Topology to include toolkit in.
        location(str): Location of the toolkit directory.
    """
    assert isinstance(topology, streamsx.topology.topology.Topology)
    tkinfo = dict()
    tkinfo['root'] = os.path.abspath(location)
    topology.graph._spl_toolkits.append(tkinfo)

