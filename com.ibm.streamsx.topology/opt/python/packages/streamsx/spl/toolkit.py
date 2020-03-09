# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2019

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

__all__ = ['add_toolkit', 'add_toolkit_dependency']

import os

def add_toolkit(topology, location):
    """Add an SPL toolkit to a topology.

    Args:
        topology(Topology): Topology to include toolkit in.
        location(str): Location of the toolkit directory.
    """
    import streamsx.topology.topology
    assert isinstance(topology, streamsx.topology.topology.Topology)
    tkinfo = dict()
    tkinfo['root'] = os.path.abspath(location)
    topology.graph._spl_toolkits.append(tkinfo)

def add_toolkit_dependency(topology, name, version):
    """Add a version dependency on an SPL toolkit to a topology.

    To specify a range of versions for the dependent toolkits,
    use brackets (``[]``) or parentheses. Use brackets to represent an
    inclusive range and parentheses to represent an exclusive range.
    The following examples describe how to specify a dependency on a range of toolkit versions:

        *  ``[1.0.0, 2.0.0]`` represents a dependency on toolkit versions 1.0.0 - 2.0.0, both inclusive.
        *  ``[1.0.0, 2.0.0)`` represents a dependency on toolkit versions 1.0.0 or later, but not including 2.0.0.
        *  ``(1.0.0, 2.0.0]`` represents a dependency on toolkits versions later than 1.0.0 and less than or equal to 2.0.0.
        *  ``(1.0.0, 2.0.0)`` represents a dependency on toolkit versions 1.0.0 - 2.0.0, both exclusive.

    Args:
        topology(Topology): Topology to include toolkit in.
        name(str): Toolkit name.
        version(str): Toolkit version dependency.

    .. seealso::

        `Toolkit information model file <https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/toolkitinformationmodelfile.html>`_

    .. versionadded:: 1.12
    """
    import streamsx.topology.topology
    assert isinstance(topology, streamsx.topology.topology.Topology)
    tkinfo = dict()
    tkinfo['name'] = name
    tkinfo['version'] = version.replace(' ', '')
    topology.graph._spl_toolkits.append(tkinfo)

