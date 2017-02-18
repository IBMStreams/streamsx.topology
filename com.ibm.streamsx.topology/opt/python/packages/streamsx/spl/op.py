# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

import streamsx.topology.exop as exop

"""
IBM Streams supports *Stream Processing Language* (SPL),
a domain specific language for streaming analytics.
SPL creates an application by building a graph of operator
invocations. These operators are declared in a SPL toolkit.

SPL streams have a fixed schema, such as
``tuple<rstring id, timestamp ts, float64 value>`` for
a sensor reading with a sensor identifier, timestamp and value.

A Python topology application can take advantage of SPL operators
by using Streams with fixed SPL schemas.

"""

class Invoke(exop.ExtensionOperator):
    """
    Declaration of an invocation of an SPL operator in a Topology.

    An SPL operator has an arbitrary of input ports and
    an arbitrary number of output ports. The kind of the
    operator places constraints on how many input and output
    ports it supports, and potentially the schemas for those
    ports. For example ``spl.relational::Filter`` has
    a single input port and one or two output ports,
    in addition the schemas of the ports must be identical.

    When the operator has output ports an instance of
    ``SPLOperator`` has an ``outputs`` attributes which
    is a list of ``Stream`` instances.

    Args:
        topology(Topology): Topology that will invoke the operator.
        kind(str): SPL operator kind, e.g. ``spl.utility::Beacon``.
        inputs: Streams to connect to the operator. If not set or set to
            ``None`` or an empty collection then the operator has no
             input ports. Otherwise a list or tuple of ``Stream`` instances
             where the number of items is the number of input ports.
        schemas: Schemas of the output ports. If not set or set to
            ``None`` or an empty collection then the operator has no
             outut ports. Otherwise a list or tuple of schemas
             where the number of items is the number of output ports.
        params: Operator parameters.
             
    """
    def __init__(self,topology,kind,inputs=None,schemas=None,params=None,name=None):
        super(Invoke,self).__init__(topology,kind,inputs,schemas,params,name)

class Source(Invoke):
    """
    Declaration of an invocation of an SPL *source* operator.

    Source operators typically bring external data into
    a Streams application as a stream. A source operator has
    no input ports and a single output port.

    An instance of Source has an attribute ``stream`` that is
    ``Stream`` produced by the operator.

    This is a utility class that allows simple invocation
    of the common case of a operator with a single output port.

    Args:
        topology(Topology): Topology that will invoke the operator.
        kind(str): SPL operator kind, e.g. ``spl.utility::Beacon``.
        schema: Schema of the output port.
        params: Operator parameters.
    """
    def __init__(self,topology,kind,schema,params=None,name=None):
        super(Source,self).__init__(topology, kind, schemas=schema, params=params,name=name)

    @property
    def stream(self):
        """
        """
        return self.outputs[0]

class Map(Invoke):
    """
    Declaration of an invocation of an SPL *map* operator.

    *Map* operators have a single input port and single
    output port.

    An instance of Map has an attribute ``stream`` that is
    ``Stream`` produced by the operator.

    This is a utility class that allows simple invocation
    of the common case of a operator with a single input stream
    and single output stream.

    Args:
        kind(str): SPL operator kind, e.g. ``spl.utility::Beacon``.
        stream: Stream to connect to the operator.
        schema: Schema of the output stream.
        params: Operator parameters.
    """
    def __init__(self,kind,stream,schema=None,params=None,name=None):
        if schema is None:
            schema = stream.oport.schema
        super(Map,self).__init__(stream.topology,kind,inputs=stream,schemas=schema,params=params,name=name)

    @property
    def stream(self):
        return self.outputs[0]

class Sink(Invoke):
    """
    Declaration of an invocation of an SPL sink operator.

    Source operators typically send data on a stream to an
    external system. A sink operator has a single input port
    and no output ports.

    This is a utility class that allows simple invocation
    of the common case of a operator with a single input port.

    Args:
        kind(str): SPL operator kind, e.g. ``spl.utility::Beacon``.
        input: Stream to connect to the operator.
        params: Operator parameters.
    """
    def __init__(self,kind,stream,params=None,name=None):
        super(Sink,self).__init__(stream.topology,kind,inputs=stream,params=params,name=name)
