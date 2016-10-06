# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from streamsx.topology.topology import Stream
from streamsx.topology.schema import _stream_schema

def source(kind, topology, schema, params=None):
    """
    Invoke an SPL source operator with a single output port.
    """
    return invoke(kind, topology, schemas=_stream_schema(schema), params=params)

def sink(kind, input, params=None):
    """
    Invoke an SPL sink operator with a single input port.
    """
    return invoke(kind, inputs=input, params=params)

def map(kind, input, schema=None, params=None):
    """
    Invoke an SPL operator with a single input port and
    single output port.
    """
    if schema is None:
        schema = input.oport.schema
    return invoke(kind, inputs=input, schemas=_stream_schema(schema), params=params)

def invoke(kind, topology=None, inputs=None, schemas=None, params=None):
    """
    Invoke an SPL operator with an arbitrary number of input ports
    and arbitrary number of output ports.
    """

    if topology is None:
         topology = inputs.topology

    # Add operator invocation
    op = topology.graph.addOperator(kind=kind)

    # Add parameters
    if params is not None:
        op.setParameters(params)

    # Input ports
    if inputs is not None:
        try:
            for input in inputs:
                op.addInputPort(outputPort=input.oport)
              
        except TypeError:
            # not iterable, single input
            op.addInputPort(outputPort=inputs.oport)

    # Output ports
    if schemas is None:
         return None

    try:
       oports = []
       for schema in schemas:
           oport = op.addOutputPort(schema=schema)
           oports.append(oport)
       return oports
 
    except TypeError:
       # not iterable, single schema
       oport = op.addOutputPort(schema=schemas)
       return Stream(topology, oport)
