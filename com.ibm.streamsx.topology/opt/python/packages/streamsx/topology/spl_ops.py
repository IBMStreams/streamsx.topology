# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
"""
Invocation of SPL operators from a Python Topology.

Arbitrary SPL operators can be be invoked.
"""

from streamsx.topology.topology import Stream
from streamsx.topology.schema import _stream_schema

def source(kind, topology, schema, params=None):
    """
    Invoke an SPL source operator with a single output port.

    Args:
       kind(str): SPL operator kind. For example `spl.utility::Beacon`.
       topology:  Topology the operator invocation will be declared in.
       schema: SPL schema of the output port.
       params(dict): Dictionary of parameters to the operator invocation.

    Returns:
       Stream: Stream that will be connected to the operator output port.
    """
    return invoke(kind, topology, schemas=_stream_schema(schema), params=params)

def sink(kind, input, params=None):
    """
    Invoke an SPL sink operator with a single input port.

    Args:
       kind(str): SPL operator kind. For example `spl.utility::FileSink`.
       input(Stream): Stream to be connected to the operator.
       params(dict): Dictionary of parameters to the operator invocation.
      
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
