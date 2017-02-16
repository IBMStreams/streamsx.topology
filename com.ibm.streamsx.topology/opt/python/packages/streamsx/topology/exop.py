# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from streamsx.topology.topology import Stream
from streamsx.topology.schema import StreamSchema


class ExtensionOperator(object):
    def __init__(self,topology,kind,inputs=None,schemas=None,params=None,name=None):
        self.topology = topology
        self.kind = kind
        if params is None:
            params = dict()
        self.params = params
        self._op = topology.graph.addOperator(kind=kind,name=name)
        # Add parameters
        if params is not None:
            self._op.setParameters(params)
        self.__inputs(inputs)
        self.__outputs(schemas)

    def __inputs(self, inputs):
        if inputs is not None:
            try:
                for input in inputs:
                    self._op.addInputPort(outputPort=input.oport)
            except TypeError:
                # not iterable, single input
                self._op.addInputPort(outputPort=inputs.oport)

    def __outputs(self, schemas):
        self.outputs = []
        if schemas is not None:
            if isinstance(schemas, str):
                schemas = (StreamSchema(schemas),)

            try:
                for schema in schemas:
                    oport = self._op.addOutputPort(schema=schema)
                    self.outputs.append(Stream(self.topology, oport))
            except TypeError:
                # not iterable, single schema
                oport = self._op.addOutputPort(schema=schemas)
                self.outputs.append(Stream(self.topology, oport))

    def add_param(self, name, value):
        if value is not None:
            self.params[name] = value
