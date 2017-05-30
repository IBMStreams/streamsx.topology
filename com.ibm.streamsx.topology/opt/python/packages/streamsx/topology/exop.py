# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from streamsx.topology.topology import Stream
import streamsx.topology.schema as sch


class ExtensionOperator(object):
    def __init__(self,topology,kind,inputs=None,schemas=None,params=None,name=None):
        self.topology = topology
        if params is None:
            params = dict()
        self._op = topology.graph.addOperator(kind=kind,name=name)
        # Add parameters
        if params is not None:
            self._op.setParameters(params)
        self.__inputs(inputs)
        self.__outputs(schemas)

    @property
    def params(self):
        """Parameters for the operator invocation.
        """
        return self._op.params

    def __inputs(self, inputs):
        if inputs is not None:
            try:
                for input in inputs:
                    self._op.addInputPort(outputPort=input.oport, name=input.name)
                self.inputs = list(inputs)
            except TypeError:
                # not iterable, single input
                self._op.addInputPort(outputPort=inputs.oport, name=inputs.name)
                self.inputs = [inputs]

    def __outputs(self, schemas):
        self.outputs = []
        if schemas is not None:
            stream_name = None
            if isinstance(schemas, str):
                schemas = (schemas,)
                stream_name = self._op.name

            try:
                for schema in schemas:
                    schema = sch._stream_schema(schema)
                    oport = self._op.addOutputPort(schema=schema, name=stream_name)
                    self.outputs.append(Stream(self.topology, oport))
            except TypeError:
                # not iterable, single schema
                schema = sch._stream_schema(schemas)
                oport = self._op.addOutputPort(schema=schema, name=self._op.name)
                self.outputs.append(Stream(self.topology, oport))
