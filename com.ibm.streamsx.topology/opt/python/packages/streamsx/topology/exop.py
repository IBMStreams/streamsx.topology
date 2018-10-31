# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from future.builtins import *
from past.builtins import basestring

import streamsx.topology.topology
import streamsx.topology.schema

def _single_schema(schemas):
    if isinstance(schemas, basestring):
        return schemas
    if isinstance(schemas, streamsx.topology.schema.CommonSchema):
        return schemas
    if isinstance(schemas, streamsx.topology.schema.StreamSchema):
        return schemas
    if isinstance(schemas, list):
        return schemas[0] if len(schemas) == 1 else None
    if isinstance(schemas, tuple):
        return schemas[0] if len(schemas) == 1 else None


class ExtensionOperator(object):
    def __init__(self,topology,kind,inputs=None,schemas=None,params=None,name=None):
        self.topology = topology
        if params is None:
            params = dict()
        self.__op = topology.graph.addOperator(kind=kind,name=name)
        # Add parameters
        if params is not None:
            self._op().setParameters(params)
        self.__inputs(inputs)
        self.__outputs(schemas)

    def _op(self):
        return self.__op

    @property
    def params(self):
        """Parameters for the operator invocation.
        """
        return self._op().params

    def _add_input(self, _input):
        win_cfg = None
        if isinstance(_input, streamsx.topology.topology.Window):
            win_cfg = _input._config
            _input = _input.stream
        self._op().addInputPort(outputPort=_input.oport, window_config=win_cfg, alias=_input.name)
        self._inputs.append(_input)

    def __inputs(self, inputs):
        if inputs is not None:
            self._inputs = []
            try:
                for _input in inputs:
                    self._add_input(_input)
            except TypeError:
                # not iterable, single input
                self._add_input(inputs)

    def __outputs(self, schemas):
        self.outputs = []
        if schemas:
            if _single_schema(schemas):
                schema = streamsx.topology.schema._stream_schema(_single_schema(schemas))
                oport = self._op().addOutputPort(schema=schema, name=self._op().name)
                self.outputs.append(streamsx.topology.topology.Stream(self.topology, oport)._make_placeable())
            else:
                for schema in schemas:
                    schema = streamsx.topology.schema._stream_schema(schema)
                    oport = self._op().addOutputPort(schema=schema)
                    self.outputs.append(streamsx.topology.topology.Stream(self.topology, oport)._make_placeable())
