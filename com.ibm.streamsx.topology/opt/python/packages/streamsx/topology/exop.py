# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from future.builtins import *
from past.builtins import basestring

import streamsx.topology.topology
import streamsx.topology.schema


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
        self._op().addInputPort(outputPort=_input.oport, name=_input.name, window_config = win_cfg)
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
        if schemas is not None:
            stream_name = None
            if isinstance(schemas, basestring):
                schemas = (schemas,)
                stream_name = self._op().name

            try:
                for schema in schemas:
                    schema = streamsx.topology.schema._stream_schema(schema)
                    oport = self._op().addOutputPort(schema=schema, name=stream_name)
                    self.outputs.append(streamsx.topology.topology.Stream(self.topology, oport)._make_placeable())
            except TypeError:
                # not iterable, single schema
                schema = streamsx.topology.schema._stream_schema(schemas)
                oport = self._op().addOutputPort(schema=schema, name=self._op().name)
                self.outputs.append(streamsx.topology.topology.Stream(self.topology, oport)._make_placeable())
