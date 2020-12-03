# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
"""
Streams Job as a Cloud Pak for Data Service.
"""

import streamsx.spl.op
import streamsx.spl.types
import streamsx.topology.composite
import streamsx.spl.toolkit as spl_tk
from streamsx.topology.schema import CommonSchema


class EndpointSource(streamsx.topology.composite.Source):
    """Declare a source stream that introduces tuples into the application and creates a service endpoint to accept data from the job service.

    With this source the Streams job is enabled as a Cloud Pak for Data service and retrieves job data using REST API.

    .. versionadded:: 2.0

    Attributes
    ----------
    schema : StreamSchema
        Schema of the source stream.
    buffer_size : int
        Size of the buffer
    documentation : dict
        Additional content to be included for an API endpoint.

    Returns:
        Stream.
    """

    def __init__(self, schema, buffer_size=None, documentation=None):
        self.schema = schema
        self.buffer_size = buffer_size
        self.documentation = documentation
        # check schema
        if self.schema is CommonSchema.Python:
            raise TypeError('CommonSchema.Python is not supported by the EndpointSource')

    def populate(self, topology, name, **options):
        # add toolkit dependency and required minimum Streams SPL toolkit version
        spl_tk.add_toolkit_dependency(topology, 'spl', '1.5.0')
        # invoke spl operator
        _op = _EndpointSource(topology, self.schema, self.buffer_size, name)
        # apply endpoint annotation
        if self.documentation is not None:
           _op._add_annotation(self.documentation, self.schema)
        return _op.outputs[0]


class EndpointSink(streamsx.topology.composite.ForEach):
    """Sends job data using REST API and creates a service endpoint to accept data from the job service.

    With this sink the Streams job is enabled as a Cloud Pak for Data service and emits job data using REST API.

    .. versionadded:: 2.0

    Attributes
    ----------
    buffer_size : int
        Size of the buffer
    documentation : dict
        Additional content to be included for an API endpoint.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.
    """
    def __init__(self, buffer_size=None, documentation=None):
        self.buffer_size = buffer_size
        self.documentation = documentation

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:
        # check schema
        schema = stream.oport.schema
        if schema is CommonSchema.Python:
            raise TypeError('CommonSchema.Python is not supported by the EndpointSink')
        # add toolkit dependency and required minimum Streams SPL toolkit version
        spl_tk.add_toolkit_dependency(topology, 'spl', '1.5.0')
        # invoke spl operator
        _op = _EndpointSink(stream, self.buffer_size, name)
        # apply endpoint annotation
        if self.documentation is not None:
           _op._add_annotation(self.documentation, schema)
        return streamsx.topology.topology.Sink(_op)


def _get_annotation(documentation, port, schema, default_summary, default_description, default_tags):
   annotation = None
   # operator annotation with documentation when parameter is dict, use defaults for non-existing fields
   if documentation is not None:
      if isinstance(documentation, dict):
         # summary
         doc_summary = documentation.get('summary', default_summary)
         if not isinstance(doc_summary, str):
            raise TypeError("Property 'summary' is expected of type string.")
         # description
         doc_description = documentation.get('description', default_description)
         if not isinstance(doc_description, str):
            raise TypeError("Property 'description' is expected of type string.")
         # tags
         doc_tags = documentation.get('tags', default_tags)
         if isinstance(doc_tags, list):
            for t in doc_tags:
               if not isinstance(t, str):
                  raise TypeError("Property 'tags' is expected of type array of string.")
         else:
            raise TypeError("Property 'tags' is expected of type array of string.")
         # attributeDescriptions
         if 'attributeDescriptions' in documentation:
            doc_attr = documentation['attributeDescriptions']
         else:
            doc_attr = {}
            if schema is CommonSchema.XML:
               msg_attr_name = 'document'
               descr = {msg_attr_name : {'description' : 'XML'}}
               doc_attr.update(descr)
            elif schema is CommonSchema.Json:
               msg_attr_name = 'jsonString'
               descr = {msg_attr_name : {'description' : 'JSON'}}
               doc_attr.update(descr)
            elif schema is CommonSchema.String:
               msg_attr_name = 'string'
               descr = {msg_attr_name : {'description' : 'string'}}
               doc_attr.update(descr)
            elif schema is CommonSchema.Binary:
               msg_attr_name = 'binary'
               descr = {msg_attr_name : {'description' : 'binary'}}
               doc_attr.update(descr)
            else:
               from streamsx.topology.schema import _SchemaParser
               import streamsx.topology.schema as _sch
               nts = _sch._normalize(schema)
               p = _SchemaParser(nts._schema)
               p._parse()
               i = 0          
               while i < len(p._type):
                  descr = {p._type[i][1] : {'description' : p._type[i][0]}}
                  doc_attr.update(descr)
                  i += 1
         # check type
         if isinstance(doc_attr, dict):
            values = doc_attr.values()
            if 'description' not in str(values):
               raise ValueError("Property 'attributeDescriptions' is expected of values containing key 'description'.")
         else:
            raise TypeError("Property 'attributeDescriptions' is expected of type dict.")
            
         annotation = {'type':'endpoint', 'properties':{'port':port, 'summary':doc_summary, 'description':doc_description, 'tags':doc_tags, 'attributeDescriptions':doc_attr}}
      else:
         raise TypeError('Parameter documentation is expected of type dict.')
   return annotation


class _EndpointSource(streamsx.spl.op.Source):
    def __init__(self, topology, schema, bufferSize=None, name=None):
        kind="spl.endpoint::EndpointSource"
        schemas=schema
        params = dict()
        if bufferSize is not None:
            params['bufferSize'] = bufferSize
        self.spl_op = super(_EndpointSource, self)
        self.spl_op.__init__(topology,kind,schemas,params,name)

    def _add_annotation(self, documentation, schema):
        op = self.spl_op._op()
        default_summary = 'Streaming application data feed'
        default_description = 'Inject data into streaming application'
        default_tags = ['Input','Streams']
        annotation = _get_annotation(documentation=documentation, port=op.runtime_id, schema=schema, default_summary=default_summary, default_description=default_description, default_tags=default_tags)
        if annotation is not None:
           op._annotation(annotation)


class _EndpointSink(streamsx.spl.op.Sink):

    def __init__(self, stream, bufferSize=None, name=None):
        topology = stream.topology
        kind="spl.endpoint::EndpointSink"
        params = dict()
        if bufferSize is not None:
            params['bufferSize'] = bufferSize
        self.spl_op = super(_EndpointSink, self)
        self.spl_op.__init__(kind,stream,params,name)
        self.stream_name = stream.name

    def _add_annotation(self, documentation, schema):
        op = self.spl_op._op()
        default_summary = 'Streaming application data feed'
        default_description = 'Emits data from a streaming application'
        default_tags = ['Output','Streams']
        annotation = _get_annotation(documentation=documentation, port=self.stream_name, schema=schema, default_summary=default_summary, default_description=default_description, default_tags=default_tags)
        if annotation is not None:
           op._annotation(annotation)


