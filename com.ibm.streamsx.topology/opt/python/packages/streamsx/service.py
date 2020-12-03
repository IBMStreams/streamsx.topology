# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
"""
Streams Jobs as a Cloud Pak for Data Service

********
Overview
********

A `streams-application` service can be used to insert data into and retrieve data from a Streams job.
When adding one or more callables using :py:class:`~streamsx.service.EndpointSource` or :py:class:`~streamsx.service.EndpointSink` to your topology application and submitting the application to run as a job, a `streams-application` service is created.

Exchanging data with the job is done by using a REST API.

The `streams-application` service instances are included in the **Services > Instances** page of the Cloud Pak for Data web client.
Selecting a service entry in the list opens the REST API documentation for the service.

.. seealso::
    `Streams Jobs as a Service <https://ibm.biz/streams-job-service>`_
        Resources for Streams developers in the IBM Community.

"""

import streamsx.spl.op
import streamsx.spl.types
import streamsx.topology.composite
import streamsx.spl.toolkit as spl_tk
from streamsx.topology.schema import CommonSchema


class EndpointSource(streamsx.topology.composite.Source):
    """Creates a service endpoint that produces a stream of data received via REST.

    With this source the Streams job is enabled as a Cloud Pak for Data service and retrieves job data using REST API.

    .. versionadded:: 2.0

    Attributes
    ----------
    schema : StreamSchema
        Schema of the source stream.
    buffer_size : int
        Size of the buffer
    service_documentation: dict
        Content to describe the service. This is set once per application only.
    endpoint_documentation : dict
        Additional content to be included for an API endpoint to describe the endpoint source.

    Returns:
        Stream.
    """

    def __init__(self, schema, buffer_size=None, endpoint_documentation=None, service_documentation=None):
        self.schema = schema
        self.buffer_size = buffer_size
        self.documentation = endpoint_documentation
        self.service_documentation = service_documentation
        # check schema
        if self.schema is CommonSchema.Python:
            raise TypeError('CommonSchema.Python is not supported by the EndpointSource')

    def populate(self, topology, name, **options):
        # set job service annotation
        service_annotation = _get_service_annotation(self.service_documentation)
        topology._set_service_annotation(service_annotation)
        # add toolkit dependency and required minimum Streams SPL toolkit version
        spl_tk.add_toolkit_dependency(topology, 'spl', '1.5.0')
        # invoke spl operator
        _op = _EndpointSource(topology, self.schema, self.buffer_size, name)
        # apply endpoint annotation
        if self.documentation is not None:
           _op._add_annotation(self.documentation, self.schema)
        return _op.outputs[0]


class EndpointSink(streamsx.topology.composite.ForEach):
    """Creates a service endpoint to consume data from that stream via REST.

    With this sink the Streams job is enabled as a Cloud Pak for Data service and emits job data using REST API.

    .. versionadded:: 2.0

    Attributes
    ----------
    buffer_size : int
        Size of the buffer
    service_documentation: dict
        Content to describe the service. This is set once per application only.
    endpoint_documentation : dict
        Additional content to be included for an API endpoint to describe the endpoint sink.

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.
    """
    def __init__(self, buffer_size=None, service_documentation=None, endpoint_documentation=None):
        self.buffer_size = buffer_size
        self.documentation = endpoint_documentation
        self.service_documentation = service_documentation

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:
        # set job service annotation
        service_annotation = _get_service_annotation(self.service_documentation)
        stream.topology._set_service_annotation(service_annotation)
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


def _get_service_annotation(documentation):
   annotation = None
   # operator annotation with documentation when parameter is dict, use defaults for non-existing fields
   if documentation is not None:
      if isinstance(documentation, dict):
         props = {}
         # title
         doc_title = documentation.get('title', None)
         if doc_title is not None:
            if not isinstance(doc_title, str):
               raise TypeError("Property 'title' is expected of type string.")
            props['title'] = doc_title

         # description
         doc_description = documentation.get('description', None)
         if doc_description is not None:
            if not isinstance(doc_description, str):
               raise TypeError("Property 'description' is expected of type string.")
            props['description'] = doc_description

         # version
         doc_version = documentation.get('version', None)
         if doc_version is not None:
            if not isinstance(doc_version, str):
               raise TypeError("Property 'version' is expected of type string.")
            props['version'] = doc_version

         # externalDocsUrl
         doc_externalDocsUrl = documentation.get('externalDocsUrl', None)
         if doc_externalDocsUrl is not None:
            if not isinstance(doc_externalDocsUrl, str):
               raise TypeError("Property 'externalDocsUrl' is expected of type string.")
            props['externalDocsUrl'] = doc_externalDocsUrl

         # externalDocsDescription
         doc_externalDocsDescription = documentation.get('externalDocsDescription', None)
         if doc_externalDocsDescription is not None:
            if not isinstance(doc_externalDocsDescription, str):
               raise TypeError("Property 'externalDocsDescription' is expected of type string.")
            props['externalDocsDescription'] = doc_externalDocsDescription
   
         # create service annotation dict
         annotation = {'type':'service', 'properties': props}   
   return annotation


def _get_annotation(documentation, port, default_summary, default_description):
   annotation = None
   # operator annotation with documentation when parameter is dict, use defaults for non-existing fields
   if documentation is not None:
      if isinstance(documentation, dict):
         props = {'port':port}

         # summary
         doc_summary = documentation.get('summary', default_summary)
         if doc_summary is not None:
            if not isinstance(doc_summary, str):
               raise TypeError("Property 'summary' is expected of type string.")
            props['summary'] = doc_summary

         # description
         doc_description = documentation.get('description', default_description)
         if doc_description is not None:
            if not isinstance(doc_description, str):
               raise TypeError("Property 'description' is expected of type string.")
            props['description'] = doc_description

         # tags
         doc_tags = documentation.get('tags', None)
         if doc_tags is not None:
            if isinstance(doc_tags, list):
               for t in doc_tags:
                  if not isinstance(t, str):
                     raise TypeError("Property 'tags' is expected of type array of string.")
            else:
               raise TypeError("Property 'tags' is expected of type array of string.")
            props['tags'] = doc_tags

         # attributeDescriptions
         doc_attr = documentation.get('attributeDescriptions', None)
         if doc_attr is not None:
            # check type
            if isinstance(doc_attr, dict):
               values = doc_attr.values()
               if 'description' not in str(values):
                  raise ValueError("Property 'attributeDescriptions' is expected of values containing key 'description'.")
            else:
               raise TypeError("Property 'attributeDescriptions' is expected of type dict.")
            props['attributeDescriptions'] = doc_attr

         # create endpoint annotation dict
         annotation = {'type':'endpoint', 'properties': props}   
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
        annotation = _get_annotation(documentation=documentation, port=op.runtime_id, default_summary=default_summary, default_description=default_description)
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
        annotation = _get_annotation(documentation=documentation, port=self.stream_name, default_summary=default_summary, default_description=default_description)
        if annotation is not None:
           op._annotation(annotation)


