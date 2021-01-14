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

    Use an instance of this class in :py:meth:`~streamsx.topology.topology.Topology.source` and create a stream::

       from streamsx.service import EndpointSource
       topo.source(EndpointSource())

    Example endpoint that receives data in JSON format::

       from streamsx.service import EndpointSource
       s = topo.source(EndpointSource(schema=CommonSchema.Json), name='cpd_endpoint_src_json')

    Example with structured Stream schema, service and endpoint documentation::

       from streamsx.topology.topology import Topology
       from streamsx.service import EndpointSource
    
       topo = Topology('endpoint_source_sample')
       service_documentation={
          'title': 'streamsx-sample-endpoint-source',
          'description': 'Streams job as service receives data',
          'version': '1.0.0'
       }
       endpoint_documentation = {
          'summary': 'Sample endpoint source',
          'description': 'CPD job endpoint injects some data'
       }
       schema = 'tuple<rstring id, int64 number>'
       s = topo.source(EndpointSource(
          schema=schema,
          buffer_size=200000,
          service_documentation=service_documentation,
          endpoint_documentation=endpoint_documentation), name='cpd_endpoint_src')

    .. versionadded:: 1.18

    Attributes
    ----------
    schema : StreamSchema
        Schema of the source stream.
    buffer_size : int
        Size of the buffer
    service_documentation: dict
        Content to describe the service. This is set once per application only.
        Apply a ``dict`` containing one or more of the keys: 'title, 'version', 'description', 'externalDocsUrl', 'externalDocsDescription', 'tags'::

           d = {
              'title': <string value>,
              'version': <string value>,
              'description': <string value>,
              'externalDocsUrl': <string value>,
              'externalDocsDescription': <string value>,
              'tags': {<key> {'description': <string value>, 'externalDocs': {'url': <string value>, 'description': <string value>}}, ...}
           }
    endpoint_documentation : dict
        Additional content to be included for an API endpoint to describe the endpoint source.
        Apply a ``dict`` containing one or more of the keys: 'summary, 'tags', 'description', 'attributeDescriptions'::

           d = {
              'summary': <string value>,
              'tags': <array of strings>,
              'description': <string value>,
              'attributeDescriptions': {<key> {'description': <string value>}, ...}
           }

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

    Use an instance of this class in :py:meth:`~streamsx.topology.topology.Stream.for_each` and terminate a stream::

       from streamsx.service import EndpointSink
       stream.for_each(EndpointSink())

    Simple example without service or endpoint documentation::
    
       from streamsx.topology.topology import Topology
       from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
       from streamsx.service import EndpointSink
       from typing import Iterable, NamedTuple
       import itertools, random

       class SampleSourceSchema(NamedTuple):
           id: str
           num: int

       # Callable of the Source
       class SampleSource(object):
           def __call__(self) -> Iterable[SampleSourceSchema]: 
               for num in itertools.count(1):
                   output_event = SampleSourceSchema(
                       id = str(num),
                       num = random.randint(0,num)
                   )
                   yield output_event
            
       topo = Topology('endpoint_sink_sample')

       stream1 = topo.source(SampleSource())
       stream1.for_each(EndpointSink(buffer_size=50000))

    Example with service and endpoint documentation::
    
       service_documentation={
          'title': 'streamsx-sample-endpoint-sink',
          'description': 'NUMBER GENERATOR',
          'version': '1.0.0',
          'externalDocsUrl': 'https://mycompany.com/numgen/doc',
          'externalDocsDescription': 'Number generator documentation'
       }
       tags = dict()
       tag1 = {
          'Output': {
             'description': 'Output tag description',
             'externalDocs': {
                'url': 'https://mycompany.com/numgen/input/doc',
                'description': 'Output tag external doc description'
             }
          }
       }
       tags.update(tag1)
       service_documentation['tags'] = tags

       endpoint_documentation = dict()
       endpoint_documentation['summary'] = 'Sample endpoint sink'
       endpoint_documentation['tags'] = ['Output']
       endpoint_documentation['description'] = 'Streams job emits some data with random numbers'

       doc_attr = dict()
       descr = {'id': {'description': 'IDENTIFIER (incremented by one per tuple)'}}
       doc_attr.update(descr)
       descr = {'num': {'description': 'RANDOM NUMBER'}}
       doc_attr.update(descr)
       endpoint_documentation['attributeDescriptions'] = doc_attr

       stream1 = topo.source(SampleSource())
       stream1.for_each(EndpointSink(
          buffer_size=50000,
          service_documentation=service_documentation,
          endpoint_documentation=endpoint_documentation), name='cpd_endpoint_sink')

    .. versionadded:: 1.18

    Attributes
    ----------
    buffer_size : int
        Size of the buffer. If the buffer capacity is reached, older tuples are removed to make room for the newer tuples. A warning is returned on an API request if the requested start time is before the oldest tuple in the buffer. The default buffer size is 1000.
    consuming_reads : boolean
        Indicates whether tuples should be removed from the endpoint buffer after they have been retuned on a REST API call. The default value is false.
    service_documentation: dict
        Content to describe the service. This is set once per application only.
        Apply a ``dict`` containing one or more of the keys: 'title, 'version', 'description', 'externalDocsUrl', 'externalDocsDescription', 'tags'::

           d = {
              'title': <string value>,
              'version': <string value>,
              'description': <string value>,
              'externalDocsUrl': <string value>,
              'externalDocsDescription': <string value>,
              'tags': {<key> {'description': <string value>, 'externalDocs': {'url': <string value>, 'description': <string value>}}, ...}
           }
    endpoint_documentation : dict
        Additional content to be included for an API endpoint to describe the endpoint sink.
        Apply a ``dict`` containing one or more of the keys: 'summary, 'tags', 'description', 'attributeDescriptions'::

           d = {
              'summary': <string value>,
              'tags': <array of strings>,
              'description': <string value>,
              'attributeDescriptions': {<key> {'description': <string value>}, ...}
           }

    Returns:
        :py:class:`topology_ref:streamsx.topology.topology.Sink`: Stream termination.
    """
    def __init__(self, buffer_size=None, consuming_reads=None, service_documentation=None, endpoint_documentation=None):
        self.buffer_size = buffer_size
        self.consuming_reads = consuming_reads
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
        if self.consuming_reads is not None:
           spl_tk.add_toolkit_dependency(topology, 'spl', '1.5.1')
        else:
           spl_tk.add_toolkit_dependency(topology, 'spl', '1.5.0')
        # invoke spl operator
        _op = _EndpointSink(stream, self.buffer_size, self.consuming_reads, name)
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
               raise TypeError("service_documentation: Property 'title' is expected of type string.")
            props['title'] = doc_title

         # description
         doc_description = documentation.get('description', None)
         if doc_description is not None:
            if not isinstance(doc_description, str):
               raise TypeError("service_documentation: Property 'description' is expected of type string.")
            props['description'] = doc_description

         # version
         doc_version = documentation.get('version', None)
         if doc_version is not None:
            if not isinstance(doc_version, str):
               raise TypeError("service_documentation: Property 'version' is expected of type string.")
            props['version'] = doc_version

         # externalDocsUrl
         doc_externalDocsUrl = documentation.get('externalDocsUrl', None)
         if doc_externalDocsUrl is not None:
            if not isinstance(doc_externalDocsUrl, str):
               raise TypeError("service_documentation: Property 'externalDocsUrl' is expected of type string.")
            props['externalDocsUrl'] = doc_externalDocsUrl

         # externalDocsDescription
         doc_externalDocsDescription = documentation.get('externalDocsDescription', None)
         if doc_externalDocsDescription is not None:
            if not isinstance(doc_externalDocsDescription, str):
               raise TypeError("service_documentation: Property 'externalDocsDescription' is expected of type string.")
            props['externalDocsDescription'] = doc_externalDocsDescription

         # tags
         doc_tags = documentation.get('tags', None)
         if doc_tags is not None:
            if not isinstance(doc_tags, dict):
               raise TypeError("service_documentation: Property 'tags' is expected of type dict.")
            else:
               values = doc_tags.values()
               if 'description' not in str(values):
                  raise ValueError("endpoint_documentation: Property 'tags' is expected of values containing key 'description'.")
               # convert to string that is expected in Java code generator to generate the annotation
               tags_str = str(doc_tags).replace("'",'\\\"')
               props['tags'] = tags_str

         # create service annotation dict
         annotation = {'type':'service', 'properties': props}
      else:
         raise TypeError("service_documentation: Parameter 'service_documentation' is expected of type dict.")
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
               raise TypeError("endpoint_documentation: Property 'summary' is expected of type string.")
            props['summary'] = doc_summary

         # description
         doc_description = documentation.get('description', default_description)
         if doc_description is not None:
            if not isinstance(doc_description, str):
               raise TypeError("endpoint_documentation: Property 'description' is expected of type string.")
            props['description'] = doc_description

         # tags
         doc_tags = documentation.get('tags', None)
         if doc_tags is not None:
            if isinstance(doc_tags, list):
               for t in doc_tags:
                  if not isinstance(t, str):
                     raise TypeError("endpoint_documentation: Property 'tags' is expected of type array of string.")
            else:
               raise TypeError("endpoint_documentation: Property 'tags' is expected of type array of string.")
            props['tags'] = doc_tags

         # attributeDescriptions
         doc_attr = documentation.get('attributeDescriptions', None)
         if doc_attr is not None:
            # check type
            if isinstance(doc_attr, dict):
               values = doc_attr.values()
               if 'description' not in str(values):
                  raise ValueError("endpoint_documentation: Property 'attributeDescriptions' is expected of values containing key 'description'.")
            else:
               raise TypeError("endpoint_documentation: Property 'attributeDescriptions' is expected of type dict.")
            props['attributeDescriptions'] = doc_attr

         # create endpoint annotation dict
         annotation = {'type':'endpoint', 'properties': props}   
      else:
         raise TypeError("endpoint_documentation: Parameter 'endpoint_documentation' is expected of type dict.")
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

    def __init__(self, stream, bufferSize=None, consumingReads=None, name=None):
        topology = stream.topology
        kind="spl.endpoint::EndpointSink"
        params = dict()
        if bufferSize is not None:
            params['bufferSize'] = bufferSize
        if consumingReads is not None:
            params['consumingReads'] = consumingReads
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


