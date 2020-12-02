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

    def populate(self, topology, name, **options):
        #spl_tk.add_toolkit_dependency(topology, 'spl', '1.6.0') # TODO what is the required version?

        _op = _EndpointSource(topology, self.schema, self.buffer_size, name = name)
        if self.documentation is not None:
           _op._add_annotation(self.documentation, self.schema)
        return _op.outputs[0]


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
        annotation = None
        # add operator annotation with documentation when parameter is dict, use defaults for non-existing fields
        if documentation is not None:
           if isinstance(documentation, dict):
              doc_summary = documentation.get('summary','Data feed')
              if not isinstance(doc_summary, str):
                 raise TypeError("Property 'summary' is expected of type string.")
              doc_description = documentation.get('description','Inject data into streaming application')
              if not isinstance(doc_description, str):
                 raise TypeError("Property 'description' is expected of type string.")
              doc_tags = documentation.get('tags',['Input','Streams'])
              if isinstance(doc_tags, list):
                 for t in doc_tags:
                    if not isinstance(t, str):
                       raise TypeError("Property 'tags' is expected of type array of string.")
              else:
                 raise TypeError("Property 'tags' is expected of type array of string.")
              if 'attributeDescriptions' in documentation:
                 doc_attr = documentation['attributeDescriptions']
              else:
                 from streamsx.topology.schema import _SchemaParser
                 p = _SchemaParser(schema)
                 p._parse()
                 i = 0
                 doc_attr = {}
                 while i < len(p._type):
                    descr = {p._type[i][1] : {'description' : p._type[i][0]}}
                    doc_attr.update(descr)
                    i += 1
              if isinstance(doc_attr, dict):
                 values = doc_attr.values()
                 if 'description' not in str(values):
                    raise ValueError("Property 'attributeDescriptions' is expected of values containing key 'description'.")
              else:
                 raise TypeError("Property 'attributeDescriptions' is expected of type dict.")
              annotation = {'type':'endpoint', 'properties':{'port':op.runtime_id, 'summary':doc_summary, 'description':doc_description, 'tags':doc_tags, 'attributeDescriptions':doc_attr}}
              #print(annotation)
           else:
              raise TypeError('Parameter documentation is expected of type dict.')

        if annotation is not None:
           op._annotation(annotation)

