# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import itertools
import os
import sys

from streamsx.topology.topology import *
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
from streamsx.topology.schema import CommonSchema
from streamsx.service import EndpointSource, EndpointSink

class TestEndpoint(unittest.TestCase):

    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False

    def _is_not_blank(self, s):
        if s is None:
            return false;
        return bool(s and s.strip())

    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_endpoint_sink(self):
        topo = Topology("test_endpoint_sink")
        stream1 = topo.source(lambda : itertools.count()).as_string()

        endpoint_documentation = dict()
        endpoint_documentation['summary'] = 'Sample endpoint sink'
        endpoint_documentation['tags'] = ['Output']
        endpoint_documentation['description'] = 'Streams job endpoint emits some data with random numbers'

        doc_attr = dict()
        descr = {'string': {'description': 'number incremented by one'}}
        doc_attr.update(descr)
        endpoint_documentation['attributeDescriptions'] = doc_attr

        service_documentation={'title': 'streamsx-sample-endpoint-sink', 'description': 'NUMBER GENERATOR', 'version': '0.1.0', 'externalDocsUrl': 'https://mycompany.com/numgen/doc', 'externalDocsDescription': 'Number generator documentation'}

        tags = dict()
        tag1 = {'Output': {'description': 'Output tag description', 'externalDocs': {'url': 'https://mycompany.com/numgen/input/doc', 'description': 'Output tag external doc description'}}}
        tags.update(tag1)
        service_documentation['tags'] = tags

        stream1.for_each(EndpointSink(buffer_size=50000, endpoint_documentation=endpoint_documentation, service_documentation=service_documentation), name='cpd_endpoint_sink')

        tester = Tester(topo)
        tester.tuple_count(stream1, 10, exact=False)
        tester.run_for(10)
        tester.test(self.test_ctxtype, self.test_config)

    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_param_consuming_reads(self):
        topo = Topology('test_param_consuming_reads')

        stream1 = topo.source(lambda : itertools.count()).as_string()

        endpoint_documentation = dict()
        endpoint_documentation['summary'] = 'Sample endpoint sink'
        endpoint_documentation['tags'] = ['Output']
        endpoint_documentation['description'] = 'Streams job endpoint emits some data with random numbers'

        doc_attr = dict()
        descr = {'string': {'description': 'number incremented by one'}}
        doc_attr.update(descr)
        endpoint_documentation['attributeDescriptions'] = doc_attr

        stream1.for_each(EndpointSink(buffer_size=50000, consuming_reads=True, endpoint_documentation=endpoint_documentation), name='cpd_endpoint_sink')

        tester = Tester(topo)
        tester.tuple_count(stream1, 10, exact=False)
        tester.run_for(10)
        tester.test(self.test_ctxtype, self.test_config)

    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_endpoint_source(self):
        topo = Topology("test_endpoint_source")

        service_documentation={'title': 'streamsx-sample-endpoint-sources', 'description': '2 sources'}

        documentation = dict()
        documentation['summary'] = 'Test endpoint source'
        documentation['tags'] = ['Input', 'STREAMS']
        documentation['description'] = 'CPD job endpoint injects some data'
        doc_attr = dict()
        descr = {'x': {'description': 'IDENTIFIER'}}
        doc_attr.update(descr)
        descr = {'n': {'description': 'NUMBER'}}
        doc_attr.update(descr)
        documentation['attributeDescriptions'] = doc_attr

        schema = 'tuple<rstring x, int64 n>'
        s = topo.source(EndpointSource(schema=schema, buffer_size=20000, service_documentation=service_documentation, endpoint_documentation=documentation), name='cpd_endpoint_src')
        s.print()

        documentation['summary'] = 'Test endpoint source JSON'
        s = topo.source(EndpointSource(schema=CommonSchema.Json, service_documentation=service_documentation, endpoint_documentation=documentation), name='cpd_endpoint_src_json')
        s.print()

        tester = Tester(topo)
        tester.run_for(10)
        tester.test(self.test_ctxtype, self.test_config)


    def test_no_python_schema(self):
        topo = Topology('test_no_python_schema')
        # EndpointSource does not support Python schema, expect TypeError
        self.assertRaises(TypeError, EndpointSource, schema=CommonSchema.Python)
        # EndpointSink does not support Python schema, expect TypeError
        stream1 = topo.source(lambda : itertools.count())
        with self.assertRaises(TypeError):
           stream1.for_each(EndpointSink())


    def test_spl_annotation(self):
        name = 'test_spl_annotation'
        topo = Topology(name)
        scriptname = os.path.basename(__file__)[:-3]
        service_documentation={
          'title': '__SERVICE__TITLE__',
          'description': '__SERVICE__DESC__',
        }
        endpoint_documentation = dict()
        endpoint_documentation['summary'] = '__ENDPOINT__SUMMARY__'
        endpoint_documentation['tags'] = ['__ENDPOINT__TAG1__', '__ENDPOINT__TAG2__']
        endpoint_documentation['description'] = '__ENDPOINT__DESC__'
        doc_attr = dict()
        descr = {'id': {'description': '__ENDPOINT__ATTR1__DESC__'}}
        doc_attr.update(descr)
        descr = {'num': {'description': '__ENDPOINT__ATTR2__DESC__'}}
        doc_attr.update(descr)
        endpoint_documentation['attributeDescriptions'] = doc_attr

        stream1 = topo.source(lambda : itertools.count()).as_string()
        stream1.for_each(EndpointSink(
          service_documentation=service_documentation,
          endpoint_documentation=endpoint_documentation))

        submission_result = submit(ContextTypes.TOOLKIT, topo)
        # check generated SPL file
        splfile = submission_result['toolkitRoot']+'/'+scriptname+'/'+name+'.spl'
        with open(splfile, "r") as fileHandle:
            ep_annotation = [line.strip() for line in fileHandle if "@endpoint" in line]
        print(str(ep_annotation))
        self.assertTrue('__ENDPOINT__SUMMARY__' in str(ep_annotation), msg=ep_annotation)
        self.assertTrue('__ENDPOINT__TAG1__' in str(ep_annotation), msg=ep_annotation)
        self.assertTrue('__ENDPOINT__TAG2__' in str(ep_annotation), msg=ep_annotation)
        self.assertTrue('__ENDPOINT__DESC__' in str(ep_annotation), msg=ep_annotation)
        self.assertTrue('__ENDPOINT__ATTR1__DESC__' in str(ep_annotation), msg=ep_annotation)
        self.assertTrue('__ENDPOINT__ATTR2__DESC__' in str(ep_annotation), msg=ep_annotation)
        with open(splfile, "r") as fileHandle:
            service_annotation = [line.strip() for line in fileHandle if "@service" in line]
        print(str(service_annotation))
        self.assertTrue('__SERVICE__TITLE__' in str(service_annotation), msg=service_annotation)
        self.assertTrue('__SERVICE__DESC__' in str(service_annotation), msg=service_annotation)


