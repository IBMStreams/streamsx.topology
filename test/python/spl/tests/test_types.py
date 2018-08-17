# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import unicode_literals
from builtins import *

import copy
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.scripts.extract

import spl_tests_utils as stu

class TestTypes(unittest.TestCase):
    _multiprocess_can_split_ = True

    """ Type tests.
    """
    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    def test_blob_type(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        data = ['Hello', 'Blob', 'Did', 'you', 'reset' ]
        s = topo.source(data)
        s = s.as_string()

        toBlob = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::ToBlob",
            s,
            'tuple<blob b>')

        toBlob = op.Map(
            "com.ibm.streamsx.topology.pysamples.positional::Noop",
            toBlob.stream,
            'tuple<blob b>')

        bt = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::BlobTest",
            toBlob.stream,
            'tuple<rstring string>',
            {'keep': True})

        bt2 = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::BlobTest",
            toBlob.stream,
            'tuple<rstring string>',
            {'keep': False})
         
        tester = Tester(topo)
        tester.contents(bt.stream, data)
        self.test_config['topology.keepArtifacts'] = True;
        tester.test(self.test_ctxtype, self.test_config)

    def test_list_blob_type(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        data = ['Hello', 'Blob', 'Did', 'you', 'reset' ]
        s = topo.source(data)
        s = s.as_string()

        toBlob = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::ToListBlob",
            s,
            'tuple<list<blob> lb>')

        bt = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::ListBlobTest",
            toBlob.stream,
            'tuple<rstring string>')
         
        tester = Tester(topo)
        tester.contents(bt.stream, data)
        tester.test(self.test_ctxtype, self.test_config)

    def test_map_blob_type(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        data = ['Hello', 'Blob', 'Did', 'you', 'reset' ]
        s = topo.source(data)
        s = s.as_string()

        toBlob = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::ToMapBlob",
            s,
            'tuple<map<rstring,blob> lb>')

        bt = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::MapBlobTest",
            toBlob.stream,
            'tuple<rstring string>')
         
        tester = Tester(topo)
        tester.contents(bt.stream, data)
        tester.test(self.test_ctxtype, self.test_config)

    def test_optional_blob_type(self):
        Tester.require_streams_version(self, '4.3')
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        data = ['Hello', 'Blob', 'Did', 'you', 'reset' ]
        s = topo.source(data)
        s = s.as_string()

        toBlob = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::ToBlob",
            s,
            'tuple<optional<blob> ob>')

        bt = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::BlobTest",
            toBlob.stream,
            'tuple<rstring string>',
            {'keep': True})
         
        tester = Tester(topo)
        tester.contents(bt.stream, data)
        tester.test(self.test_ctxtype, self.test_config)


    def test_map_return(self):
        """Simple test of returning values from a map."""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        s = topo.source(range(20))
        s = s.map(lambda v : (v,), schema='tuple<int32 val>')

        values = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::MapReturnValues",
            s, 'tuple<rstring how, int32 val>')


        tester = Tester(topo)
        tester.contents(values.stream, MRV_EXPECTED)
        tester.test(self.test_ctxtype, self.test_config)

    def test_source_return(self):
        """Simple test of returning values from a source operator."""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        values = op.Source(
            topo,
            kind="com.ibm.streamsx.topology.pytest.pytypes::SourceReturnValues",
            schema='tuple<rstring how, int32 val>')

        expected = copy.deepcopy(MRV_EXPECTED)
        # no assignment from input tuple so all automatically
        # assigned values will be zero
        for d in expected:
            if d['val'] < 100:
                d['val'] = 0

        tester = Tester(topo)
        tester.contents(values.stream, expected)
        tester.test(self.test_ctxtype, self.test_config)

    def test_primitive_submit(self):
        """Simple test of submitting values from a primitive operator."""
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))

        values = op.Source(
            topo,
            kind="com.ibm.streamsx.topology.pytest.pytypes::PrimitiveReturnValues",
            schema='tuple<rstring how, int32 val>')

        expected = copy.deepcopy(MRV_EXPECTED)
        # no assignment from input tuple so all automatically
        # assigned values will be zero
        for d in expected:
            if d['val'] < 100:
                d['val'] = 0

        tester = Tester(topo)
        tester.contents(values.stream, expected)
        tester.test(self.test_ctxtype, self.test_config)

MRV_EXPECTED = [
    {'how': 'astuple', 'val':823},
    {'how': 'aspartialtuple', 'val':2},
    {'how': '', 'val':3},
    {'how': 'asdict', 'val':234},
    {'how': 'aspartialdict1', 'val':5},
    {'how': 'aspartialdict2', 'val':6},
    {'how': '', 'val':7},
    {'how': '', 'val':10},
    {'how': '', 'val':10},
    {'how': '', 'val':10},
    {'how': 'listtuple', 'val':494},
    {'how': 'listdict', 'val':863},
    {'how': 'listpartialtuple', 'val':12},
    {'how': 'listpartialdict', 'val':12},
    ]
