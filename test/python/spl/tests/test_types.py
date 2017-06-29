# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit


class TestSPL(unittest.TestCase):
    """ Test invocations of SPL operators from Python topology.
    """
    def setUp(self):
        Tester.setup_standalone(self)

    def test_blob_type(self):
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, '../testtkpy')
        data = ['Hello', 'Blob', 'Did', 'you', 'reset' ]
        s = topo.source(data)
        s = s.as_string()

        toBlob = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::ToBlob",
            s,
            'tuple<blob b>')

        bt = op.Map(
            "com.ibm.streamsx.topology.pytest.pytypes::BlobTest",
            toBlob.stream,
            'tuple<rstring string>')
         
        tester = Tester(topo)
        tester.contents(bt.stream, data)
        tester.test(self.test_ctxtype, self.test_config)
