# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from __future__ import print_function
import unittest
import sys
import os

import test_functions

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context

class TestBuildArchive(unittest.TestCase):
  
    def setUp(self):
        # Test without access to a Streams install
        self._si = os.environ.get('STREAMS_INSTALL')
        if self._si is not None:
            os.unsetenv('STREAMS_INSTALL')

    def test_build_archive(self):
        topo = Topology("test_build_archive")
        hw = topo.source((1,2,3))
        hw.print()
        streamsx.topology.context.submit("BUILD_ARCHIVE", topo)

    def tearDown(self):
        if self._si is not None:
            os.environ['STREAMS_INSTALL'] = self._si
