# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from __future__ import print_function
import unittest
import sys
import os

# Test without access to a Streams install
os.unsetenv('STREAMS_INSTALL')

import test_functions

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context


class TestBuildArchive(unittest.TestCase):

  def test_build_archive(self):
     topo = Topology("test_build_archive")
     hw = topo.source((1,2,3))
     hw.print()
     streamsx.topology.context.submit("BUILD_ARCHIVE", topo)
   
if __name__ == '__main__':
    unittest.main()
