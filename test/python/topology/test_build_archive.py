# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

from __future__ import print_function
import unittest
import sys
import os
import shutil
import tempfile
import zipfile

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

        self.archive = None
        self.tmp = None

    def test_build_archive(self):
        topo = Topology("test_build_archive")
        hw = topo.source((1,2,3))
        hw.print()
        sr = streamsx.topology.context.submit("BUILD_ARCHIVE", topo)
        self.assertIn('archivePath', sr)
        self.archive = sr['archivePath']
        self._check_archive()

    def _check_archive(self):
        self.assertTrue(os.path.exists(self.archive))

        self.tmp = tempfile.mkdtemp()
        self.assertTrue(self.archive.endswith('.zip'))
        with zipfile.ZipFile(self.archive, 'r') as zip_ref:
            zip_ref.extractall(self.tmp)

        # Verify expected build files
        for fn in ['Makefile', 'main_composite.txt', 'manifest_tk.txt']:
            self.assertTrue(os.path.isfile(os.path.join(self.tmp, fn)))

        # Verify topology toolkit
        topo_tk = os.path.join(self.tmp, 'com.ibm.streamsx.topology')
        self.assertTrue(os.path.isdir(topo_tk))

        # Verify each toolkit
        with open(os.path.join(self.tmp, 'manifest_tk.txt')) as fp:
            for tkn in  fp.read().splitlines():
                tkd = os.path.join(self.tmp, tkn)
                for nd in ['doc', 'output', 'samples', 'toolkit.xml']:
                    self.assertFalse(os.path.exists(os.path.join(tkd, nd)))
                for root, dirs, files in os.walk(tkd):
                    for fn in files:
                        self.assertFalse(fn.endswith('.pyi'))
                        self.assertFalse(fn.endswith('.pyc'))

    def tearDown(self):
        if self._si is not None:
            os.environ['STREAMS_INSTALL'] = self._si
        if self.archive:
            os.remove(self.archive);
        if self.tmp:
            shutil.rmtree(self.tmp);

