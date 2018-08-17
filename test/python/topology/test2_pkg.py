# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_functions
from test_utilities import standalone

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context

import test_package.test_subpackage.test_module
import test2_pkg_helpers

class TestPackages(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
      Tester.setup_standalone(self)

  # test using input functions from a regular package that has __init__.py
  # test using input functions that are fully qualified
  def test_TopologyImportPackage(self):
      topo = Topology("test_TopologyImportPackage")
      hw = topo.source(test_package.test_subpackage.test_module.SourceTuples(["Hello", "World!"]))
      hwf = hw.filter(test_package.test_subpackage.test_module.filter)
      tester = Tester(topo)
      tester.contents(hwf, ["Hello"])
      tester.test(self.test_ctxtype, self.test_config)

  # indirect import is included
  def test_TopologyIndirectPackage(self):
      topo = Topology("test_TopologyIndirectPackage")
      hw = topo.source(["Hello", "World!"])
      hwf = hw.transform(test2_pkg_helpers.imported_package)
      tester = Tester(topo)
      tester.contents(hwf, ["HelloIP"])
      tester.test(self.test_ctxtype, self.test_config)

  # Now test that if we exclude the package it is not found
  def test_TopologyExcludePackage(self):
      topo = Topology("test_TopologyExcludePackage")
      topo.exclude_packages.add('test_package')
      hw = topo.source(["Hello", "World!"])
      hwf = hw.transform(test2_pkg_helpers.missing_package)
      tester = Tester(topo)
      tester.contents(hwf, ["HelloMP", "World!MP"])
      pypath = None

      # if PYTHONPATH includes the tests in standalone
      # then the test will find the excluded module.
      # Others are fine since PYTHONPATH in the instance
      # will not be set.
      if 'STANDALONE' == self.test_ctxtype:
          pypath = os.environ.pop('PYTHONPATH', None)

      try:
          tester.test(self.test_ctxtype, self.test_config)
      finally:
          if pypath is not None:
              os.environ['PYTHONPATH'] = pypath

class TestDistributedPackages(TestPackages):
  def setUp(self):
      Tester.setup_distributed(self)

class TestBluemixPackages(TestPackages):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

    def test_add_pip_package(self):
        topo = Topology()
        topo.add_pip_package('pint')
        s = topo.source([1])
        s = s.map(lambda x : __import__('pint').__name__)
        tester = Tester(topo)
        tester.contents(s, ['pint'])
        tester.test(self.test_ctxtype, self.test_config)
