# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

import test_functions
from test_utilities import standalone

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context

import test_package.test_subpackage.test_module
import test2_pkg_helpers

ctxtype = "STANDALONE"

@unittest.skipIf(sys.version_info.major == 2 or not test_vers.tester_supported() , "tester requires Python 3.5 and Streams >= 4.2")
class TestPackages(unittest.TestCase):

  # test using input functions from a regular package that has __init__.py
  # test using input functions that are fully qualified
  def test_TopologyImportPackage(self):
      topo = Topology("test_TopologyImportPackage")
      hw = topo.source(test_package.test_subpackage.test_module.SourceTuples(["Hello", "World!"]))
      hwf = hw.filter(test_package.test_subpackage.test_module.filter)
      tester = Tester(topo)
      tester.contents(hwf, ["Hello"])
      tester.test(ctxtype)

  # indirect import is included
  def test_TopologyIndirectPackage(self):
      topo = Topology("test_TopologyIndirectPackage")
      hw = topo.source(["Hello", "World!"])
      hwf = hw.transform(test2_pkg_helpers.imported_package)
      tester = Tester(topo)
      tester.contents(hwf, ["HelloIP"])
      tester.test(ctxtype)

  # Now test that if we exclude the package it is not found
  def test_TopologyExcludePackage(self):
      topo = Topology("test_TopologyExcludePackage")
      topo.exclude_packages.add('test_package')
      hw = topo.source(["Hello", "World!"])
      hwf = hw.transform(test2_pkg_helpers.missing_package)
      tester = Tester(topo)
      tester.contents(hwf, ["HelloMP", "World!MP"])
      tester.test(ctxtype)
