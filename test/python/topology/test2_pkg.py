# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019
import unittest
import os
import sys
import itertools
from pathlib import Path

import test_functions
from test_utilities import standalone

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
from streamsx.topology.context import ConfigParams

import test_package.test_subpackage.test_module
import test2_pkg_helpers



class TestPackages(unittest.TestCase):
  _multiprocess_can_split_ = True

  @classmethod
  def setUpClass(cls):
      my_path = Path(__file__).parent
      # test_symlink_to_module -> test_symlink_to_module_real
      dst = os.path.join(my_path, 'test_symlink_to_module')
      src = os.path.join(my_path, 'test_symlink_to_module_real')
      if not Path(dst).is_symlink():
          os.symlink(src, dst)
      # symlinkmodule.py -> test_symlink_to_module_real/customsource.py
      dst = os.path.join(my_path, 'symlinkmodule.py')
      src = os.path.join(my_path, 'test_symlink_to_module_real', 'customsource.py')
      if not Path(dst).is_symlink():
          os.symlink(src, dst)

  @classmethod
  def tearDownClass(cls):
      my_path = Path(__file__).parent
      dst = os.path.join(my_path, 'test_symlink_to_module')
      if Path(dst).is_symlink():
          os.unlink(dst)
      dst = os.path.join(my_path, 'symlinkmodule.py')
      if Path(dst).is_symlink():
          os.unlink(dst)

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
      hwf = hw.map(test2_pkg_helpers.imported_package)
      tester = Tester(topo)
      tester.contents(hwf, ["HelloIP"])
      tester.test(self.test_ctxtype, self.test_config)

  # Now test that if we exclude the package it is not found
  def test_TopologyExcludePackage(self):
      topo = Topology("test_TopologyExcludePackage")
      topo.exclude_packages.add('test_package')
      hw = topo.source(["Hello", "World!"])
      hwf = hw.map(test2_pkg_helpers.missing_package)
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

  # directory symlink to module
  def test_TopologySymlinkDir(self):
      # test_symlink_to_module -> test_symlink_to_module_real
      from test_symlink_to_module.customsource import customsource
      topo = Topology("test_TopologySymlinkDir")
      st = topo.source(customsource())

      tester = Tester(topo)
      tester.tuple_count(st, 2, exact=True)
      tester.test(self.test_ctxtype, self.test_config)

  # file symlink to module
  def test_TopologySymlinkFile(self):
      # symlinkmodule.py -> test_symlink_to_module_real/customsource.py
      from symlinkmodule import customsource
      topo = Topology("test_TopologySymlinkFile")
      st = topo.source(customsource())

      tester = Tester(topo)
      tester.tuple_count(st, 2, exact=True)
      tester.test(self.test_ctxtype, self.test_config)

class TestDistributedPackages(TestPackages):

    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False

    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_add_pip_package(self):
        topo = Topology()
        topo.add_pip_package('pint')
        s = topo.source([1])
        s = s.map(lambda x : __import__('pint').__name__)
        tester = Tester(topo)
        tester.contents(s, ['pint'])
        tester.test(self.test_ctxtype, self.test_config)

    @unittest.skipIf('CP4D_URL' not in os.environ and 'STREAMS_REST_URL' not in os.environ, 'CP4D_URL/STREAMS_REST_URL not set')
    def test_add_pip_package_whl_from_url(self):
        topo = Topology()
        topo.add_pip_package('https://github.com/IBMStreams/streamsx.topology/raw/master/test/python/topology/test_package_whl/whl/tstexamplepkg-1.0-py3-none-any.whl', name='tstexamplepkg')
        s = topo.source([1])
        s = s.map(lambda x : __import__('tstexamplepkg').__name__)
        tester = Tester(topo)
        tester.contents(s, ['tstexamplepkg'])
        tester.test(self.test_ctxtype, self.test_config)

class TestSasPackages(TestDistributedPackages):

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


