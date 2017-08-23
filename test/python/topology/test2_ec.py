# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import tempfile

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.ec as ec

import test_vers

def read_config_file(name):
    path = os.path.join(ec.get_application_directory(), 'etc', name)
    with open(path, encoding='utf-8') as f:
        return f.read()

class EcSource(object):
    def __init__(self, val):
        self.val = val
        self.ev = None

    def __call__(self):
        return [(self.val, self.ev)]

    def __enter__(self):
        self.ev = 'EcSource_enter'

    def __exit__(self, a, b, c):
        pass

class EcFilter(object):
    def __init__(self, val):
        self.val = val
        self.ev = None

    def __call__(self, tuple):
        return self.val == self.ev

    def __enter__(self):
        self.ev = self.val

    def __exit__(self, a, b, c):
        pass

class EcMap(object):
    def __init__(self, val):
        self.val = val
        self.ev = None

    def __call__(self, tuple):
        return tuple + (self.val, self.ev)

    def __enter__(self):
        self.ev = 'EcMap_enter'

    def __exit__(self, a, b, c):
        pass

class EcForEach(object):
    def __init__(self):
        self.ev = False

    def __call__(self, tuple):
        if not self.ev:
            raise AssertionError("__enter__ not called")
        
    def __enter__(self):
        self.ev = True

    def __exit__(self, a, b, c):
        pass

def get_sys_argv():
    import sys as sys_ec_test
    return sys_ec_test.argv

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestEc(unittest.TestCase):

  def setUp(self):
      Tester.setup_standalone(self)

  def test_enter_called(self):
      topo = Topology()
      s = topo.source(EcSource('A211'))
      s = s.filter(EcFilter('F243'))
      s.for_each(EcForEach())
      s = s.map(EcMap('M523'))
      tester = Tester(topo)
      tester.contents(s, [('A211', 'EcSource_enter', 'M523', 'EcMap_enter')])
      tester.test(self.test_ctxtype, self.test_config)

  def test_sys_argv(self):
      topo = Topology()
      s = topo.source(get_sys_argv)
      tester = Tester(topo)
      tester.contents(s, [''])
      tester.test(self.test_ctxtype, self.test_config)

  def test_app_dir(self):
      fn = None
      with tempfile.NamedTemporaryFile(delete=False) as temp:
          temp.write("SomeConfig".encode('utf-8'))
          temp.flush()
          fn = temp.name

      topo = Topology()
      topo.add_file_dependency('etc', temp.name)

      s = topo.source(['A'])
      s = s.filter(lambda x : os.path.isdir(ec.get_application_directory()))
      bfn = os.path.basename(fn)
      s = s.map(lambda x : read_config_file(bfn))

      tester = Tester(topo)
      tester.contents(s, ['SomeConfig'])
      tester.test(self.test_ctxtype, self.test_config)

      os.remove(fn)
      
@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedEc(TestEc):
  def setUp(self):
      Tester.setup_standalone(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixEc(TestEc):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
