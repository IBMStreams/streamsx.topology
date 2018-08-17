# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
import logging
import tempfile
import codecs

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.ec as ec

def _trc_msg_direct(level):
    atm = (level, "direct _ec message:" + str(level*77), "A1,B2,python", "MyFile.py", "MyFunc", 4242)
    import _streamsx_ec
    _streamsx_ec._app_trc(atm)
    ctl = _streamsx_ec._app_trc_level()
    print("Current Trace level:", ctl, logging.getLevelName(ctl))

def _log_msg_direct(level):
    atm = (level, "direct _ec log message:" + str(level*77), "C1,D2,python", "MyLogFile.py", "MyLogFunc", 2189)
    import _streamsx_ec
    _streamsx_ec._app_log(atm)
    ctl = _streamsx_ec._app_log_level()
    print("Current Log level:", ctl, logging.getLevelName(ctl))

def _trc_msg(msg):
    logger = logging.getLogger()
    logger.critical("Critical:%s", msg)
    logger.error("Error:" + msg)
    logger.warning("Warning:" + msg)
    logger.info("Info:%s")
    logger.debug("Debug:" + msg)
    ctl = logger.getEffectiveLevel()
    print("Current Root logger Trace level:", ctl, logging.getLevelName(ctl))

def _log_msg(msg):
    logger = logging.getLogger('com.ibm.streams.log')
    logger.critical("Critical:" + msg)
    logger.error("Error:" + msg)
    logger.warning("Warning:" + msg)
    logger.info("Info:" + msg)
    ctl = logger.getEffectiveLevel()
    print("Current Stream log logger level:", ctl, logging.getLevelName(ctl))

def read_config_file(name):
    path = os.path.join(ec.get_application_directory(), 'etc', name)
    with codecs.open(path, encoding='utf-8') as f:
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

class TestEc(unittest.TestCase):
  _multiprocess_can_split_ = True

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

      bfn = os.path.basename(fn)
      topo = Topology()
      rtpath = topo.add_file_dependency(temp.name, 'etc')
      self.assertEqual('etc/' + bfn, rtpath)

      s = topo.source(['A'])
      s = s.filter(lambda x : os.path.isdir(ec.get_application_directory()))
      s = s.map(lambda x : read_config_file(bfn))

      tester = Tester(topo)
      tester.contents(s, ['SomeConfig'])
      tester.test(self.test_ctxtype, self.test_config)

      os.remove(fn)

  def test_app_trc_direct(self):
      topo = Topology()
      s = topo.source([40,30,20,10,99])
      at = s.filter(lambda x : x != 99)
      at.for_each(_trc_msg_direct)
      tester = Tester(topo)
      tester.tuple_count(s, 5)
      tester.test(self.test_ctxtype, self.test_config)

  def test_app_log_direct(self):
      topo = Topology()
      s = topo.source([40,30,20,99])
      at = s.filter(lambda x : x != 99)
      at.for_each(_log_msg_direct)
      tester = Tester(topo)
      tester.tuple_count(s, 4)
      tester.test(self.test_ctxtype, self.test_config)
   
  def test_app_trc(self):
      topo = Topology()
      s = topo.source(['msg1', 'msg2你好'])
      s.for_each(_trc_msg)
      tester = Tester(topo)
      tester.tuple_count(s, 2)
      tester.test(self.test_ctxtype, self.test_config)

  def test_app_log(self):
      topo = Topology()
      s = topo.source(['logmsg1', 'logmsg2你好'])
      s.for_each(_log_msg)
      tester = Tester(topo)
      tester.tuple_count(s, 2)
      tester.test(self.test_ctxtype, self.test_config)
      
class TestDistributedEc(TestEc):
  def setUp(self):
      Tester.setup_distributed(self)

class TestBluemixEc(TestEc):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
