# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import test_vers

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestUnicode(unittest.TestCase):
  def setUp(self):
      Tester.setup_standalone(self)

  def test_strings(self):
     """ Test strings that are unicode.
     """
     topo = Topology()
     ud = []
     ud.append('⡍⠔⠙⠖ ⡊ ⠙⠕⠝⠰⠞ ⠍⠑⠁⠝ ⠞⠕ ⠎⠁⠹ ⠹⠁⠞ ⡊ ⠅⠝⠪⠂ ⠕⠋ ⠍⠹')
     ud.append('2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm')
     ud.append('многоязычных')
     ud.append("Arsenal hammered 5-1 by Bayern again")
     s = topo.source(ud)
     sas = s.as_string()
     sd = s.map(lambda s : {'val': s + "_test_it!"})
     tester = Tester(topo)
     tester.contents(s, ud)
     tester.contents(sas, ud)
     dud = []
     for v in ud:
         dud.append({'val': v + "_test_it!"})
     tester.contents(sd, dud)
    
     tester.test(self.test_ctxtype, self.test_config)
     print(tester.result)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedUnicode(TestUnicode):
  def setUp(self):
      Tester.setup_distributed(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixUnicode(TestUnicode):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
