# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import test_vers

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestUDP(unittest.TestCase):

  def setUp(self):
      Tester.setup_standalone(self)

  def test_TopologyParallel(self):
      for width in (1,3):
          with self.subTest(width=width):
              topo = Topology("test_TopologyParallel" + str(width))
              hw = topo.source(range(17,142))
              hwp = hw.parallel(width)
              hwm = hwp.map(lambda tuple : tuple + 19)
              hwep = hwm.end_parallel()

              tester = Tester(topo)
              tester.contents(hwep, range(36,161), ordered=width==1)
              tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedUDP(TestUDP):
  def setUp(self):
      Tester.setup_standalone(self)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixUDP(TestUDP):
  def setUp(self):
      Tester.setup_streaming_analytics(self, force_remote_build=True)
