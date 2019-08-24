# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import decimal
import random
import time
from streamsx.topology.tester import Tester

from datetime import datetime

import test_functions
from test_utilities import standalone

try:
    import numpy as np
    skip_numpy = False
except ImportError:
    skip_numpy = True

from streamsx.topology.topology import *
from streamsx.topology import schema
import streamsx.topology.context

class Gen(object):
    def __init__(self, n):
        self.n = n

    def __iter__(self):
        self.c = 0
        self.start = time.time()
        self.dt = datetime.now()
        return self

    def __next__(self):
        self.c += 1
        if self.c <= self.n:
            return {'id': random.randint(0, 1000), 'ts':time.time(), 'value': random.random() }
        end = time.time()
        dur = end - self.start
        raise StopIteration()

class M(object):
    def __call__(self, tuple_):
        tuple_['dt'] = datetime.now()
        return decimal.Decimal(tuple_['value'])

def _rand_msg():
    import streamsx.ec
    for _ in range(2000):
        streamsx.ec.is_active()
        datetime.now()
        yield random.randint(0,3)
        time.sleep(0.001)

class TestLambdas(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
      Tester.setup_standalone(self)
      #Tester.setup_distributed(self)

  def test_TopologyLambdaFilter(self):
      topo = Topology("test_TopologyLambdaFilter")
      hw = topo.source(['Hello', 'World'])
      hwf = hw.filter(lambda t : "Wor" in t)

      tester = Tester(topo)
      tester.contents(hwf, ['World'])
      tester.test(self.test_ctxtype, self.test_config)

  def test_local_capture(self):
      topo = Topology("test_local_capture")
      n = topo.source([1,2,4])
      x = 93
      n = n.map(lambda v : v + x)

      tester = Tester(topo)
      tester.contents(n, [94,95,97])
      tester.tuple_count(n, 3)
      tester.test(self.test_ctxtype, self.test_config)

  def test_lambda_module_refs(self):

      topo = Topology("test_lambda_module_refs")
      sr = topo.source(_rand_msg, name='RM')
      #sevs = hw.split(3, lambda m: M.get(m, -1), names=['high', 'medium', 'low'], name='SeveritySplit')

      tt = 33

      s1 = topo.source(lambda : [tt], name="S1")
      s2 = topo.source(lambda : [streamsx.ec.is_active()], name="S2")
      s3 = topo.source(lambda : [random.randint(0,99)], name="S3")

      s1.for_each(lambda x : tt, name='S1E')
      s2.for_each(lambda x : streamsx.ec.is_active(), name='S2E')
      s3.for_each(lambda x : None, name='S3E')

      srm1 = sr.map(lambda x : tt, name='SMR1')
      srm2 = sr.map(lambda x : streamsx.ec.is_active(), name='SMR2')
      srm3 = sr.map(lambda x : random.randint(0, 100), name='SMR3')

      srf1 = srm1.filter(lambda x : tt, name='SRF1')
      srf2 = srm2.filter(lambda x : streamsx.ec.is_active() and datetime.now(), name='SRF2')
      srf3 = srm3.filter(lambda x : random.randint(0,2), name='SRF3')

      tester = Tester(topo)
      # Mostly testing this runs without failing due to
      # unresolved modules
      tester.contents(s1, [33])
      tester.contents(s2, [True])
      tester.tuple_count(s3, 1)
      tester.tuple_count(srf1, 100, exact=False)
      tester.tuple_count(srf2, 100, exact=False)
      tester.tuple_count(srf3, 30, exact=False)
      tester.test(self.test_ctxtype, self.test_config)

  @unittest.skipIf(skip_numpy, "Numpy not available")
  def test_TopologyLambdaModule(self):
      "Lambda using an imported module"
      topo = Topology("test_TopologyLambdaModule")
      hw = topo.source(['A'])
      hw = hw.map(lambda t: t + ":" +  str(round(np.cos(0.4),3)))

      tester = Tester(topo)
      tester.contents(hw, ['A:' + str(round(np.cos(0.4),3))])
      tester.test(self.test_ctxtype, self.test_config)

  def test_modules_in_main_class(self):
      topo = Topology("test_modules_in_main_class")
      s = topo.source(Gen(7))
      s = s.map(M())
      tester = Tester(topo)
      tester.tuple_count(s, 7)
      tester.test(self.test_ctxtype, self.test_config)

  def test_closure_vars(self):
      topo = Topology("test_closure_vars")
      s = topo.source([1,2,3,4,5,6,7,8,9])
      cv_y = 7.0
      s = s.filter(lambda x : x < cv_y)

      tester = Tester(topo)
      tester.contents(s, [1,2,3,4,5,6])
      tester.test(self.test_ctxtype, self.test_config)

if __name__ == '__main__':
    unittest.main()
