# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import itertools
from streamsx.topology.topology import *

def f42():
    pass

class cool_class(object):
    def __init__(self):
        pass
    def __call__(self, t):
        pass

class TestNames(unittest.TestCase):

  def test_names(self):
     topo = Topology("Abc")

     s1 = topo.source([], name="CoolSource")
     self.assertEqual(s1.name, "CoolSource")

     s1a = topo.source([], name="CoolSource")
     self.assertEqual(s1a.name, "CoolSource_2")

     s1b = topo.source([], name="CoolSource")
     self.assertEqual(s1b.name, "CoolSource_3")

     s2 = topo.source(f42)
     self.assertEqual(s2.name, "f42")
     s2a = topo.source(f42)
     self.assertEqual(s2a.name, "f42_2")

     s3 = topo.source(lambda : [])
     self.assertEqual(s3.name, "source_lambda")
     s3a = topo.source(lambda : [])
     self.assertEqual(s3a.name, "source_lambda_2")

     s4 = topo.source(cool_class())
     self.assertEqual(s4.name, "cool_class")

     s4a = topo.source(cool_class())
     self.assertEqual(s4a.name, "cool_class_2")

     s5 = topo.source([])
     self.assertEqual(s5.name, "list_2")
     s5a = topo.source([])
     self.assertEqual(s5a.name, "list_3")

     s6 = topo.source(itertools.repeat('Fred'))
     self.assertEqual(s6.name, "repeat")
     s6a = topo.source(itertools.repeat('Fred'))
     self.assertEqual(s6a.name, "repeat_2")

     s6.for_each(name="End", func=cool_class)

     s7 = s6.filter(cool_class, name="mYF")
     self.assertEqual(s7.name, "mYF")
     s8 = s6.filter(cool_class)
     self.assertEqual(s8.name, "cool_class_3")

     s9 = s6.map(cool_class, name="mYM")
     self.assertEqual(s9.name, "mYM")
     s10 = s6.map(cool_class)
     self.assertEqual(s10.name, "cool_class_4")

     s11 = s6.flat_map(cool_class, name="mYFM")
     self.assertEqual(s11.name, "mYFM")
     s12 = s6.flat_map(cool_class)
     self.assertEqual(s12.name, "cool_class_5")
     
