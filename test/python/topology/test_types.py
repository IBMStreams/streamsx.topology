# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import dill
import datetime
import random

from streamsx.spl.types import Timestamp
from streamsx.topology import schema

class TestTypes(unittest.TestCase):

  def test_Timestamp(self):
      s = random.randint(0, 999999999999)
      ns = random.randint(0, 1000000000)
      mid = random.randint(0, 200000)
      ts = Timestamp(s, ns, mid)
      self.assertEqual(s, ts.seconds)
      self.assertEqual(ns, ts.nanoseconds)
      self.assertEqual(mid, ts.machine_id)

      t = ts.tuple()
      self.assertEqual(3, len(t))
      self.assertEqual(s, t[0])
      self.assertEqual(ns, t[1])
      self.assertEqual(mid, t[2])

      s = random.randint(0, 999999999999)
      ns = random.randint(0, 1000000000)
      ts = Timestamp(s, ns)
      self.assertEqual(s, ts.seconds)
      self.assertEqual(ns, ts.nanoseconds)
      self.assertEqual(0, ts.machine_id)

      ft = ts.time()
      self.assertIsInstance(ft, float)
      eft = s + (ns / 1000.0 / 1000.0 / 1000.0)
      self.assertEqual(eft, ft)
      
  def test_TimestampToDatetime(self):
      # 2017-06-04 11:48:25.008880
      ts = Timestamp(1496576905, 888000000, 0)
      dt = ts.datetime()
      self.assertIsInstance(dt, datetime.datetime)

      self.assertIsNone(dt.tzinfo)
      ts = Timestamp(1496576905, 888000000, 0)
      
  def test_TimestampToDatetime(self):
      # 2017-06-04 11:48:25.008880
      ts = Timestamp(1496576905, 888000000, 0)
      dt = ts.datetime()
      self.assertIsInstance(dt, datetime.datetime)

      self.assertIsNone(dt.tzinfo)

      self.assertEqual(2017, dt.year)
      self.assertEqual(6, dt.month)
      self.assertEqual(4, dt.day)

      self.assertEqual(11, dt.hour)
      self.assertEqual(48, dt.minute)
      self.assertEqual(25, dt.second)

  def test_DatetimeToTimestamp(self):
      dt = datetime.datetime.now()
      ts = Timestamp.from_datetime(dt)
      self.assertEqual(dt, ts.datetime())
      self.assertEqual(0, ts.machine_id)

      ts = Timestamp.from_datetime(dt, 892)
      self.assertEqual(dt, ts.datetime())
      self.assertEqual(892, ts.machine_id)
