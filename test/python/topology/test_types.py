# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import dill
import pickle
import datetime
import time
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

      self.assertEqual(Timestamp, type(ts))
      self.assertTrue(isinstance(ts, tuple))

      t = ts.tuple()
      self.assertEqual(3, len(t))
      self.assertEqual(s, t[0])
      self.assertEqual(ns, t[1])
      self.assertEqual(mid, t[2])

      ts2 = Timestamp(ts.seconds, ts.nanoseconds, ts.machine_id)
      self.assertEqual(ts, ts2)

      now = time.time()
      ts2 = Timestamp(now, 0)
      self.assertEqual(int(now), ts2.seconds)
      self.assertEqual(0, ts2.nanoseconds)
      self.assertEqual(0, ts2.machine_id)

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

      tsft = Timestamp.from_time(23423.02, 93)
      self.assertEqual(23423, tsft.seconds)
      self.assertEqual(20*1000.0*1000.0, float(tsft.nanoseconds))
      self.assertEqual(93, tsft.machine_id)

  def test_timestamp_pickle(self):
     ts = Timestamp(1,2,3)
     tsp = pickle.loads(pickle.dumps(ts))
     self.assertEqual(ts, tsp)

  def test_timestamp_dill(self):
     ts = Timestamp(4,5,6)
     tsp = dill.loads(dill.dumps(ts))
     self.assertEqual(ts, tsp)

  def test_timestamp_now(self):
      now = time.time()
      ts = Timestamp.now()
      self.assertTrue(ts.time() >= now)

  def test_timestamp_nanos(self):
      Timestamp(1, 0)
      Timestamp(1, 999999999)
      self.assertRaises(ValueError, Timestamp, 1, -1)
      self.assertRaises(ValueError, Timestamp, 1, -2)
      self.assertRaises(ValueError, Timestamp, 1, 1000000000)
      self.assertRaises(ValueError, Timestamp, 1, 5000000000)
      
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
