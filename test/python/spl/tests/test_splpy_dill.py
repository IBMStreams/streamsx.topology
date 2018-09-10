# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
import unittest
import dill
import ops_dill

class TestDillState(unittest.TestCase):
    """ 
    Test ability to pickle/dill operators.
    """
    def test_filter(self):
        op = ops_dill.F1(7)
        self.assertEqual(0, op.c)
        self.assertEqual(7, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(0, op.c)
        self.assertEqual(7, op.v)

        self.assertTrue(op(3))
        self.assertFalse(op(8))
        self.assertEqual(2, op.c)
        self.assertEqual(7, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(2, op.c)
        self.assertEqual(7, op.v)

    def test_filter_getstate(self):
        op = ops_dill.F2(11)
        self.assertEqual(42, op.c)
        self.assertEqual(11, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(42+99, op.c)
        self.assertEqual(11, op.v)

        self.assertTrue(op(3))
        self.assertFalse(op(12))
        self.assertEqual(42+2+99, op.c)
        self.assertEqual(11, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(42+99+2+99, op.c)
        self.assertEqual(11, op.v)

    def test_map(self):
        op = ops_dill.M1(9)
        self.assertEqual(0, op.c)
        self.assertEqual(9, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(0, op.c)
        self.assertEqual(9, op.v)

        self.assertEqual(12, op(3)[0])
        self.assertEqual(17, op(8)[0])
        self.assertEqual(2, op.c)
        self.assertEqual(9, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(2, op.c)
        self.assertEqual(9, op.v)

    def test_map_getstate(self):
        op = ops_dill.M2(17)
        self.assertEqual(0, op.c)
        self.assertEqual(17, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(43, op.c)
        self.assertEqual(17, op.v)

        self.assertEqual(20, op(3)[0])
        self.assertEqual(25, op(8)[0])
        self.assertEqual(43+2, op.c)
        self.assertEqual(17, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(43+2+43, op.c)
        self.assertEqual(17, op.v)

    def test_for_each(self):
        op = ops_dill.FE1(91)
        self.assertEqual(0, op.c)
        self.assertEqual(91, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(0, op.c)
        self.assertEqual(91, op.v)

        self.assertIsNone(op(3))
        self.assertIsNone(op(8))
        self.assertEqual(91+91+3+8, op.c)
        self.assertEqual(91, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(91+91+3+8, op.c)
        self.assertEqual(91, op.v)

    def test_for_each_getstate(self):
        op = ops_dill.FE2(63)
        self.assertEqual(0, op.c)
        self.assertEqual(63, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(74, op.c)
        self.assertEqual(63, op.v)

        self.assertIsNone(op(3))
        self.assertIsNone(op(8))
        self.assertEqual(63+63+3+8+74, op.c)
        self.assertEqual(63, op.v)

        op = dill.loads(dill.dumps(op))
        self.assertEqual(63+63+3+8+74+74, op.c)
        self.assertEqual(63, op.v)
