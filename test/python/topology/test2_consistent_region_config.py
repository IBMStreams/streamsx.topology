from streamsx.topology.topology import *
from streamsx.topology.consistent import ConsistentRegionConfig

import unittest
from datetime import timedelta

# Test the ConsistentRegionConfig class.
class TestConsistentRegionConfig(unittest.TestCase):
    _DEFAULT_DRAIN_TIMEOUT = 180.0
    _DEFAULT_RESET_TIMEOUT = 180.0
    _DEFAULT_ATTEMPTS = 5

    def test_op_driven(self):
        config = ConsistentRegionConfig.operator_driven()
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        # verify the correct defaults have been applied
        self.assertEqual(config.drainTimeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.resetTimeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.maxConsecutiveAttempts, self._DEFAULT_ATTEMPTS)

    def test_op_driven_simple_drainTimeout(self):
        config = ConsistentRegionConfig.operator_driven(drainTimeout=14)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drainTimeout, 14)
        self.assertEqual(config.resetTimeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.maxConsecutiveAttempts, self._DEFAULT_ATTEMPTS)

    def test_op_driven_simple_resetTimeout(self):
        config = ConsistentRegionConfig.operator_driven(resetTimeout=14)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drainTimeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.resetTimeout, 14)
        self.assertEqual(config.maxConsecutiveAttempts, self._DEFAULT_ATTEMPTS)

    def test_op_driven_simple_attempts(self):
        config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=14)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drainTimeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.resetTimeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.maxConsecutiveAttempts, 14)

    def test_op_driven_all(self):
        config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=14, drainTimeout=1, resetTimeout=2)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drainTimeout, 1)
        self.assertEqual(config.resetTimeout, 2)
        self.assertEqual(config.maxConsecutiveAttempts, 14)

    def test_op_driven_drainTimeout(self):
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drainTimeout=-1)

        # timedelta, positive
        config = ConsistentRegionConfig.operator_driven(drainTimeout=timedelta(seconds=1))
        self.assertEqual(config.drainTimeout, timedelta(seconds=1))

        # timedelta, zero
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drainTimeout=timedelta(seconds=0))

        # timedelta, negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drainTimeout=timedelta(seconds=-1))

        # can cast to float
        config = ConsistentRegionConfig.operator_driven(drainTimeout="8.2")
        self.assertEqual(config.drainTimeout, "8.2")

        # cannot cast to float
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drainTimeout="clogged")

    def test_op_driven_resetTimeout(self):
        # negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(resetTimeout=-1)

        # timedelta, positive
        config = ConsistentRegionConfig.operator_driven(resetTimeout=timedelta(seconds=1))
        self.assertEqual(config.resetTimeout, timedelta(seconds=1))

        # timedelta, zero
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(resetTimeout=timedelta(seconds=0))

        # timedelta, negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(resetTimeout=timedelta(seconds=-1))

        # can cast to float
        config = ConsistentRegionConfig.operator_driven(resetTimeout="8.2")
        self.assertEqual(config.resetTimeout, "8.2")

        # cannot cast to float
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(resetTimeout="clogged")

    # test the validation of maxConsecutiveAttempts
    def test_op_driven_attempts(self):

        # negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=-1)

        # zero
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=0)

        # one
        config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=1)
        self.assertEqual(config.maxConsecutiveAttempts, 1)

        # exactly 0x7FFFFFFF
        config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=0x7FFFFFFF)
        self.assertEqual(config.maxConsecutiveAttempts, 0x7FFFFFFF)

        # greater than 0x7FFFFFF
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=0x80000000)

        # float literal, exactly integral
        config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=12.0)

        self.assertEqual(config.maxConsecutiveAttempts, 12.0)

        # not exactly integral
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts=12.2)

        # a string that can be cast to a valid integral value
        config = ConsistentRegionConfig.operator_driven(maxConsecutiveAttempts="14")

        self.assertEqual(config.maxConsecutiveAttempts, "14")

    def test_periodic(self):
        config = ConsistentRegionConfig.periodic(14.0)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.PERIODIC)
        self.assertEqual(config.period, 14.0)

        # verify the correct defaults have been applied
        self.assertEqual(config.drainTimeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.resetTimeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.maxConsecutiveAttempts, self._DEFAULT_ATTEMPTS)

    def test_periodic_period(self):

        # No period
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig(ConsistentRegionConfig.Trigger.PERIODIC)

        # zero (float)
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.periodic(0.0)

        # negative (float)
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.periodic(-1.0)

        # timedelta
        config = ConsistentRegionConfig.periodic(timedelta(seconds=41))
        self.assertEqual(config.period, timedelta(seconds=41))

        # zero (timedelta)
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.periodic(timedelta(seconds=0))

        # negative (timedelta)
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.periodic(timedelta(seconds=-8))

        # can cast to float
        config = ConsistentRegionConfig.periodic("4")
        self.assertEqual(config.period, "4")

        # cannot cast to float
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.periodic("table")

    def test_no_trigger(self):
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig()
