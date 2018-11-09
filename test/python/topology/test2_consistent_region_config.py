from streamsx.topology.topology import *
from streamsx.topology.state import ConsistentRegionConfig

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
        self.assertEqual(config.drain_timeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.reset_timeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.max_consecutive_attempts, self._DEFAULT_ATTEMPTS)

    def test_op_driven_simple_drain_timeout(self):
        config = ConsistentRegionConfig.operator_driven(drain_timeout=14)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drain_timeout, 14)
        self.assertEqual(config.reset_timeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.max_consecutive_attempts, self._DEFAULT_ATTEMPTS)

    def test_op_driven_simple_reset_timeout(self):
        config = ConsistentRegionConfig.operator_driven(reset_timeout=14)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drain_timeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.reset_timeout, 14)
        self.assertEqual(config.max_consecutive_attempts, self._DEFAULT_ATTEMPTS)

    def test_op_driven_simple_attempts(self):
        config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=14)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drain_timeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.reset_timeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.max_consecutive_attempts, 14)

    def test_op_driven_all(self):
        config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=14, drain_timeout=1, reset_timeout=2)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
        self.assertEqual(config.drain_timeout, 1)
        self.assertEqual(config.reset_timeout, 2)
        self.assertEqual(config.max_consecutive_attempts, 14)

    def test_op_driven_drain_timeout(self):
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drain_timeout=-1)

        # timedelta, positive
        config = ConsistentRegionConfig.operator_driven(drain_timeout=timedelta(seconds=1))
        self.assertEqual(config.drain_timeout, timedelta(seconds=1))

        # timedelta, zero
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drain_timeout=timedelta(seconds=0))

        # timedelta, negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drain_timeout=timedelta(seconds=-1))

        # can cast to float
        config = ConsistentRegionConfig.operator_driven(drain_timeout="8.2")
        self.assertEqual(config.drain_timeout, "8.2")

        # cannot cast to float
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(drain_timeout="clogged")

    def test_op_driven_reset_timeout(self):
        # negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(reset_timeout=-1)

        # timedelta, positive
        config = ConsistentRegionConfig.operator_driven(reset_timeout=timedelta(seconds=1))
        self.assertEqual(config.reset_timeout, timedelta(seconds=1))

        # timedelta, zero
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(reset_timeout=timedelta(seconds=0))

        # timedelta, negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(reset_timeout=timedelta(seconds=-1))

        # can cast to float
        config = ConsistentRegionConfig.operator_driven(reset_timeout="8.2")
        self.assertEqual(config.reset_timeout, "8.2")

        # cannot cast to float
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(reset_timeout="clogged")

    # test the validation of max_consecutive_attempts
    def test_op_driven_attempts(self):

        # negative
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=-1)

        # zero
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=0)

        # one
        config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=1)
        self.assertEqual(config.max_consecutive_attempts, 1)

        # exactly 0x7FFFFFFF
        config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=0x7FFFFFFF)
        self.assertEqual(config.max_consecutive_attempts, 0x7FFFFFFF)

        # greater than 0x7FFFFFF
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=0x80000000)

        # float literal, exactly integral
        config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=12.0)

        self.assertEqual(config.max_consecutive_attempts, 12.0)

        # not exactly integral
        with self.assertRaises(ValueError):
            config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts=12.2)

        # a string that can be cast to a valid integral value
        config = ConsistentRegionConfig.operator_driven(max_consecutive_attempts="14")

        self.assertEqual(config.max_consecutive_attempts, "14")

    def test_periodic(self):
        config = ConsistentRegionConfig.periodic(14.0)
        self.assertEqual(config.trigger, ConsistentRegionConfig.Trigger.PERIODIC)
        self.assertEqual(config.period, 14.0)

        # verify the correct defaults have been applied
        self.assertEqual(config.drain_timeout, self._DEFAULT_DRAIN_TIMEOUT)
        self.assertEqual(config.reset_timeout, self._DEFAULT_RESET_TIMEOUT)
        self.assertEqual(config.max_consecutive_attempts, self._DEFAULT_ATTEMPTS)

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
