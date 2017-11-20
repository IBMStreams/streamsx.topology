# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import unittest
import logging

logger = logging.getLogger('string_tests')
logger.setLevel(logging.INFO)

class TestStringFeatures(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        pass

    def test_upper(self):
        logger.warning("running test_upper")
        self.assertEqual("asdf".upper(), "ASDF")

    @classmethod
    def tearDownClass(cls):
        pass



