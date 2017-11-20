# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import unittest
import test_vers
from common_tests import CommonTests, logger
from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams
from streamsx.rest import StreamsConnection

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestRestFeaturesLocal(CommonTests):
    @classmethod
    def setUpClass(cls):
        cls.logger = logger

    def setUp(self):
        Tester.setup_distributed(self)
        self.sc = StreamsConnection()
        self.sc.session.verify = False
        self.test_config[ConfigParams.STREAMS_CONNECTION] = self.sc
