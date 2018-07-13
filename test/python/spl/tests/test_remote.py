# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit

import spl_tests_utils as stu

class TestRemote(unittest.TestCase):
    """ Test remote build with a SPL python primitive operator
        that has not been extracted, so the extraction and
        pip requirements install is all done remotly.
    """
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

    def test_with_pint(self):
        schema='tuple<float64 temp>'
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy_remote'))
        s = topo.source([0, 100, 28.5])
        s = s.map(lambda t : {'temp':t}, schema=schema)
        fh = op.Map(
            "com.ibm.streamsx.topology.pytest.temps::ToFahrenheit",
            s)
        r = fh.stream.map(lambda x : x['temp'])

        tester = Tester(topo)
        # We round off to ints because pint temp conversion
        # is actually incorrect!
        tester.contents(r, [32.0, 212.0, 83.0])
        tester.test(self.test_ctxtype, self.test_config)
