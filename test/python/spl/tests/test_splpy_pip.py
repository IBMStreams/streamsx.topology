# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.scripts.extract

import spl_tests_utils as stu

def down_a_pint():
    try:
        import pint
        return "PintImported"
    except ImportError:
        return "NoPintsForYou"

def down_a_pint_source():
    return itertools.repeat(down_a_pint(), 1)

class TestLocal(unittest.TestCase):
    def test_no_pints(self):
        self.assertEqual("NoPintsForYou", down_a_pint())

class TestPipInstalls(unittest.TestCase):
    """ Test remote pip install of packages.
    """

    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy_pip_op')
        stu._extract_tk('testtkpy_pip_toolkit')

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

    def test_verify_no_pint(self):
        """ Verify pint is not installed on the service """
        topo = Topology()
        s = topo.source(down_a_pint_source)
        tester = Tester(topo)
        tester.contents(s, ['NoPintsForYou'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_verify_operator_pip_install(self):
        """ Verify pint is installed by the operator module """
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy_pip_op'))
        s = topo.source(['a'])
        s = s.as_string()

        fp = op.Map(
            "com.ibm.streamsx.topology.pytest.pypip::find_a_pint",
            s)
        tester = Tester(topo)
        tester.contents(fp.stream, ['RTOP_PintImported'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_verify_toolkit_pip_install(self):
        """ Verify pint is installed by requirements.txt in toolkit """
        topo = Topology()
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy_pip_toolkit'))
        s = topo.source(['a'])
        s = s.as_string()

        fp = op.Map(
            "com.ibm.streamsx.topology.pytest.pypip::find_a_pint_toolkit",
            s)
        tester = Tester(topo)
        tester.contents(fp.stream, ['RTTK_PintImported'])
        tester.test(self.test_ctxtype, self.test_config)
