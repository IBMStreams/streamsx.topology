import unittest
from datetime import timedelta

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit

import spl_tests_utils as stu

class TestCheckpointing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    # Source operator
    def test_source(self):
        topo = Topology("test")
        topo.checkpoint_period = timedelta(seconds=1)
        #s = topo.source(TimeCounter(iterations=30, period=0.1))
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        bop = op.Source(topo, "com.ibm.streamsx.topology.pytest.checkpoint::TimeCounter", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':30,'period':0.1})
        s = bop.stream
        tester = Tester(topo)
        #tester.contents(s, range(0,30))
        tester.tuple_count(s, 30)
        tester.contents(s, list(zip(range(0,30))))

        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)
