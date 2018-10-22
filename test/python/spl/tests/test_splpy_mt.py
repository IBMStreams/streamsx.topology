import unittest
from datetime import timedelta

from streamsx.topology.topology import *
from streamsx.topology import schema
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit

import spl_tests_utils as stu

class TestMT(unittest.TestCase):
    _multiprocess_can_split_ = True

    @classmethod
    def setUpClass(cls):
        """Extract Python operators in toolkit"""
        stu._extract_tk('testtkpy')

    def setUp(self):
        Tester.setup_standalone(self)

    # Source operator
    def test_mt(self):
        topo = Topology()
        N = 1000
        streamsx.spl.toolkit.add_toolkit(topo, stu._tk_dir('testtkpy'))
        b1 = op.Source(topo, "spl.utility::Beacon", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':N})
        b1.f = b1.output('(int32)IterationCount()')

        b2 = op.Source(topo, "spl.utility::Beacon", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':N})
        b2.f = b2.output(str(N) + ' + (int32)IterationCount()')

        b3 = op.Source(topo, "spl.utility::Beacon", schema.StreamSchema('tuple<int32 f>').as_tuple(), params={'iterations':N})
        b3.f = b3.output(str(2*N) + ' + (int32)IterationCount()')

        s1 = b1.stream.low_latency()
        s2 = b2.stream.low_latency()
        s3 = b3.stream.low_latency()

        s = s1.union({s2, s3})

        f = op.Map("com.ibm.streamsx.topology.pytest.mt::MTFilter", s)
        m = op.Map("com.ibm.streamsx.topology.pytest.mt::MTMap", f.stream)
        op.Sink("com.ibm.streamsx.topology.pytest.mt::MTForEach", f.stream)

        cr = m.stream.flat_map()

        tester = Tester(topo)
        tester.tuple_count(m.stream, 3*N)
        tester.contents(cr, range(3*N), ordered=False)
        tester.test(self.test_ctxtype, self.test_config)
