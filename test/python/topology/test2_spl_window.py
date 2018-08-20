# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
import sys
import itertools
import threading

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
import streamsx.spl.op as op


class TestSPLWindow(unittest.TestCase):
    """ Test invocations of SPL operators from Python topology.
    """
    _multiprocess_can_split_ = True

    # Fake out subTest
    if sys.version_info.major == 2:
        def subTest(self, **args): return threading.Lock()

    def setUp(self):
        Tester.setup_standalone(self)

    def test_sliding_count(self):
        for step in [1, 3]:
            with self.subTest(step=step):
                topo = Topology()
                b = op.Source(topo, "spl.utility::Beacon",
                    'tuple<uint64 seq>',
                    params = {'iterations':12})
                b.seq = b.output('IterationCount()')
                s = b.stream
 
                agg = op.Map('spl.relational::Aggregate', s.last(4).trigger(step),
                    schema = 'tuple<uint64 sum, uint64 max>')
                agg.sum = agg.output('Sum(seq)')
                agg.max = agg.output('Max(seq)')
 
                expected = []
                for i in range(4 + step - 2, 12, step):
                    expected.append({'sum': sum(range(i-3, i+1)), 'max': i})

                tester = Tester(topo)
                tester.contents(agg.stream, expected)
                tester.test(self.test_ctxtype, self.test_config)
