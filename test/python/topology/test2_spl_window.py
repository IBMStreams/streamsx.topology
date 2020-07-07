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
from streamsx.topology.context import JobConfig
from streamsx.topology.context import ConfigParams
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

    def test_sliding_count_stv(self):
        for step in [1, 3]:
            with self.subTest(step=step):
                topo = Topology()
                b = op.Source(topo, "spl.utility::Beacon",
                    'tuple<uint64 seq>',
                    params = {'iterations':12})
                b.seq = b.output('IterationCount()')
                s = b.stream
 
                count = topo.create_submission_parameter('count', 4)
                window = s.last(count).trigger(step)

                agg = op.Map('spl.relational::Aggregate', window,
                    schema = 'tuple<uint64 sum, uint64 max>')
                agg.sum = agg.output('Sum(seq)')
                agg.max = agg.output('Max(seq)')
 
                expected = []
                for i in range(4 + step - 2, 12, step):
                    expected.append({'sum': sum(range(i-3, i+1)), 'max': i})

                tester = Tester(topo)
                tester.contents(agg.stream, expected)
                tester.test(self.test_ctxtype, self.test_config)

    def test_sliding_count_stv_no_default(self):
        step =1
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'iterations':12})
        b.seq = b.output('IterationCount()')
        s = b.stream

        count = topo.create_submission_parameter('count', type_=int)
        window = s.last(count).trigger(step)

        agg = op.Map('spl.relational::Aggregate', window,
            schema = 'tuple<uint64 sum, uint64 max>')
        agg.sum = agg.output('Sum(seq)')
        agg.max = agg.output('Max(seq)')

        expected = []
        for i in range(4 + step - 2, 12, step):
            expected.append({'sum': sum(range(i-3, i+1)), 'max': i})

        jc = JobConfig()
        jc.submission_parameters['count'] = 4
        jc.add(self.test_config)

        tester = Tester(topo)
        tester.contents(agg.stream, expected)
        tester.test(self.test_ctxtype, self.test_config)


    def test_sliding_time_stv(self):
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'iterations':12})
        b.seq = b.output('IterationCount()')
        s = b.stream

        time = topo.create_submission_parameter('time', 2)
        window = s.lastSeconds(time).trigger(1)

        agg = op.Map('spl.relational::Aggregate', window,
            schema = 'tuple<uint64 sum, uint64 max>')
        agg.sum = agg.output('Sum(seq)')
        agg.max = agg.output('Max(seq)')

        tester = Tester(topo)
        tester.tuple_count(agg.stream, 12)
        tester.test(self.test_ctxtype, self.test_config)

    def test_sliding_time_stv_no_default(self):
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq>',
            params = {'iterations':12})
        b.seq = b.output('IterationCount()')
        s = b.stream

        wtime = topo.create_submission_parameter(name='secs', type_=int)
        window = s.lastSeconds(wtime).trigger(1)

        agg = op.Map('spl.relational::Aggregate', window,
            schema = 'tuple<uint64 sum, uint64 max>')
        agg.sum = agg.output('Sum(seq)')
        agg.max = agg.output('Max(seq)')

        jc = JobConfig()
        jc.submission_parameters['secs'] = 2
        jc.add(self.test_config)

        tester = Tester(topo)
        tester.tuple_count(agg.stream, 12)
        tester.test(self.test_ctxtype, self.test_config)


class TestDistributedSPLWindow(TestSPLWindow):
    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False
