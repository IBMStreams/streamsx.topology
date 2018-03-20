# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
from __future__ import print_function
import unittest
import os
import sys
import time

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit
 
def random_topic(prefix='RT_'):
    return prefix + ''.join(random.choice('ABCDEFGIHIJLK') for i in range(16))
SCHEMA = StreamSchema('tuple<uint64 seq>')

def slowme(t):
    if (t['seq'] % 20) == 0:
        time.sleep(0.2)
    return True
   
def check_lt_87(t):
    return t['seq'] < 87

class TestPubSub(unittest.TestCase):
    """ Test basic pub-sub in SPL
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def _publish(self, topo, N, topic, width=None, allow_filter=False):
        b = op.Source(topo, "spl.utility::Beacon",
            SCHEMA,
            params = {'initDelay':10.0, 'iterations':N})
        b.seq = b.output('IterationCount()')

        ps = b.stream
        if width:
            ps = ps.parallel(width=width)

        p = op.Sink('com.ibm.streamsx.topology.topic::Publish',
            ps,
            params = {'topic': topic},
            name='MSP')
        if allow_filter:
           p.params['allowFilter'] = True


    def _subscribe(self, topo, topic, direct=True, drop=None, filtered=False, extra=None):
        s = op.Source(topo,
               "com.ibm.streamsx.topology.topic::FilteredSubscribe" if filtered else "com.ibm.streamsx.topology.topic::Subscribe",
            SCHEMA,
            params = {'topic':topic, 'streamType':SCHEMA},
            name='MSS')

        if extra:
            s.params.update(extra)

        if not direct:
            s.params['connect'] = op.Expression.expression('com.ibm.streamsx.topology.topic::Buffered')
            if drop:
                s.params['bufferFullPolicy'] =  op.Expression.expression('Sys.' + drop)
            return s.stream.filter(slowme)

        return s.stream

    def _get_single_sub_op(self):
        job = self.tester.submission_result.job
        self.assertEqual('healthy', job.health)
        for op in job.get_operators():
           if op.name.startswith('MSS') and op.operatorKind == 'spl.relational::Filter':
              mss = op
        return mss

    def _get_single_sub_metrics(self, mss):
        nDropped = None
        nProcessed = None
        ip = mss.get_input_ports()[0]
        while nDropped is None or nProcessed is None:
            if nDropped is None:
                metrics = ip.get_metrics(name='nTuplesDropped')
                if metrics:
                    nDropped = metrics[0]
            if nProcessed is None:
                metrics = ip.get_metrics(name='nTuplesProcessed')
                if metrics:
                    nProcessed = metrics[0]
              
        return nDropped, nProcessed


    def check_single_sub(self):
        """
        Check we get all the tuples with none dropped
        with a single subcriber.
        """
        mss = self._get_single_sub_op()
        nDropped, nProcessed = self._get_single_sub_metrics(mss)
        while nProcessed.value < self.N:
            self.assertEqual(0, nDropped.value)
            time.sleep(2)
            nDropped, nProcessed = self._get_single_sub_metrics(mss)

        self.assertEqual(0, nDropped.value)
        self.assertEqual(self.N, nProcessed.value)

    def check_single_sub_drop(self):
        """
        Check we get all the tuples with none dropped
        with a single subcriber.
        """
        mss = self._get_single_sub_op()
        nDropped, nProcessed = self._get_single_sub_metrics(mss)
        while nDropped.value + nProcessed.value < self.N:
            time.sleep(2)
            nDropped, nProcessed = self._get_single_sub_metrics(mss)

        self.assertEqual(self.N, nDropped.value + nProcessed.value)
        self.assertTrue(nDropped.value > 0)

    def test_One2One(self):
        """Publish->Subscribe
        """
        N = 2466
        topic = random_topic()
        topo = Topology()

        # Subscriber
        s = self._subscribe(topo, topic)

        # Publisher
        self._publish(topo, N, topic)

        self.tester = Tester(topo)
        self.tester.run_for(15)
        self.N = N
        self.tester.tuple_count(s, N)
        self.tester.local_check = self.check_single_sub
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_One2OneNonDirect(self):
        """Publish->Subscribe with a buffered subscriber.
        """
        N = 3252
        topic = random_topic()
        topo = Topology()

        # Subscriber
        s = self._subscribe(topo, topic, direct=False)

        # Publisher
        self._publish(topo, N, topic)

        self.tester = Tester(topo)
        self.tester.run_for(15)
        self.N = N
        self.tester.tuple_count(s, N)
        self.tester.local_check = self.check_single_sub
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_One2OneNonDirectDropFirst(self):
        """Publish->Subscribe with a buffered subscriber.
        """
        N = 5032
        topic = random_topic()
        topo = Topology()

        # Subscriber
        s = self._subscribe(topo, topic, direct=False, drop='DropFirst')

        # Publisher
        self._publish(topo, N, topic)

        self.tester = Tester(topo)
        self.tester.run_for(15)
        self.N = N
        # 1000-2 for window & final mark
        self.tester.tuple_count(s, 998, exact=False)
        self.tester.local_check = self.check_single_sub_drop
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_One2OneNonDirectDropLast(self):
        """Publish->Subscribe with a buffered subscriber.
        """
        N = 5032
        topic = random_topic()
        topo = Topology()

        # Subscriber
        s = self._subscribe(topo, topic, direct=False, drop='DropLast')

        # Publisher
        self._publish(topo, N, topic)

        self.tester = Tester(topo)
        self.tester.run_for(15)
        self.N = N
        self.tester.tuple_count(s, 1000, exact=False)
        self.tester.local_check = self.check_single_sub_drop
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_UDPMany2One(self):
        """
        UDP publishers to a single subscriber.
        """
        N = 17342

        for pw in (1,5):
            topic = random_topic()
            topo = Topology()

            # Subscriber
            s = self._subscribe(topo, topic)

            # Publisher
            self._publish(topo, N, topic, width=pw)

            self.tester = Tester(topo)
            self.tester.run_for(15)
            self.tester.tuple_count(s, N)
            self.N = N
            self.tester.local_check = self.check_single_sub
            self.tester.test(self.test_ctxtype, self.test_config)

    def test_Many2One(self):
        """
        Many non-UDP publishers to a single subscriber.
        """
        N = 17342

        topic = random_topic()
        topo = Topology()

        # Subscriber
        s = self._subscribe(topo, topic)

        # Publisher
        M=3
        for i in range(M):
            self._publish(topo, N, topic)

        self.tester = Tester(topo)
        self.tester.run_for(15)
        self.tester.tuple_count(s, N*M)
        self.N = N*M
        self.tester.local_check = self.check_single_sub
        self.tester.test(self.test_ctxtype, self.test_config)

    def test_allow_filter_subscribe(self):
        N = 99

        topic = random_topic()
        topo = Topology()

        # Non-Filter Subscriber
        s = self._subscribe(topo, topic)

        # Publisher
        self._publish(topo, N, topic, allow_filter=True)

        self.tester = Tester(topo)
        self.tester.run_for(15)
        self.N = N
        self.tester.tuple_count(s, N)
        self.tester.local_check = self.check_single_sub
        self.tester.test(self.test_ctxtype, self.test_config)


    def test_allow_filter_filtered_subscribe(self):
        N = 201
        F = 87
        pd = os.path.dirname(os.path.dirname(__file__))
        ttk = os.path.join(pd, 'testtk')

        for af,d in [(False,False), (False,True), (True,False), (True,True)]:

            #af = c[0]
            #d = c[1]

            topic = random_topic()
            topo = Topology()

            streamsx.spl.toolkit.add_toolkit(topo, ttk)

            extra = {}
            extra['remoteFilter'] = 'seq < 87'
            extra['localFilterFunction'] = op.Expression.expression('testspl::affs')
            s = self._subscribe(topo, topic, direct=d, filtered=True, extra=extra)

            sf = s.filter(check_lt_87)

            # Publisher
            self._publish(topo, N, topic, allow_filter=af)

            self.tester = Tester(topo)
            self.tester.run_for(15)
            self.N = F
            self.tester.tuple_count(s, F)
            self.tester.tuple_check(s, check_lt_87)
            #self.tester.local_check = self.check_single_sub
            self.tester.test(self.test_ctxtype, self.test_config)


class TestPubSubCloud(TestPubSub):
    """ Test basic pub-sub in SPL
    """
    def setUp(self):
        Tester.setup_streaming_analytics(self)

