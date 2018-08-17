# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys
import itertools
from enum import IntEnum
import datetime
import decimal

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology.context import JobConfig
import streamsx.spl.op as op


class AddIt(object):
    def __init__(self, sp):
        self.sp = sp
    def __enter__(self):
        self.spv = self.sp()
    def __exit__(self, exc_type, exc_value, traceback):
        pass
    def __call__(self, t):
        return str(t) + '-' + self.spv

class TestSubmissionParams(unittest.TestCase):
    """ Test submission params (distributed).
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_spl_default(self):
        """
        Test passing as with default using SPL
        """
        N=27
        G='hey there'
        t = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topic = 'topology/test/python/' + t
       
        topo = Topology()
        spGreet = topo.create_submission_parameter('greeting', default=G)
        self.assertIsNone(spGreet())

        sch = StreamSchema('tuple<uint64 seq, rstring s>')
        b = op.Source(topo, "spl.utility::Beacon", sch,
            params = {'initDelay': 10.0, 'period': 0.02, 'iterations':N})
        b.seq = b.output('IterationCount()')
        b.s = b.output(spGreet)

        tester = Tester(topo)
        tester.tuple_count(b.stream, N)
        tester.contents(b.stream, [{'seq':i, 's':G} for i in range(N)])
        tester.test(self.test_ctxtype, self.test_config)

    def test_topo(self):
        topo = Topology()
        s = topo.source(range(38))
        lower = topo.create_submission_parameter('lower')
        upper = topo.create_submission_parameter('upper')
        addin = topo.create_submission_parameter('addin')

        s = s.filter(lambda v: v < int(lower()) or v > int(upper()))

        m = s.filter(lambda v : v < 3)
        m = m.map(AddIt(addin))

        jc = JobConfig()
        jc.submission_parameters['lower'] = 7
        jc.submission_parameters['upper'] = 33
        jc.submission_parameters['addin'] = 'Yeah!'
        jc.add(self.test_config)

        tester = Tester(topo)
        tester.contents(s, [0,1,2,3,4,5,6,34,35,36,37])
        tester.contents(m, ['0-Yeah!','1-Yeah!','2-Yeah!'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_topo_with_def_and_type(self):
        topo = Topology()
        s = topo.source(range(38))
        lower = topo.create_submission_parameter('lower', default=0)
        upper = topo.create_submission_parameter('upper', default=30)

        s = s.filter(lambda v: v < lower() or v > upper())

        jc = JobConfig()
        jc.submission_parameters['lower'] = 5
        jc.add(self.test_config)

        tester = Tester(topo)
        tester.contents(s, [0,1,2,3,4,31,32,33,34,35,36,37])
        tester.test(self.test_ctxtype, self.test_config)

    def test_topo_types_from_default(self):
        topo = Topology()
        sp_str = topo.create_submission_parameter('sp_str', default='Hi')
        sp_int = topo.create_submission_parameter('sp_int', default=89)
        sp_float = topo.create_submission_parameter('sp_float', default=0.5)
        sp_bool = topo.create_submission_parameter('sp_bool', default=False)
        
        s = topo.source(range(17))
        s = s.filter(lambda v : isinstance(sp_str(), str) and sp_str() == 'Hi')
        s = s.filter(lambda v : isinstance(sp_int(), int) and sp_int() == 89)
        s = s.filter(lambda v : isinstance(sp_float(), float) and sp_float() == 0.5)
        s = s.filter(lambda v : isinstance(sp_bool(), bool) and sp_bool() is False)
     
        tester = Tester(topo)
        tester.tuple_count(s, 17)
        tester.test(self.test_ctxtype, self.test_config)

    def test_topo_types_explicit_set(self):
        topo = Topology()
        sp_str = topo.create_submission_parameter('sp_str', type_=str)
        sp_int = topo.create_submission_parameter('sp_int', type_=int)
        sp_float = topo.create_submission_parameter('sp_float', type_=float)
        sp_bool = topo.create_submission_parameter('sp_bool', type_=bool)
        
        s = topo.source(range(17))
        s = s.filter(lambda v : isinstance(sp_str(), str) and sp_str() == 'SeeYa')
        s = s.filter(lambda v : isinstance(sp_int(), int) and sp_int() == 10)
        s = s.filter(lambda v : isinstance(sp_float(), float) and sp_float() == -0.5)
        s = s.filter(lambda v : isinstance(sp_bool(), bool) and sp_bool() is True)
        jc = JobConfig()
        jc.submission_parameters['sp_str'] = 'SeeYa'
        jc.submission_parameters['sp_int'] = 10
        jc.submission_parameters['sp_float'] = -0.5
        jc.submission_parameters['sp_bool'] = True
        jc.add(self.test_config)
     
        tester = Tester(topo)
        tester.tuple_count(s, 17)
        tester.test(self.test_ctxtype, self.test_config)

    def test_parallel(self):
        topo = Topology()
        sp_w1 = topo.create_submission_parameter('w1', type_=int)
        sp_w2 = topo.create_submission_parameter('w2', type_=int)
        
        s = topo.source(range(67)).set_parallel(sp_w1)
        s = s.filter(lambda v : v % sp_w1() == 0)
        s = s.end_parallel()
        s = s.parallel(width=sp_w2)
        s = s.filter(lambda v : v % sp_w2() == 0)
        s = s.end_parallel()

        jc = JobConfig()
        jc.submission_parameters['w1'] = 3
        jc.submission_parameters['w2'] = 5
        jc.add(self.test_config)
     
        tester = Tester(topo)
        tester.contents(s,[0,15,30,45,60]*3, ordered=False)
        tester.test(self.test_ctxtype, self.test_config)

class TestSubmissionParamsDistributed(TestSubmissionParams):
    """ Test submission params (distributed).
    """
    def setUp(self):
        Tester.setup_distributed(self)

    def test_spl(self):
        """
        Test passing as an SPL parameter.
        """
        N=22
        G='hey'
        t = ''.join(random.choice('0123456789abcdef') for x in range(20))
        topic = 'topology/test/python/' + t
       
        topo = Topology()
        spTopic = topo.create_submission_parameter('mytopic')
        spGreet = topo.create_submission_parameter('greeting')

        self.assertIsNone(spTopic())
        self.assertIsNone(spGreet())

        sch = StreamSchema('tuple<uint64 seq, rstring s>')
        b = op.Source(topo, "spl.utility::Beacon", sch,
            params = {'initDelay': 10.0, 'period': 0.02, 'iterations':N})
        b.seq = b.output('IterationCount()')
        b.s = b.output(spGreet)
     
        p = op.Sink("com.ibm.streamsx.topology.topic::Publish", b.stream,
            params={'topic': topic})

        s = op.Source(topo, "com.ibm.streamsx.topology.topic::Subscribe", sch,
            params = {'streamType': sch, 'topic': spTopic})

        jc = JobConfig()
        jc.submission_parameters['mytopic'] = topic
        jc.submission_parameters['greeting'] = G
        jc.add(self.test_config)

        tester = Tester(topo)
        tester.tuple_count(s.stream, N)
        #tester.run_for(300)
        tester.contents(s.stream, [{'seq':i, 's':G} for i in range(N)])
        tester.test(self.test_ctxtype, self.test_config)

