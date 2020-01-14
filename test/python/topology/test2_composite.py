# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import context
from streamsx.topology.composite import *
from streamsx.topology.context import ConfigParams
from streamsx import rest
import streamsx.ec as ec

class S1(Source):
    def populate(self, topology, name, **options):
        self.topo = topology
        self.name = name
        self.options = options
        return topology.source([], name)

class S2Bad(Source):
    def populate(self, topology, name, **options):
        return 3

class S3Bad(Source): pass

class M1(Map):
    def populate(self, topology, stream, schema, name, **options):
        self.topo = topology
        self.stream = stream
        self.schema = schema
        self.name = name
        self.options = options
        return stream.map(name=name)

class M2Bad(Map):
    def populate(self, topology, stream, schema, name, **options):
        return 3

class M3Bad(Map): pass

class FE1(ForEach):
    def populate(self, topology, stream, name, **options):
        self.topo = topology
        self.stream = stream
        self.name = name
        self.options = options
        return stream.for_each(lambda _ : None, name=name)

class FE2Bad(ForEach):
    def populate(self, topology, stream, name, **options):
        return topology.source([])

class FE3Bad(ForEach): pass

class TestComposite(unittest.TestCase):
    _multiprocess_can_split_ = True


    def test_source(self):
        topo = Topology()

        src = S1()
        s = topo.source(src, name='S1_SRC')
        self.assertEqual(topo, src.topo)
        self.assertEqual('S1_SRC', src.name)
        self.assertFalse(src.options)
        self.assertIsInstance(s, Stream)

    def test_source_no_name(self):
        topo = Topology()

        src = S1()
        s = topo.source(src)
        self.assertEqual(topo, src.topo)
        self.assertIsNone(src.name)
        self.assertFalse(src.options)
        self.assertIsInstance(s, Stream)

    def test_bad_source_not_stream(self):
        topo = Topology()
        self.assertRaises(TypeError, topo.source, S2Bad())

    def test_bad_source_abstract(self):
        self.assertRaises(TypeError, S3Bad)

    def test_map(self):
        topo = Topology()

        s = topo.source([])
        m = M1()
        sm = s.map(m, name='M1_MAP')
        self.assertEqual(topo, m.topo)
        self.assertEqual(s, m.stream)
        self.assertEqual('M1_MAP', m.name)
        self.assertFalse(m.options)
        self.assertIsInstance(sm, Stream)

    def test_map_no_options(self):
        topo = Topology()

        s = topo.source([])
        m = M1()
        sm = s.map(m)
        self.assertEqual(topo, m.topo)
        self.assertEqual(s, m.stream)
        self.assertIsNone(m.name)
        self.assertIsNone(m.schema)
        self.assertFalse(m.options)
        self.assertIsInstance(sm, Stream)

    def test_bad_map_no_stream(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.map, M2Bad())

    def test_bad_map_abstract(self):
        self.assertRaises(TypeError, M3Bad)

    def test_for_each(self):
        topo = Topology()

        s = topo.source([])
        fe = FE1()
        e = s.for_each(fe, name='FE1_FOR_EACH')
        self.assertEqual(topo, fe.topo)
        self.assertEqual(s, fe.stream)
        self.assertEqual('FE1_FOR_EACH', fe.name)
        self.assertFalse(fe.options)
        self.assertIsInstance(e, Sink)

    def test_for_each_no_options(self):
        topo = Topology()

        s = topo.source([])
        fe = FE1()
        e = s.for_each(fe)
        self.assertEqual(topo, fe.topo)
        self.assertEqual(s, fe.stream)
        self.assertIsNone(fe.name)
        self.assertFalse(fe.options)
        self.assertIsInstance(e, Sink)

    def test_for_each_map_no_sink(self):
        topo = Topology()
        s = topo.source([])
        self.assertRaises(TypeError, s.for_each, FE2Bad())

    def test_bad_for_each_abstract(self):
        self.assertRaises(TypeError, FE3Bad)

class WordCount(Map):
    def __init__(self, period, update):
        super(WordCount, self).__init__()
        self.period = period
        self.update = update

    def populate(self, topology, stream, schema, name, **options):
        words = stream.flat_map(lambda line : line.split())
        win = words.last(size=self.period).trigger(self.update).partition(lambda s : s)
        return win.aggregate(lambda values : (values[0], len(values)))

class TestRealComposite(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_wordcount(self):
        topo = Topology()
        s = topo.source(['Two things are infinite the universe and human stupidty and Im not sure about the universe'])
        s = s.as_string()
        wc = s.map(WordCount(3, 2))

        tester = Tester(topo)
        tester.contents(wc, [('and', 2), ('the', 2), ('universe', 2)])

        tester.test(self.test_ctxtype, self.test_config)
