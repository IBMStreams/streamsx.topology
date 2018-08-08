# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from past.builtins import unicode
import unittest
import random
import collections
import sys
import threading


from streamsx.topology.topology import Topology, Routing
from streamsx.topology.schema import _SchemaParser
from streamsx.topology.tester import Tester
import streamsx.topology.schema as _sch
import streamsx.topology.runtime as _str

_PRIMITIVES = ['boolean', 'blob', 'int8', 'int16', 'int32', 'int64',
                 'uint8', 'uint16', 'uint32', 'uint64',
                 'float32', 'float64',
                 'decimal32', 'decimal64', 'decimal128',
                 'complex32', 'complex64',
                 'timestamp', 'xml'
               ]

_COLLECTIONS = ['list', 'set']

def _random_type(depth):
    r = random.random()
    if r < 0.10 and depth < 3:
        return _random_schema(depth=depth+1)
    elif r < 0.2:
         c = 'map<'
         c += _random_type(depth)
         c += ','
         c += _random_type(depth)
         c += '>'
         return c
    elif r < 0.35:
         c = random.choice(_COLLECTIONS)
         c += '<'
         c += _random_type(depth)
         c += '>'
         return c
    elif r < 0.45:
         c = 'optional<'
         c += _random_type(depth)
         c += '>'
         return c
    else:
        return random.choice(_PRIMITIVES)

def _random_schema(depth=0):
    depth += 1
    s = 'tuple<'
    for an in range(random.randint(1, 30)):
        if an != 0:
            s += ','
        s += _random_type(depth)
        s += " A_" + str(an)
    s += '>'
    return s

class TestSchema(unittest.TestCase):

    # Fake out subTest
    if sys.version_info.major == 2:
        def subTest(self, **args): return threading.Lock()

    def test_simple(self):
      p = _SchemaParser('tuple<int32 a, int64 b>')
      p._parse()
      self.assertEqual(2, len(p._type))
      self.assertEqual('int32', p._type[0][0])
      self.assertEqual('a', p._type[0][1])

      self.assertEqual('int64', p._type[1][0])
      self.assertEqual('b', p._type[1][1])

    def test_primitives(self):
      for typ in _PRIMITIVES:
          with self.subTest(typ = typ):
              p = _SchemaParser('tuple<' + typ + ' p>')
              p._parse()
              self.assertEqual(1, len(p._type))
              self.assertEqual(typ, p._type[0][0])
              self.assertEqual('p', p._type[0][1])

    def test_collections(self):
      for ctyp in _COLLECTIONS:
          for etyp in _PRIMITIVES:
              typ = ctyp + '<' + etyp + '>'
              with self.subTest(typ = typ):
                  p = _SchemaParser('tuple<' + typ + ' c>')
                  p._parse()
                  self.assertEqual(1, len(p._type))
                  self.assertIsInstance(p._type[0][0], tuple)
                  self.assertEqual(p._type[0][0][0], ctyp)
                  self.assertEqual(p._type[0][0][1], etyp)
                  self.assertEqual('c', p._type[0][1])

    def test_map(self):
        typ = 'map<int32, complex64>'
        p = _SchemaParser('tuple<' + typ + ' m>')
        p._parse()
        self.assertEqual(1, len(p._type))
        self.assertIsInstance(p._type[0][0], tuple)
        self.assertEqual(p._type[0][0][0], 'map')
        self.assertIsInstance(p._type[0][0][1], tuple)
        self.assertEqual(p._type[0][0][1][0], 'int32')
        self.assertEqual(p._type[0][0][1][1], 'complex64')
        self.assertEqual('m', p._type[0][1])

    def test_optional(self):
        for typ in _PRIMITIVES:
            otyp = 'optional<' + typ + '>'
            with self.subTest(otyp = otyp):
                p = _SchemaParser('tuple<' + otyp + ' o>')
                p._parse()
                self.assertEqual(1, len(p._type))
                self.assertIsInstance(p._type[0][0], tuple)
                self.assertEqual(p._type[0][0][0], 'optional')
                self.assertEqual(p._type[0][0][1], typ)
                self.assertEqual('o', p._type[0][1])

    def test_nested_tuple(self):
      p = _SchemaParser('tuple<int32 a, tuple<int64 b, complex32 c, float32 d> e>')
      p._parse()
      self.assertEqual(2, len(p._type))

      self.assertEqual('int32', p._type[0][0])
      self.assertEqual('a', p._type[0][1])

      self.assertIsInstance(p._type[1][0], tuple)
      self.assertEqual('e', p._type[1][1])
      nt = p._type[1][0]
      self.assertEqual('tuple', nt[0])
      self.assertIsInstance(nt[1], list)
      self.assertEqual(3, len(nt[1]))
      nttyp = nt[1]

      self.assertEqual('int64', nttyp[0][0])
      self.assertEqual('b', nttyp[0][1])
      self.assertEqual('complex32', nttyp[1][0])
      self.assertEqual('c', nttyp[1][1])
      self.assertEqual('float32', nttyp[2][0])
      self.assertEqual('d', nttyp[2][1])

    def test_random_schemas(self):
        """Just verify random schemas can be parsed"""
        for r in range(200):
            schema = _random_schema()
            p = _SchemaParser(schema)
            p._parse()

    def test_bounded_schema(self):
        s = _sch.StreamSchema('tuple<rstring[1] a, boolean alert>')
        s = _sch.StreamSchema('tuple<map<int32,rstring>[8] a>')
        s = _sch.StreamSchema('tuple<list<int32>[100] a>')
        s = _sch.StreamSchema('tuple<set<list<int32>[9]>[100] a>')


    @unittest.skip("not yet supported")
    def test_named_schema(self):
        s = _sch.StreamSchema('tuple<int32 a, boolean alert>')

        nt1 = s._namedtuple()
        nt2 = s._namedtuple()
        self.assertIs(nt1, nt2)

        t = nt1(345, False)
        self.assertEqual(345, t.a)
        self.assertFalse(t.alert)
        self.assertEqual(345, t[0])
        self.assertFalse(t[1])

    def test_common_styles(self):
        """ Test that common schemas cannot have their style changed"""
        s = _sch.CommonSchema.Python
        st = s.value.as_tuple()
        self.assertIs(s.value, st)

        s = _sch.CommonSchema.String
        st = s.value.as_tuple()
        self.assertIs(s.value, st)

        s = _sch.CommonSchema.Json
        st = s.value.as_tuple()
        self.assertIs(s.value, st)

        s = _sch.CommonSchema.Binary
        st = s.value.as_tuple()
        self.assertIs(s.value, st)

        s = _sch.CommonSchema.XML
        st = s.value.as_tuple()
        self.assertIs(s.value, st)

    def test_styles(self):
        s = _sch.StreamSchema('tuple<int32 a, boolean alert>')
        self.assertEqual(dict, s.style)
        st = s.as_tuple()
        self.assertIsNot(s, st)
        self.assertEqual(tuple, st.style)

        sd = s.as_dict()
        self.assertIs(s, sd)
        self.assertEqual(dict, sd.style)

        sd2 = st.as_dict()
        self.assertIsNot(st, sd2)
        self.assertEqual(dict, sd2.style)

        self.assertEqual(object, _sch.CommonSchema.Python.value.style)
        self.assertEqual(unicode if sys.version_info.major == 2 else str, _sch.CommonSchema.String.value.style)
        self.assertEqual(dict, _sch.CommonSchema.Json.value.style)

        snt = s.as_tuple(named='Alert')
        self.assertIsNot(s, snt)
        self.assertTrue(issubclass(snt.style, tuple))
        self.assertTrue(hasattr(snt.style, '_fields'))
        self.assertTrue(hasattr(snt.style, '_splpy_namedtuple'))
        self.assertTrue('Alert', snt.style._splpy_namedtuple)

        tv = snt.style(23, True)
        self.assertEqual(23, tv[0])
        self.assertEqual(23, tv.a)
        self.assertTrue(tv[1])
        self.assertTrue(tv.alert)

        self.assertTrue(str(tv).startswith('Alert('))
        
        snt2 = s.as_tuple(named=True)
        self.assertIsNot(s, snt2)
        self.assertIsNot(snt, snt2)
        self.assertTrue(issubclass(snt2.style, tuple))
        self.assertTrue(hasattr(snt2.style, '_fields'))
        self.assertTrue(hasattr(snt2.style, '_splpy_namedtuple'))
        self.assertTrue('StreamTuple', snt2.style._splpy_namedtuple)

        tv = snt2.style(83, False)
        self.assertEqual(83, tv[0])
        self.assertEqual(83, tv.a)
        self.assertFalse(tv[1])
        self.assertFalse(tv.alert)
        self.assertTrue(str(tv).startswith('StreamTuple('))

    def test_get_namedtuple_make(self):
        sch = 'tuple<int32 b, rstring c>'
        cls = _str._get_namedtuple_cls(sch, 'MyTuple')
        tv = cls(932, 'hello')
        self.assertEqual(932, tv.b)
        self.assertEqual('hello', tv.c)
        self.assertTrue(str(tv).startswith('MyTuple('))


        

class TestKeepSchema(unittest.TestCase):
    """
    Testing that schemas are maintained through various transforms.
    We test items have the same schema, we don't actually run any apps.
    """

    def test_keep_schema_python(self):
        topo = Topology()
        s = topo.source([])
        self._check_kept(s)

    def test_keep_schema_string(self):
        topo = Topology()
        s = topo.source([]).as_string()
        self._check_kept(s)

    def test_keep_schema_json(self):
        topo = Topology()
        s = topo.source([]).as_json()
        self._check_kept(s)

    def test_keep_schema_schema(self):
        topo = Topology()
        s = topo.source([]).map(lambda x : x, schema='tuple<rstring a, int32 b>')
        self._check_kept(s)

    def _check_kept(self, s):
       # Stream.oport.schema is an internal api
       s1 = s.low_latency()
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.filter(lambda t : True)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s.end_low_latency()
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.filter(lambda t : True)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.isolate()
       self.assertEqual(s.oport.schema, s1.oport.schema)

       s1 = s1.parallel(width=2)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.filter(lambda t : True)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.end_parallel()
       self.assertEqual(s.oport.schema, s1.oport.schema)

       s1 = s1.parallel(width=2, routing=Routing.ROUND_ROBIN)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.filter(lambda t : True)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.end_parallel()
       self.assertEqual(s.oport.schema, s1.oport.schema)

       s1 = s1.parallel(width=2, routing=Routing.HASH_PARTITIONED, func=hash)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.filter(lambda t : True)
       self.assertEqual(s.oport.schema, s1.oport.schema)
       s1 = s1.end_parallel()
       self.assertEqual(s.oport.schema, s1.oport.schema)

       s1 = s1.filter(lambda t : True)
       self.assertEqual(s.oport.schema, s1.oport.schema)

       s2 = s.union({s1})
       self.assertEqual(s.oport.schema, s2.oport.schema)

