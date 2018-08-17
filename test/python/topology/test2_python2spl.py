# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest
import sys
import itertools


from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema
from streamsx.topology.tester import Tester

"""
Test that we can covert Python streams to SPL tuples.
"""

class TestPython2SPL(unittest.TestCase):
    """ Test invocations handling of SPL schemas in Python ops.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_object_to_schema(self):
        topo = Topology()
        s = topo.source([1,2,3])
        st = s.map(lambda x : (x,), schema='tuple<int32 x>')

        tester = Tester(topo)
        tester.contents(st, [{'x':1}, {'x':2}, {'x':3}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_to_schema_dict(self):
        topo = Topology()
        s = topo.source([1,2,3])
        st = s.map(lambda x : {'x':x}, schema='tuple<int32 x>')

        tester = Tester(topo)
        tester.contents(st, [{'x':1}, {'x':2}, {'x':3}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_to_schema_opt(self):
        Tester.require_streams_version(self, '4.3')
        topo = Topology()
        s = topo.source([1,2,3])
        st = s.map(lambda x :
            (None,) if x == 2
            else (None,33,) if x == 3
            else (x,),
                schema='tuple<optional<int32> x, optional<int32> y>')
        tester = Tester(topo)
        tester.contents(st, [{'x':1, 'y':None}, {'x':None, 'y':None}, {'x':None, 'y':33}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_to_schema_dict_opt(self):
        Tester.require_streams_version(self, '4.3')
        topo = Topology()
        s = topo.source([1,2,3])
        st = s.map(lambda x :
            {'x':None} if x == 2
            else {'x':None, 'y':33} if x == 3
            else {'x':x},
                schema='tuple<optional<int32> x, optional<int32> y>')
        tester = Tester(topo)
        tester.contents(st, [{'x':1, 'y':None}, {'x':None, 'y':None}, {'x':None, 'y':33}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_to_string(self):
        topo = Topology()
        s = topo.source([93,'hello',True])
        st = s.map(lambda x : x, schema=CommonSchema.String)

        tester = Tester(topo)
        tester.contents(st, ['93','hello','True'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_object_to_json(self):
        topo = Topology()
        s = topo.source([{'a': 7}, {'b': 8}, {'c': 9}])
        st = s.map(lambda x: x, schema=CommonSchema.Json)

        tester = Tester(topo)
        tester.contents(st, [{'a': 7}, {'b': 8}, {'c': 9}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_to_schema(self):
        topo = Topology()
        s = topo.source(['a', 'b', 'c']).as_string()
        st = s.map(lambda x : (x+'struct!',), schema='tuple<rstring y>')

        tester = Tester(topo)
        tester.contents(st, [{'y':'astruct!'}, {'y':'bstruct!'}, {'y':'cstruct!'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_to_schema_dict(self):
        topo = Topology()
        s = topo.source(['a', 'b', 'c']).as_string()
        st = s.map(lambda x : {'z': x+'dict!'}, schema='tuple<rstring z>')

        tester = Tester(topo)
        tester.contents(st, [{'z':'adict!'}, {'z':'bdict!'}, {'z':'cdict!'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_to_string(self):
        topo = Topology()
        s = topo.source([False, 'b', 19]).as_string()
        st = s.map(lambda x: x + '3', schema=CommonSchema.String)

        tester = Tester(topo)
        tester.contents(st, ['False3', 'b3', '193'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_to_json(self):
        topo = Topology()
        s = topo.source(['a', 79, 'c']).as_string()
        st = s.map(lambda x: x if x == 'c' else {'v': x + 'd'}, schema=CommonSchema.Json)

        tester = Tester(topo)
        tester.contents(st, [{'v': 'ad'}, {'v': '79d'}, 'c'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_json_to_schema(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.map(lambda x : (next(iter(x)), x[next(iter(x))]), schema='tuple<rstring y, int32 x>')

        tester = Tester(topo)
        tester.contents(st, [{'y':'a', 'x':7}, {'y':'b', 'x':8}, {'y':'c', 'x':9}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_json_to_schema_dict(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.map(lambda x : {'y':next(iter(x)), 'x':x[next(iter(x))]}, schema='tuple<rstring y, int32 x>')

        tester = Tester(topo)
        tester.contents(st, [{'y':'a', 'x':7}, {'y':'b', 'x':8}, {'y':'c', 'x':9}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_json_to_string(self):
        topo = Topology()
        s = topo.source([{'a': True}, {'a': 8}, {'a': 'third'}]).as_json()
        st = s.map(lambda x : x['a'], schema=CommonSchema.String)

        tester = Tester(topo)
        tester.contents(st, ['True', '8', 'third'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_json_to_json(self):
        topo = Topology()
        s = topo.source([{'a': True}, {'a': 8}, {'a': 'third'}]).as_json()
        st = s.map(lambda x : {'yy': x['a']}, schema=CommonSchema.Json)

        tester = Tester(topo)
        tester.contents(st, [{'yy': True}, {'yy': 8}, {'yy': 'third'}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_dict_to_schema(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.map(lambda x : (next(iter(x)), x[next(iter(x))]), schema='tuple<rstring y, int32 x>')
        st = st.map(lambda x : (x['y'], x['x']+3), schema='tuple<rstring id, int32 value>')

        tester = Tester(topo)
        tester.contents(st, [{'id':'a', 'value':10}, {'id':'b', 'value':11}, {'id':'c', 'value':12}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_dict_to_schema_dict(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.map(lambda x : {'y':next(iter(x)), 'x':x[next(iter(x))]}, schema='tuple<rstring y, int32 x>')
        st = st.map(lambda x : {'id':x['y'], 'value':x['x']+3}, schema='tuple<rstring id, int32 value>')

        tester = Tester(topo)
        tester.contents(st, [{'id':'a', 'value':10}, {'id':'b', 'value':11}, {'id':'c', 'value':12}])
        tester.test(self.test_ctxtype, self.test_config)

    def test_dict_to_string(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.map(lambda x : (next(iter(x)), x[next(iter(x))]), schema='tuple<rstring y, int32 x>')

        st = st.map(lambda x : (x['y'], x['x']+20), schema=CommonSchema.String)

        if sys.version_info.major == 2:
            expected = ["(u'a', 27L)", "(u'b', 28L)", "(u'c', 29L)"]
        else:
            expected = ["('a', 27)", "('b', 28)", "('c', 29)"]

        tester = Tester(topo)
        tester.contents(st, expected)
        tester.test(self.test_ctxtype, self.test_config)

    def test_dict_to_json(self):
        topo = Topology()
        s = topo.source([{'a':7}, {'b':8}, {'c':9}]).as_json()
        st = s.map(lambda x: (next(iter(x)), x[next(iter(x))]), schema='tuple<rstring y, int32 x>')
        st = st.map(lambda x: x, schema=CommonSchema.Json)

        tester = Tester(topo)
        tester.contents(st, [{'y': 'a', 'x': 7}, {'y':'b', 'x': 8}, {'y':'c', 'x': 9}])
        tester.test(self.test_ctxtype, self.test_config)
