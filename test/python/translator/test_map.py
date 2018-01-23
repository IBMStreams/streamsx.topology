import unittest
import itertools

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

import translate_test_utils

def check_map_nt(t):
    return t['b'] == (t['a'] * 3) and t['c'] == t['a'] * 17.5

class TestMap(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_map_namedtuple(self):
        for translate in [False, True]:
            with self.subTest(translate=translate):
                topo = Topology()

                translate_test_utils.setup(topo, translate)
                    
                s1 = topo.source(range(2000))
                translate_test_utils.check_stream(self, s1, False)

                s = s1.map(lambda t : (t,), schema=StreamSchema('tuple<int32 a>').as_tuple(named=True))
                translate_test_utils.check_stream(self, s, False)

                s = s.map(lambda val : (val.a, val.a * 3, val.a * 17.5) ,
                     schema='tuple<int32 a, int64 b, float64 c>')
                translate_test_utils.check_stream(self, s, translate, 'Functor')

                tester = Tester(topo)
                tester.tuple_count(s1, 2000)
                tester.tuple_check(s, check_map_nt)
                tester.tuple_count(s, 2000)
                tester.test(self.test_ctxtype, self.test_config)
