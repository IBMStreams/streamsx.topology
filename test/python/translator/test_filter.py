import unittest
import itertools

from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

import translate_test_utils

def check_lt900(t):
    return t[0] < 900 

class TestFilter(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_filter_tuple(self):
        for translate in [False, True]:
            with self.subTest(translate=translate):
                topo = Topology()

                translate_test_utils.setup(topo, translate)
                    
                s1 = topo.source(range(2000))
                translate_test_utils.check_stream(self, s1, False)

                s = s1.map(lambda t : (t%1000,), schema=StreamSchema('tuple<int32 a>').as_tuple())
                translate_test_utils.check_stream(self, s, False)

                s = s.filter(lambda tuple_ : tuple_[0] < 900)
                translate_test_utils.check_stream(self, s, translate, 'Filter')

                tester = Tester(topo)
                tester.tuple_count(s1, 2000)
                tester.tuple_check(s, check_lt900)
                tester.tuple_count(s, 1800)
                tester.test(self.test_ctxtype, self.test_config)
