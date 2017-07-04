import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

class TestPythonWindowing(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_BasicCountCountWindow(self):
        topo = Topology()
        s = topo.source([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        s = s.last(10).trigger(1).aggregate(lambda x: sum(x)/len(x))
        
        tester = Tester(topo)
        tester.contents(s, [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5])
        tester.test(self.test_ctxtype, self.test_config)

if __name__ == '__main__':
    unittest.main()
