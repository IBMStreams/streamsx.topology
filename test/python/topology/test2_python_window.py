import unittest

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema
from streamsx.topology.tester import Tester

class Person(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def rough_birth_year(self):
        return time.localtime().tm_year - self.age;

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestPythonWindowing(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_BasicCountCountWindow(self):
        topo = Topology()
        s = topo.source([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        s = s.last(10).trigger(2).aggregate(lambda x: sum(x)/len(x))

        tester = Tester(topo)
        tester.contents(s, [1.5,2.5,3.5,4.5,5.5,7.5,9.5])
        tester.test(self.test_ctxtype, self.test_config)

    def test_JsonInputCountCountWindow(self):
        topo = Topology()
        s = topo.source([{'a':1},{'b':2,'c':3}, {'d': 4, 'e': 5}])
        
        # Check the averages of the values of the Json objects
        s = s.map(lambda x: x, schema = CommonSchema.Json)
        s = s.last(3).trigger(1).aggregate(lambda tuples: [[set(tup.keys()), sum(tup.values())] for tup in tuples],
                                           schema = CommonSchema.Python)
        
        tester = Tester(topo)
        tester.contents(s, [ [[{'a'},1]],
                             [[{'a'},1], [{'c','b'}, 5]],
                             [[{'a'},1], [{'c','b'}, 5], [{'d','e'}, 9]]
                           ])
        tester.test(self.test_ctxtype, self.test_config)

    def test_StringInputCountCountWindow(self):
        topo = Topology()
        s = topo.source(['1','3','5','7'])
        s = s.map(lambda x: x, schema = CommonSchema.String)
        s = s.last(3).trigger(1).aggregate(lambda tuples: ''.join(tuples),
                                           schema = CommonSchema.Python)

        tester = Tester(topo)
        tester.contents(s, ['1','13','135','357'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_NotByRefWindow(self):
        topo = Topology()
        s = topo.source(['1','3','5','7'])

        # Used to prevent pass by ref for the source
        f = s.filter(lambda x: True)

        s = s.last(3).trigger(4).aggregate(lambda x: int(sum([int(s) for s in x])/len(x)),
                                           schema = CommonSchema.Python)        
        tester = Tester(topo)
        tester.contents(s, [5])
        tester.test(self.test_ctxtype, self.test_config)


    def test_ClassCountCountWindow(self):
        topo = Topology()
        s = topo.source([
                ['Wallace', 55],
                ['Copernicus', 544],
                ['Feynman', 99],
                ['Dirac', 115],
                ['Pauli', 117],
                ['Frenkel', 49],
                ['Terence Tao', 42]
        ])
        s = s.map(lambda x: Person(x[0], x[1]))
        s = s.last(3).trigger(1).aggregate(lambda x: int(sum([p.rough_birth_year() for p in x])/len(x)))

        tester = Tester(topo)
        tester.contents(s, [1962, 1717, 1784, 1764, 1906, 1923, 1947])
        tester.test(self.test_ctxtype, self.test_config)


    # Windowing doesn't currently support the 'dict' type.
    @unittest.expectedFailure
    def test_DictInputWindow(self):
        topo = Topology()
        s = topo.source([1,2,3,4])
        s = s.map(lambda x: ('a', x), schema = "tuple<rstring a, int32 b>")

        # Canned aggregate
        s = s.last(3).trigger(4).aggregate(lambda x: 0),
                                           
        tester = Tester(topo)
        tester.test(self.test_ctxtype, self.test_config)


if __name__ == '__main__':
    unittest.main()
