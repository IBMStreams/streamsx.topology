import unittest

import test_vers

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema
from streamsx.topology.tester import Tester
import time
import datetime

class Person(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def rough_birth_year(self):
        return time.localtime().tm_year - self.age;

class TimeCounter(object):
    """Count up from zero every `period` seconds for a given number of 
    iterations."""
    def __init__(self, period=None, iterations=None):
        if period is None:
            period = 1.0

        self.period = period
        self.iterations = iterations
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        # If the number of iterations has been met, stop iterating.
        if self.iterations is not None and self.count >= self.iterations:
            raise StopIteration

        # Otherwise increment, sleep, and return.
        to_return = self.count
        self.count += 1
        time.sleep(self.period)
        return to_return

class TriggerDiff(object):
    """Given any input, returns the timespan (in seconds) between now
    and the last time the TriggerDiff callable was invoked."""
    def __init__(self):
        self.last = None
    
    def __call__(self, param):
        # Record the last time
        _last = self.last

        # Set the current time
        self.last = time.time()

        # Handle the fist invocation
        if _last is None:
            return None
        
        # Return the time diff
        return self.last - _last

class TupleTimespanCheck(object):
    """Checks whether each item in a list passed to the callable has a timestamp marked after
    a certain point in time"""
    def __init__(self, span):
        self.span = span

    def __call__(self, items):
        mark = time.time() - self.span
        return all([mark < item[1] for item in items])
            
# Given a value, a tolerance, and the expected value, return true iff the value is
# within the margin of error.
within_tolerance = lambda val, tol, exp: val < exp + (tol*exp) and val > exp - (tol*exp)

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

    def test_BasicCountTimeWindow(self):
        # Aggregate every 0.5 seconds
        aggregate_period = 0.5
        # Check that each aggregation is triggered at the right time, with a maximum %20 error
        tolerance = 0.20

        topo = Topology()
        s = topo.source(TimeCounter(iterations = 10))
        s = s.last(1).trigger(datetime.timedelta(seconds=aggregate_period)).aggregate(TriggerDiff())
        tester = Tester(topo)
        tester.tuple_check(s, lambda val: within_tolerance(val, tolerance, aggregate_period))
        tester.test(self.test_ctxtype, self.test_config)

    def test_BasicTimeCountWindow(self):
        # Ensure tuples are evicted no later than (window_span*tolerance + window_span)
        tolerance = 0.20
        window_span = 2.0
        max_evict = window_span*tolerance + window_span

        topo = Topology()
        s = topo.source(TimeCounter(iterations = 100, period = 0.1))
        s = s.map(lambda x: (x, time.time()))
        s = s.last(datetime.timedelta(seconds=window_span)).trigger(20).aggregate(TupleTimespanCheck(max_evict))
        
        tester = Tester(topo)
        tester.tuple_check(s, lambda val: val)
        tester.test(self.test_ctxtype, self.test_config)

    def test_JsonInputCountCountWindow(self):
        topo = Topology()
        s = topo.source([{'a':1},{'b':2,'c':3}, {'d': 4, 'e': 5}])
        
        # Check the averages of the values of the Json objects
        s = s.map(lambda x: x, schema = CommonSchema.Json)
        s = s.last(3).trigger(1).aggregate(lambda tuples: [[set(tup.keys()), sum(tup.values())] for tup in tuples])
        
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
        s = s.last(3).trigger(1).aggregate(lambda tuples: ''.join(tuples))

        tester = Tester(topo)
        tester.contents(s, ['1','13','135','357'])
        tester.test(self.test_ctxtype, self.test_config)

    def test_NotByRefWindow(self):
        topo = Topology()
        s = topo.source(['1','3','5','7'])

        # Used to prevent pass by ref for the source
        f = s.filter(lambda x: True)

        s = s.last(3).trigger(4).aggregate(lambda x: int(sum([int(s) for s in x])/len(x)))        
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
