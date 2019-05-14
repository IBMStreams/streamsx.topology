import unittest

from streamsx.topology.topology import *
from streamsx.topology import context
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.spl import op
import time
import os
import datetime

def _delay(x):
    time.sleep(0.2)
    return x

class _BatchTimeCheck(object):
    def __init__(self):
        self.expect = 0 
        self.last = None;
    def __call__(self, items):
        for i in items:
            if i != self.expect:
                return False
            self.expect = i + 1
        if self.last is not None and self.expect < 49:
            delay = time.time() - self.last
            if delay < 1.5 or delay > 3:
                return False
            n = delay / 0.2
            l = len(items)
            if l < n-1 or l > n+1:
                return False
        self.last = time.time()
        return True
        

class Person(object):
    def __init__(self, name, birth_year):
        self.name = name
        self._birth_year = birth_year

    def birth_year(self):
        return self._birth_year

expected_contents = """8
Punctuation received: WindowMarker
9
Punctuation received: WindowMarker
10
Punctuation received: WindowMarker
11
Punctuation received: WindowMarker
Punctuation received: FinalMarker
"""

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
    def next(self):
        return self.__next__()

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

class TestPythonWindowing(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_BasicCountCountWindow(self):
        topo = Topology()
        s = topo.source([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        # Need a float cast to make the value consistent with Python 2/3
        s = s.last(10).trigger(2).aggregate(lambda x: float(sum(x))/float(len(x)))

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
        current_year = time.localtime().tm_year
        s = topo.source([
                ['Wallace', 1962],
                ['Copernicus', 1473],
                ['Feynman', 1918],
                ['Dirac', 1902],
                ['Pauli', 1900],
                ['Frenkel', 1968],
                ['Terence Tao', 1975]
        ])
        s = s.map(lambda x: Person(x[0], x[1]))
        s = s.last(3).trigger(1).aggregate(lambda x: int(sum([p.birth_year() for p in x])/len(x)))

        tester = Tester(topo)
        tester.contents(s, [1962, 1717, 1784, 1764, 1906, 1923, 1947])
        tester.test(self.test_ctxtype, self.test_config)


    def test_structured_as_dict(self):
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('c', 2),('d', 9)])
        s = s.map(lambda x: x, schema = "tuple<rstring a, int32 b>")

        s = s.last(3).trigger(2).aggregate(lambda items: (items[0]['a'],items[1]['b']))

        tester = Tester(topo)
        tester.contents(s, [('a',7), ('b',2)] )
        tester.test(self.test_ctxtype, self.test_config)

    def test_structured_as_tuple(self):
        schema = StreamSchema("tuple<rstring a, int32 b>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('c', 2),('d', 9)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).aggregate(lambda items: (items[1][0], items[0][1]))

        tester = Tester(topo)
        tester.contents(s, [('b',1), ('c',7)] )
        tester.test(self.test_ctxtype, self.test_config)

    def test_structured_as_named_tuple(self):
        schema = StreamSchema("tuple<rstring a, int32 b>").as_tuple(named=True)
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('c', 2),('d', 9)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).aggregate(lambda items: (items[1].a, items[0].b))

        tester = Tester(topo)
        tester.contents(s, [('b',1), ('c',7)] )
        tester.test(self.test_ctxtype, self.test_config)

    def test_WindowPunctuation(self):
        """Trigger an aggregation 4 times. Ensure that window punctuations are submitted each time
        by writing them to an output file, and then verifying that the file contains the correct
        contents."""
        topo = Topology()
        s = topo.source([1,2,3,4])

        # Aggregate and write to file.
        s = s.last(1).trigger(1).aggregate(lambda x: x[0]+7)
        # Ensure map/flat_map/filter passes window marks through.
        s = s.flat_map(lambda x : [x])
        s = s.filter(lambda x : True)
        s = s.map(lambda x : (x,), schema='tuple<int32 z>')
        op_params = {'file' : 'punct_file', 'writePunctuations' : True, 'flushOnPunctuation' : True}
        op.Sink("spl.adapter::FileSink", s, params = op_params)

        # Copy the config, since it's shared across all tests, and not every test needs a data
        # directory.
        cfg = self.test_config.copy()
        jc = context.JobConfig(data_directory=os.getcwd())
        jc.add(cfg)
         
        tester = Tester(topo)
        tester.test(self.test_ctxtype, cfg)

        path = os.path.join(os.getcwd(), 'punct_file')
        
        # Validate the contents of the file.
        with open(path, 'r') as f:
            file_contents = f.read()
            self.assertEqual(expected_contents, file_contents)
            
        os.remove(path)

    def test_batch_count(self):
        topo = Topology()
        s = topo.source(lambda : range(20))
        b = s.batch(4)
        r = b.aggregate(lambda items : sum(items))

        tester = Tester(topo)
        tester.contents(r, [0+1+2+3,4+5+6+7,8+9+10+11,12+13+14+15,16+17+18+19])
        tester.tuple_count(r, 5)
        tester.test(self.test_ctxtype, self.test_config)

    def test_batch_time(self):
        topo = Topology()
        s = topo.source(lambda : map(_delay, range(50)), name='A')
        b = s.batch(datetime.timedelta(seconds=2))
        r = b.aggregate(lambda x : x)
        rf = r.flat_map()

        tester = Tester(topo)
        tester.tuple_count(rf, 50)
        tester.run_for((50*0.2) + 20)
        tester.tuple_check(r, _BatchTimeCheck())
        tester.test(self.test_ctxtype, self.test_config)
