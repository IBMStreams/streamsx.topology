import unittest

from streamsx.topology.topology import *
from streamsx.topology import context
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.spl import op

def ModTwo(arg):
    return arg % 2;

# Test partitioned windows in python topology operators.
class TestPythonWindowPartition(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    # Partition by attribute not supported for common schema
    def test_common_schema_python(self):
        topo = Topology()
        s = topo.source([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        self.assertIs(s.oport.schema, CommonSchema.Python)
        # We cannot partition by attribute because the attributes are not
        # named
        with self.assertRaises(ValueError):
            s = s.last(10).trigger(2).partition('a').aggregate(lambda x: float(sum(x))/float(len(x)))

    # Partition by attribute not supported for common schema
    def test_common_schema_json(self):
        topo = Topology()
        s = topo.source(['1','3','5','7'])
        s = s.map(lambda x: x, schema = CommonSchema.String)
        self.assertIs(s.oport.schema, CommonSchema.String)
        # We cannot partition by attribute because the attributes are not
        # named
        with self.assertRaises(ValueError):
            s = s.last(3).trigger(1).partition('x').aggregate(lambda tuples: ''.join(tuples))

    # Partition by attribute not supported for common schema
    def test_common_schema_json(self):
        topo = Topology()
        s = topo.source([{'a':1},{'b':2,'c':3}, {'d': 4, 'e': 5}])
        
        s = s.map(lambda x: x, schema = CommonSchema.Json)
        self.assertIs(s.oport.schema, CommonSchema.Json)
        with self.assertRaises(ValueError):
            s = s.last(3).trigger(1).partition('a').aggregate(lambda tuples: [[set(tup.keys()), sum(tup.values())] for tup in tuples])


    # Partition by attribute supported for streams schema
    def test_structured_as_dict(self):
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = "tuple<rstring c, int32 d>")

        s = s.last(3).trigger(2).partition('c').aggregate(lambda items: (items[1]['c'],items[0]['d']))

        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 2), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)


    # Partition by attribute supported for streams schema
    def test_structured_as_tuple(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).partition('c').aggregate(lambda items: (items[1][0], items[0][1]))

        # s.print()
        # streamsx.topology.context.submit('TOOLKIT', topo)
 
        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 2), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)


    # Partition by attribute supported for streams schema
    def test_structured_as_named_tuple(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple(named=True)
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).partition('c').aggregate(lambda items: (items[1].c, items[0].d))

        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 2), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)

    # partition by attribute not part of schema
    def test_partition_by_attribute_not_part_of_schema(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).partition('q').aggregate(lambda items: (items[1][0], items[0][1]))

        tester = Tester(topo)
        self.assertFalse(tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False))

    # partition before trigger
    def test_partition_before_trigger(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).partition('c').trigger(2).aggregate(lambda items: (items[1][0], items[0][1]))

        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 2), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)

    # no trigger
    def test_partition_no_trigger(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).partition('c').aggregate(lambda items: (items[0][0], items[0][1]))

        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 1), ('b', 7), ('a', 1), ('a', 2), ('b', 7), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)

    # batch (tumbling) window with partition
    # one partition has an incomplete batch to be aggregated at the end
    # of the run
    def test_partition_batch_one_incomplete(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17), ('a', 10)])
        s = s.map(lambda x: x, schema = schema)

        s = s.batch(2).partition('c').aggregate(lambda items: (items[0][0], sum(item[1] for item in items)))

        tester = Tester(topo)
        tester.contents(s, [('a',3), ('b',16), ('a', 9), ('b', 25), ('a', 10)])
        tester.test(self.test_ctxtype, self.test_config)

    # batch (tumbling) window with partition
    # all partitions have an incomplete batch to be aggregated at the end
    # of the run
    def test_partition_batch_all_incomplete(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17), ('a', 10), ('b', 11)])
        s = s.map(lambda x: x, schema = schema)

        s = s.batch(2).partition('c').aggregate(lambda items: (items[0][0], sum(item[1] for item in items)))

        tester = Tester(topo)
        tester.contents(s, [('a',3), ('b',16), ('a', 9), ('b', 25), ('a', 10), ('b', 11)])
        tester.test(self.test_ctxtype, self.test_config)

    # batch (tumbling) window with partition
    # no partition has an incomplete batch to be aggregated at the end
    # of the run
    def test_partition_batch_no_incomplete(self):
        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.batch(2).partition('c').aggregate(lambda items: (items[0][0], sum(item[1] for item in items)))

        tester = Tester(topo)
        tester.contents(s, [('a',3), ('b',16), ('a', 9), ('b', 25)])
        tester.test(self.test_ctxtype, self.test_config)

    # a batch (tumbling) window partitioned by a callable.
    def test_partition_batch_lambda(self):
        topo = Topology()
        s = topo.source([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])

        s = s.batch(2).partition(lambda x: x % 2).aggregate(lambda items: (sum(item for item in items)))

        tester = Tester(topo)
        tester.contents(s, [1+3,2+4,5+7,6+8,9+11,10+12,13+15,14])
        tester.test(self.test_ctxtype, self.test_config)

    # a batch (tumbling) window partitioned by a callable.
    def test_partition_batch_func(self):
        topo = Topology()
        s = topo.source([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])

        s = s.batch(2).partition(ModTwo).aggregate(lambda items: (sum(item for item in items)))

        tester = Tester(topo)
        tester.contents(s, [1+3,2+4,5+7,6+8,9+11,10+12,13+15,14])
        tester.test(self.test_ctxtype, self.test_config)

    # Partition using a tuple with a structured schema and a python callable.
    def test_structured_as_tuple_lambda_partition(self):

        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).partition(lambda x: x[0]).aggregate(lambda items: (items[1][0], items[0][1]))
 
        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 2), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)

    # partition before trigger using python callable
    def test_partition_before_trigger_callable(self):

        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).partition(lambda x: x[0]).trigger(2).aggregate(lambda items: (items[1][0], items[0][1]))
 
        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',7), ('a', 2), ('b', 9)] )
        tester.test(self.test_ctxtype, self.test_config)

    # Partition by attribute and by callable (not allowed)
    def test_partition_by_attribute_and_callable(self):

        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        with self.assertRaises(ValueError):
            s = s.last(3).trigger(2).partition(lambda x: x[0]).partition('c').aggregate(lambda items: (items[1][0], items[0][1]))

    # Partition by attribute twice (not allowed)
    def test_partition_by_attribute_twice(self):

        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        with self.assertRaises(ValueError):
            s = s.last(3).trigger(2).partition('d').partition('c').aggregate(lambda items: (items[1][0], items[0][1]))
 
    # Partition by attribute twice (not allowed)
    def test_partition_by_callable_twice(self):

        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1),('b', 7),('a', 2),('b', 9), ('a', 4), ('a', 5), ('b', 8), ('b', 17)])
        s = s.map(lambda x: x, schema = schema)

        with self.assertRaises(ValueError):
            s = s.last(3).trigger(2).partition(lambda x: x[0]).partition(lambda x: x[1]).aggregate(lambda items: (items[1][0], items[0][1]))

    # To make sure we are partitioning by value and not by object identity,
    # partition by something other than integer values, characters, or short
    # strings.
    def test_partition_by_tuple(self):

        schema = StreamSchema("tuple<rstring c, int32 d>").as_tuple()
        topo = Topology()
        s = topo.source([('a',1,2),('b',7,8),('a',2,2),('b',9,19), ('a',1,4), ('a',1,5), ('b',9,7), ('b',7,17)])
        s = s.map(lambda x: x, schema = schema)

        s = s.last(3).trigger(2).partition(lambda x: (x[0], x[1])).aggregate(lambda items: (items[1][0], items[0][1]))
 
        tester = Tester(topo)
        tester.contents(s, [('a',1), ('b',9), ('b',7)] )
        tester.test(self.test_ctxtype, self.test_config)
 
    def test_partition_by_callable_json_schema(self):
        topo = Topology()
        s = topo.source([{'a':1},{'b':2,'c':3}, {'d': 4, 'e': 5}])
        
        # Check the averages of the values of the Json objects
        s = s.map(lambda x: x, schema = CommonSchema.Json)
        s = s.last(3).trigger(1).partition(lambda tup: len(tup.keys())).aggregate(lambda tuples: [[set(tup.keys()), sum(tup.values())] for tup in tuples])
        
        tester = Tester(topo)
        tester.contents(s, [ [[{'a'},1]],
                             [[{'c','b'}, 5]],
                             [[{'c','b'}, 5], [{'d','e'}, 9]]
                           ])

        tester.test(self.test_ctxtype, self.test_config)
