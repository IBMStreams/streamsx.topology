# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import unittest
#import sys
#import itertools
#import threading

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology import schema
import streamsx.topology.context
from streamsx.topology.context import ConfigParams
import streamsx.spl.op as op
import streamsx.spl.types as spltypes


class TestSplOutAttrAssignmentEqProperties(unittest.TestCase):
    """ Test output attribute assignments to attributes that are (inherited) properties in the Invoke class
    like category, params, and resource_tags.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def test_attributes_category_params_resource_tags(self):
        topo = Topology()
        b = op.Source(topo, "spl.utility::Beacon",
            'tuple<uint64 seq, rstring category, boolean params, int32 resource_tags>',
            params = {'iterations':3})
        # update operator parameters
        b.params['period'] = 0.01
        b.params.update({'initDelay':1.0})
        # the SPL operator's category
        b.category = 'sources_category'
        # output assignments:
        b.seq = b.output('IterationCount()')
        b.category = b.output('"category"')  # an rstring literal
        b.params = b.output('true')
        b.resource_tags = b.output(spltypes.int32(42))
        beacon_stream = b.stream

        functor_params = {'filter':op.Expression.expression('seq % 2ul == 0ul')}
        f = op.Invoke(topo, "spl.relational::Functor",
              inputs=beacon_stream,
              schemas=['tuple<uint64 seq, rstring category, boolean params, int32 resource_tags, rstring extension>', 'tuple<uint64 seq>'],
              params=functor_params,
              name=None)
        f.category = 'analytics_category'
        # output streams fs0 and fs1
        fs0 = f.outputs[0]
        fs1 = f.outputs[1]

        # output assignments for port 0
        f.category = f.output(fs0, f.attribute(beacon_stream, 'category'))
        f.extension = f.output(fs0, '"extension_attr_val"')
        # output assignments for port 1
        f.seq = f.output(fs1, op.Expression.expression("seq * 10ul"))

        # test that property access works
        self.assertEqual(b.category, 'sources_category')
        self.assertDictEqual(b.params, {'iterations': 3, 'period': 0.01, 'initDelay': 1.0})
        self.assertSetEqual(b.resource_tags, set())

        self.assertEqual(f.category, 'analytics_category')
        self.assertDictEqual(f.params, functor_params)
        self.assertSetEqual(f.resource_tags, set())
        
        tester = Tester(topo)
        tester.tuple_count(fs0, 2)
        tester.contents(fs0, [{'seq':0, 'category':'category', 'params':True, 'resource_tags':42, 'extension':'extension_attr_val'},
                              {'seq':2, 'category':'category', 'params':True, 'resource_tags':42, 'extension':'extension_attr_val'}], ordered=True)
        tester.test(self.test_ctxtype, self.test_config)
