# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
from streamsx.topology.tester import Tester
from streamsx.topology.topology import Topology
from streamsx.topology.topology import Routing
from streamsx.topology import context
from streamsx.topology.context import submit, ContextTypes, ConfigParams
import random
import itertools
from typing import Iterable, NamedTuple
import streamsx.ec as ec
import datetime
import time

class SourceSchema(NamedTuple):
    key: str
    value: int
    id: str
    ts: datetime.datetime

class ChannelSchema(NamedTuple):
    channel_num: str
    key: str
    value: int
    id: str
    ts: datetime.datetime

class DataGen(object):

    def __init__(self, iterations=None):
        self.iterations = iterations

    def __call__(self) -> Iterable[SourceSchema]:
        for num in itertools.count(1):
            if num >  self.iterations:
                break
            id = random.randint(0, 2)
            if num % 2 == 0:
               key = 'A'
            else:
               key = 'B'
            dt = datetime.datetime.now()
            yield {"key": key, "value": num, "id": "id_" + str(id), "ts": dt}

class AddChannel(object):
    def __init__(self):
        pass

    def __call__(self, tuple) -> Iterable[ChannelSchema]:
        output_event = ChannelSchema(
            channel_num = str(self.channel),
            key = tuple.key,
            value = tuple.value,
            id = tuple.id,
            ts = tuple.ts
        )
        return output_event

    def __enter__(self):
        self.channel = ec.channel(self)

    def __exit__(self, type, value, traceback):
        pass

class TestParallel(unittest.TestCase):

    def setUp(self):
        Tester.setup_standalone(self)

    def test_structured_schema_valid_keys(self):
        topo = Topology("test_structured_schema_valid_keys")
        num_tuples = 1000
        s = topo.source(DataGen(num_tuples))
        s = s.parallel(6, routing=Routing.KEY_PARTITIONED, keys=['key', 'id'])
        s = s.map(AddChannel())
        r = s.end_parallel()
        #r.print()
        tester = Tester(topo)
        tester.tuple_count(r, num_tuples)
        if ("TestDistributed" in str(self)):
            # Ignore configurations errors on CP4D caused by 6 channels
            # Internal Server Error -- {"messages":[{"id":"CDISR3257E","message":"CDISR3257E Cpu specification adjustment: Res[0.799->16.0] is too large for job or instance with: Res[1.199->24.0] compared with limit: Res[20.0->20.0].","status":"error"}]}
            res = tester.test(self.test_ctxtype, self.test_config, assert_on_fail=False)
            print (str(res))
            if False == res:
                 self.skipTest("Skip test, most likely Internal Server Error.")
        else:
            tester.test(self.test_ctxtype, self.test_config)

    def test_structured_schema_single_key(self):
        topo = Topology("test_structured_schema_single_key")
        num_tuples = 1000
        s = topo.source(DataGen(num_tuples))
        s = s.parallel(2, routing=Routing.KEY_PARTITIONED, keys=['key'])
        s = s.map(AddChannel())
        r = s.end_parallel()
        #r.print()
        tester = Tester(topo)
        tester.tuple_count(r, num_tuples)
        tester.test(self.test_ctxtype, self.test_config)

    def test_keys_is_none(self):
        topo = Topology("test_keys_is_none")
        s = topo.source(DataGen(10))
        with self.assertRaises(NotImplementedError):
            s = s.parallel(3, routing=Routing.KEY_PARTITIONED, keys=None)

    def test_keys_not_array(self):
        topo = Topology("test_keys_not_array")
        s = topo.source(DataGen(10))
        with self.assertRaises(TypeError):
            s = s.parallel(3, routing=Routing.KEY_PARTITIONED, keys='key')

    def test_structured_schema_invalid_keys(self):
        topo = Topology("test_structured_schema_invalid_keys")
        s = topo.source(DataGen(10))
        with self.assertRaises(ValueError):
            s = s.parallel(3, routing=Routing.KEY_PARTITIONED, keys=['invalid', 'keys'])

    def test_python_schema(self):
        topo = Topology("test_python_schema")
        s = topo.source(range(10))
        s = s.map(lambda x : {'value': x})
        with self.assertRaises(TypeError):
            s = s.parallel(3, routing=Routing.KEY_PARTITIONED, keys=['any'])

    def test_json_schema(self):
        topo = Topology("test_json_schema")
        s = topo.source(range(10))
        s = s.map(lambda x : {'value': x}).as_json()
        with self.assertRaises(TypeError):
            s = s.parallel(3, routing=Routing.KEY_PARTITIONED, keys=['any'])

    def test_string_schema(self):
        topo = Topology("test_string_schema")
        s = topo.source(range(10))
        s = s.map(lambda x : {'value': x}).as_string()
        with self.assertRaises(TypeError):
            s = s.parallel(3, routing=Routing.KEY_PARTITIONED, keys=['any'])


class TestDistributedParallel(TestParallel):
    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config[ConfigParams.SSL_VERIFY] = False

class TestSasParallel(TestParallel):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)


