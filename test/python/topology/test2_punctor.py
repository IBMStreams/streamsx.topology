# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020
import unittest
import os

from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology.tester import Tester
from streamsx.topology.context import submit, ContextTypes, ConfigParams, JobConfig
import streamsx.spl.op as op


"""
Test punctor function
"""

expected_contents_punct_before_each_tuple = """Punctuation received: WindowMarker
1
Punctuation received: WindowMarker
2
Punctuation received: WindowMarker
3
Punctuation received: WindowMarker
4
Punctuation received: FinalMarker
"""

expected_contents_punct_before = """1
2
Punctuation received: WindowMarker
3
Punctuation received: WindowMarker
4
Punctuation received: FinalMarker
"""

expected_contents_punct_after = """1
2
3
Punctuation received: WindowMarker
4
Punctuation received: WindowMarker
Punctuation received: FinalMarker
"""



class TestPunctor(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        Tester.setup_standalone(self)

    def _test_punct_file(self, topo, s, expected_content, expected_tuple_count, expected_punct_count=None):
        s = s.map(lambda x : (x,), schema='tuple<int32 z>')
        op_params = {'file' : 'punct_file', 'writePunctuations' : True, 'flushOnPunctuation' : True}
        op.Sink("spl.adapter::FileSink", s, params = op_params)

        # Copy the config, since it's shared across all tests, and not every test needs a data
        # directory.
        cfg = self.test_config.copy()
        jc = JobConfig(data_directory=os.getcwd())
        jc.add(cfg)
         
        tester = Tester(topo)
        tester.tuple_count(s, expected_tuple_count)
        if expected_punct_count is not None:
            tester.punct_count(s, expected_punct_count)
        tester.test(self.test_ctxtype, cfg)

        path = os.path.join(os.getcwd(), 'punct_file')
        
        # Validate the contents of the file.
        with open(path, 'r') as f:
            file_contents = f.read()
            self.assertEqual(expected_content, file_contents)
            
        os.remove(path)

    def test_punct_before_each_tuple(self):
        topo = Topology('test_punct_before_each_tuple')
        s = topo.source([1,2,3,4])
        s = s.punctor(lambda x: True)
        self._test_punct_file(topo, s, expected_contents_punct_before_each_tuple, 4, 4)

    def test_punct_before(self):
        topo = Topology('test_punct_before')
        s = topo.source([1,2,3,4])
        s = s.punctor(func=(lambda t : 2 < t), before=True)
        self._test_punct_file(topo, s, expected_contents_punct_before, 4, 2)

    def test_punct_after(self):
        topo = Topology('test_punct_after')
        s = topo.source([1,2,3,4])
        s = s.punctor(func=(lambda t : 2 < t), before=False)
        self._test_punct_file(topo, s, expected_contents_punct_after, 4, 2)





