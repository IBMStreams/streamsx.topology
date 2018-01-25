from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import os
import shutil
import tempfile
import unittest

import test_vers

class EE(object):
    def __init__(self, td):
        self.td = td
    def __enter__(self):
        open(os.path.join(self.td, type(self).__name__) + "_ENTER", 'w').close()
    def __exit__(self, exc_type, exc_value, traceback):
        open(os.path.join(self.td, type(self).__name__) + "_EXIT", 'w').close()

class EESource(EE):
    def __iter__(self):
        for a in range(101):
             yield a

class EEMap(EE):
    def __call__(self, t):
        return t

class EEFilter(EE):
    def __call__(self, t):
        return True

class EEForEach(EE):
    def __call__(self, t):
        return None

class EEFlatMap(EE):
    def __call__(self, t):
        return [t]

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestEnterExit(unittest.TestCase):

    def setUp(self):
        self.td = None
        Tester.setup_standalone(self)

    def tearDown(self):
        if self.td:
            shutil.rmtree(self.td)

    def test_calls(self):
        self.td = tempfile.mkdtemp()
        topo = Topology()
        s = topo.source(EESource(self.td))
        s = s.map(EEMap(self.td))
        s = s.flat_map(EEFlatMap(self.td))
        s = s.filter(EEFilter(self.td))
        s.for_each(EEForEach(self.td))
        tester = Tester(topo)
        tester.tuple_count(s, 101)
        tester.test(self.test_ctxtype, self.test_config)

        for op in ['EESource', 'EEMap', 'EEFlatMap', 'EEFilter', 'EEForEach']:
            self.assertTrue(os.path.isfile(os.path.join(self.td, op + "_ENTER")))
            self.assertTrue(os.path.isfile(os.path.join(self.td, op + "_EXIT")))

