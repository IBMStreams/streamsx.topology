import unittest
import os

from streamsx.topology.topology import Topology

class TestForce(unittest.TestCase):
    def setUp(self):
        self._saved = os.environ.get('STREAMSX_TOPOLOGY_TRANSLATE')
    def tearDown(self):
        if self._saved:
            os.environ['STREAMSX_TOPOLOGY_TRANSLATE'] = self._saved

    def test_envvar_setting(self):
        for translate in ['True', 'yes']:
            with self.subTest(translate=translate):
                os.environ['STREAMSX_TOPOLOGY_TRANSLATE'] = translate
                topo = Topology()
                self.assertTrue(topo.features[Topology.TRANSLATE_FEATURE])
