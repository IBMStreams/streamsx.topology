# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
import unittest
import sys

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

SKIP=False
try:
    from sklearn import datasets, svm
except:
    SKIP=True

@unittest.skipIf(SKIP, "Scikit-learn not available")
class TestScikit(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_scikit_learn(self):
        """Verify basic scikit-learn tutorial code works as a stream."""
        digits = datasets.load_digits()
        clf = svm.SVC(gamma=0.001, C=100.)
        clf.fit(digits.data[:-10], digits.target[:-10])

        expected = []
        for i in digits.data[-10:]:
            d = clf.predict(i.reshape(1,-1))
            expected.append(d[0])

        topo = Topology()

        topo.add_pip_package('scikit-learn')
        topo.exclude_packages.add('sklearn')

        images = topo.source(digits.data[-10:], name='Images')
        images_digits = images.map(lambda image : clf.predict(image.reshape(1,-1))[0], name='Predict Digit')

        tester = Tester(topo)
        tester.contents(images_digits, expected)
        tester.tuple_count(images_digits, 10)
        tester.test(self.test_ctxtype, self.test_config)

@unittest.skipIf(SKIP, "Scikit-learn not available")
class TestServiceScikit(TestScikit):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
