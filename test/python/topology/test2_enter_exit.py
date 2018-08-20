from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

import os
import shutil
import tempfile
import unittest
import time

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

class EESMSource(object):
    def __iter__(self):
        return self
    def __next__(self):
        raise StopIteration()


class EESMSourceM(object):
    def __init__(self):
        self.count = -1
    def __iter__(self):
        return self
    def __next__(self):
        if self.count >= 20:
            raise StopIteration()
        self.count += 1
        if self.count % 3 == 0:
            raise ValueError()
        return self.count
    def __enter__(self):
        pass
    def __exit__(self, exc_type, exc_value, traceback):
        return exc_type == ValueError

class EESMClass(object):
    def __call__(self):
        return None

class EESMClassM(object):
    def __init__(self, f):
        self.f = f
    def __call__(self, t):
        if t % self.f == 0:
            raise ValueError()
        return t
    def __enter__(self):
        pass
    def __exit__(self, exc_type, exc_value, traceback):
        return exc_type == ValueError

def EEMSFunc(x):
    return None

class TestSuppressMetricDistributed(unittest.TestCase):
    _multiprocess_can_split_ = True
    def setUp(self):
        Tester.setup_distributed(self)

    def test_suppress_metric(self):
        topo = Topology()

        # No metric
        s1 = topo.source(EESMSource(), name='NOMETRIC_SA')
        s2 = topo.source([], name='NOMETRIC_SB')
        s3 = topo.source(range(0), name='NOMETRIC_SB')
        s = s1.union({s2,s3})

        s = s.filter(EESMClass(), name='NOMETIC_FA')
        s = s.filter(lambda x : False, name='NOMETIC_FA')
        s = s.filter(ascii, name='NOMETIC_FA')

        s = s.map(EESMClass(), name='NOMETIC_MA')
        s = s.map(lambda x : False, name='NOMETIC_MB')
        s = s.map(ascii, name='NOMETIC_MC')

        s.for_each(EESMClass(), name='NOMETIC_EA')
        s.for_each(lambda x : False, name='NOMETIC_EB')
        s.for_each(ascii, name='NOMETIC_EC')

        # Has metric
        m = topo.source(EESMSourceM(), name='HASMETRIC_S_7')
        m = m.filter(EESMClassM(5), name='HASMETRIC_F_3')
        m = m.map(EESMClassM(7), name='HASMETRIC_M_2')
        m.for_each(EESMClassM(4), name='HASMETRIC_E_3')

        self.tester = Tester(topo)
        self.tester.local_check = self.check_suppress_metric
        self.tester.contents(m, [1,2,4,8,11,13,16,17,19])
        self.tester.test(self.test_ctxtype, self.test_config)

    def check_suppress_metric(self):
        job = self.tester.submission_result.job
        hms = []
        for op in job.get_operators():
            seen_suppress_metric = False
            for m in op.get_metrics():
                if m.name == 'nExceptionsSuppressed':
                    seen_suppress_metric = True
            if 'NOMETRIC_' in op.name:
                self.assertFalse(seen_suppress_metric, msg=op.name)
            elif 'HASMETRIC_' in op.name:
                hms.append(op)
                self.assertTrue(seen_suppress_metric, msg=op.name)

        self.assertEqual(4, len(hms))
        for _ in range(10):
            ok = True
            for op in hms:
                exp = int(op.name.split('_')[2])
                m = op.get_metrics(name='nExceptionsSuppressed')[0]
                if m.value != exp:
                    ok = False
            if ok:
                break
            time.sleep(2)
        for op in hms:
            exp = int(op.name.split('_')[2])
            m = op.get_metrics(name='nExceptionsSuppressed')[0]
            self.assertEqual(exp, m.value, msg=op.name)


class TestSuppressMetricService(TestSuppressMetricDistributed):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
