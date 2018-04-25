# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import unittest
import sys

from streamsx.topology.topology import Topology
from streamsx.topology.schema import CommonSchema

class CallTopo(object):
    def __init__(self):
        pass

    def ecruos(self, topo):
        return topo.source(['Hello'])

    def ebircsbus(self, topo):
        return topo.subscribe('some/topic')

    def retlif(self, s):
        return s.filter(lambda x : True)

    def pam(self, s):
        return s.map(lambda x : x)

    def pam_talf(self, s):
        return s.flat_map(lambda x : [x])

    def gnirts_sa(self, s):
        return s.as_string()

    def nosj_sa(self, s):
        return s.as_json()

    def hcae_rof(self, s):
        return s.for_each(lambda x:None)

    def hsilbup(self, s, schema=None):
        return s.publish('some/topic', schema)

    def tnirp(self, s):
        return s.print()
    
def fn_ecruos(topo):
    return topo.source(['Hello'])

def fn_ebircsbus(topo):
    return topo.subscribe('some/topic')

def fn_retlif(s):
    return s.filter(lambda x : True)

def fn_pam(s):
    return s.map(lambda x : x)

def fn_pam_talf(s):
    return s.flat_map(lambda x : [x])

def fn_gnirts_sa(s):
    return s.as_string()

def fn_nosj_sa(s):
    return s.as_json()

def fn_hcae_rof(s):
    return s.for_each(lambda x:None)

def fn_hsilbup(s, schema=None):
    return s.publish('some/topic', schema)

def fn_tnirp(s):
    return s.print()
    
class TestSourceLocations(unittest.TestCase):
    def _csl_stream(self, stream, api, meth, cls=None):
        self._csl_op(stream._op(), api, meth, cls)

    def _csl_sink(self, sink, api, meth, cls=None):
        self._csl_op(sink._op(), api, meth, cls)

    def _csl_op(self, op, api, meth, cls=None):
        sl = op.sl
        jsl = sl.spl_json()
        self.assertEqual(api, jsl['api.method'])
        self.assertEqual(meth, jsl['method'])
        self.assertTrue(api, jsl['line'] > 0)
        if cls:
            self.assertEqual(cls, jsl['class'])
        else:
            self.assertNotIn('class', jsl)

    def test_class(self):
        topo = Topology()
        ct = CallTopo()

        s = ct.ecruos(topo)
        self._csl_stream(s, 'source', 'ecruos', cls='CallTopo')

        s = ct.retlif(s)
        self._csl_stream(s, 'filter', 'retlif', cls='CallTopo')

        s = ct.pam(s)
        self._csl_stream(s, 'map', 'pam', cls='CallTopo')

        s = ct.pam_talf(s)
        self._csl_stream(s, 'flat_map', 'pam_talf', cls='CallTopo')
        
        s = ct.gnirts_sa(s)
        self._csl_stream(s, 'as_string', 'gnirts_sa', cls='CallTopo')

        s = ct.nosj_sa(s)
        self._csl_stream(s, 'as_json', 'nosj_sa', cls='CallTopo')

        st = ct.ebircsbus(topo)
        self._csl_stream(st, 'subscribe', 'ebircsbus', cls='CallTopo')

        e = ct.hcae_rof(s)
        self._csl_sink(e, 'for_each', 'hcae_rof', cls='CallTopo')

        e = ct.hsilbup(s)
        self._csl_sink(e, 'publish', 'hsilbup', cls='CallTopo')

        # test with implict schema change
        e = ct.hsilbup(topo.source([]), schema=CommonSchema.Json)
        self._csl_sink(e, 'publish', 'hsilbup', cls='CallTopo')

        e = ct.tnirp(s)
        self._csl_sink(e, 'print', 'tnirp', cls='CallTopo')

    def test_fn(self):
        topo = Topology()

        s = fn_ecruos(topo)
        self._csl_stream(s, 'source', 'fn_ecruos')

        s = fn_retlif(s)
        self._csl_stream(s, 'filter', 'fn_retlif')

        s = fn_pam(s)
        self._csl_stream(s, 'map', 'fn_pam')

        s = fn_pam_talf(s)
        self._csl_stream(s, 'flat_map', 'fn_pam_talf')
        
        s = fn_gnirts_sa(s)
        self._csl_stream(s, 'as_string', 'fn_gnirts_sa')

        s = fn_nosj_sa(s)
        self._csl_stream(s, 'as_json', 'fn_nosj_sa')

        st = fn_ebircsbus(topo)
        self._csl_stream(st, 'subscribe', 'fn_ebircsbus')

        e = fn_hcae_rof(s)
        self._csl_sink(e, 'for_each', 'fn_hcae_rof')

        e = fn_hsilbup(s)
        self._csl_sink(e, 'publish', 'fn_hsilbup')

        e = fn_hsilbup(topo.source([]), schema=CommonSchema.Json)
        self._csl_sink(e, 'publish', 'fn_hsilbup')

        e = fn_tnirp(s)
        self._csl_sink(e, 'print', 'fn_tnirp')
