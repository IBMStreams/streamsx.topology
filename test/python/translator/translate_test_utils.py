from streamsx.topology.topology import Topology

def setup(topo, translate):
    if translate:
        topo.features[Topology.TRANSLATE_FEATURE] = True


def check_stream(tc, stream, translate, kind=None):
    if translate:
        tc.assertTrue(stream._spl_translated)
        tc.assertEqual('spl.relational::' + kind, stream._op().kind)
    else:
        tc.assertFalse(hasattr(stream, '_spl_translated'))
