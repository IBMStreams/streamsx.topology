# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pypip"

@spl.map()
def find_a_pint_toolkit(*t):
    try:
        import pint
        return ("RTTK_PintImported",)
    except ImportError:
        return ("RTTK_NoPintsForYou",)
