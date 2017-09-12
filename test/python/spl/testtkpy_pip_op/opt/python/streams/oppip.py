# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pypip"

def spl_pip_packages():
    return ['pint']

@spl.map()
def find_a_pint(*t):
    try:
        import pint
        return ("RTOP_PintImported",)
    except ImportError:
        return ("RTOP_NoPintsForYou",)
