# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018

# Import the SPL decorators
from streamsx.spl import spl
import streamsx.ec as ec

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


@spl.filter()
def check_not_extracting(*t):
    return not spl.extracting() 

@spl.filter()
def check_ec_active(*t):
    return ec.is_active()

if not spl.extracting():
    import string

@spl.filter()
def check_protected_import(*t):
    return t[0] in string.ascii_lowercase

