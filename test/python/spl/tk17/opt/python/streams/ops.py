# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

from streamsx.spl import spl
import streamsx.ec as ec

# A toolkit that has an older release of the streamsx package
# saved under opt/python/.__splpy

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.tk17"

@spl.map()
def M17F(*t):
    return t + ('1.7','F')

@spl.map()
class M17C(object):
    def __init__(self, x):
        self.x = x
    def __call__(self, *t):
        return t + ('1.7','C', self.x)
