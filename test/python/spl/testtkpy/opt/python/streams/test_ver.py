# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

from streamsx.spl import spl

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pyvers"

@spl.map()
def StreamsxVersion(*t):
    import streamsx.topology.topology as stt
    return t + ('aggregate', str(hasattr(stt.Window, 'aggregate')))
