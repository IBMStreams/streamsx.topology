# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test passing in SPL types functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.pyprimitives"

@spl.primitive_operator()
class NoPorts(object):
    def __init__(self, mn, iv):
        self.mn = mn
        self.iv = iv

    def __enter__(self):
        import streamsx.ec as ec
        self.cm = ec.CustomMetric(self, 'NP_' + self.mn)
        self.cm.value = self.iv

    def __exit__(self, exc_type, exc_value, traceback):
        pass
   
@spl.primitive_operator()
class SingleInputPort(object):
    def __init__(self):
        pass

    def __enter__(self):
        import streamsx.ec as ec
        self.cm = ec.CustomMetric(self, 'SIP_METRIC')

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @spl.input_port()
    def my_only_port(self, *t):
        self.cm.value = t[0] + 17

@spl.primitive_operator()
class MultiInputPort(object):
    def __init__(self):
        pass

    def __enter__(self):
        import streamsx.ec as ec
        self.cm0 = ec.CustomMetric(self, 'MIP_METRIC_0')
        self.cm1 = ec.CustomMetric(self, 'MIP_METRIC_1')
        self.cm2 = ec.CustomMetric(self, 'MIP_METRIC_2')

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @spl.input_port()
    def port0(self, *t):
        self.cm0.value = t[0] + 17

    @spl.input_port()
    def port1(self, **t):
        self.cm1.value = t['v'] + 34

    @spl.input_port()
    def port2(self, *t):
        self.cm2.value = t[0] + 51
