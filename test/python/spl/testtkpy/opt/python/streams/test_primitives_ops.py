# coding=utf-8
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

@spl.primitive_operator(output_ports=['A'])
class SingleOutputPort(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port()
    def port0(self, *t):
        self.submit('A', t)

@spl.primitive_operator(output_ports=['A'])
class SingleOutputPortPunct(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port()
    def port0(self, *t):
        self.submit_punct('A')
        self.submit('A', t)

@spl.primitive_operator(output_ports=['A'])
class SingleOutputPortPunctForward(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port()
    def port0(self, *t):
        self.submit('A', t)

    def on_punct(self):
        self.submit_punct('A')

@spl.for_each(style='position')
class VerifyPosInt(object):
    def __init__(self): 
        pass

    def __call__(self, value):
        assert value > 0
        
    def on_punct(self):
        pass

@spl.primitive_operator(output_ports=[1,2,3])
class MultiOutputPorts(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port()
    def port0(self, *t):
        self.submit(1, t)
        self.submit(2, (t[0] + 921,))
        self.submit(3, (t[0] - 407,))

@spl.primitive_operator(output_ports=[1,2,3])
class MultiOutputPortsPunct(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port()
    def port0(self, *t):
        self.submit(1, t)
        self.submit_punct(1)
        self.submit(2, (t[0] + 921,))
        self.submit_punct(2)
        self.submit(3, (t[0] - 407,))
        self.submit_punct(3)

@spl.primitive_operator(output_ports=['A', 'B'])
class DictOutputPorts(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port()
    def port0(self, **tuple_):
        self.submit('A', tuple_)
        self.submit('B', [{'d':tuple_['d'] + 7, 'e':tuple_['f'] + 77, 'f':tuple_['e'] + 777}, tuple_],)


@spl.primitive_operator(output_ports=['A'])
class InputByPosition(spl.PrimitiveOperator):
    def __init__(self):
        pass

    @spl.input_port(style='position')
    def port0(self, a, b):
        self.submit('A', (a,b+89,-92))

import threading
@spl.primitive_operator(output_ports=['A'])
class OutputOnly(spl.PrimitiveOperator):
    def __init__(self, count):
        self.count = count

    def all_ports_ready(self):
        self.submitter = threading.Thread(target=self.submit_some)
        self.submitter.start()
        return self.submitter.join

    def submit_some(self):
        for i in range(self.count):
            self.submit('A', (i+501,))
