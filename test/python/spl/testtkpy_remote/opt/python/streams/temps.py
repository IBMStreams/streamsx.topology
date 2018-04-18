# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
from pint import UnitRegistry

# Import the SPL decorators
from streamsx.spl import spl

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.temps"

ureg = UnitRegistry()

@spl.map()
def ToFahrenheit(**t):
    c = ureg.Quantity(t['temp'], ureg.degC)
    t['temp'] = int(c.to('degF').magnitude)
    print('AAAA', t, flush=True)
    return t
