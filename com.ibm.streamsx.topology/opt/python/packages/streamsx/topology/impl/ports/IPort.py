__author__ = 'wcmarsha'

from topology.api.ports.PortDeclaration import PortDeclaration
from topology.api.ports.InputPort import InputPort
from topology.impl.ports.OPort import OPort
from topology.impl.ports.Port import Port
from topology.utils.decorators import overrides

class IPort(Port, InputPort):
    def __init__(self, name, operator = None, index = None):
        self.name = name
        self.operator = operator
        self.index = index
        self.outputPorts = []

    @overrides(InputPort)
    def getOutputPorts(self):
        return self.outputPorts

    @overrides(PortDeclaration)
    def connect(self, oport):
        if not oport in self.outputPorts:
            self.outputPorts.append(oport)

        if not self in oport.inputPorts:
            oport.connect(self)

    def getSPLInputPort(self):
        _iport = {}
        _iport["type"] = "tuple<blob __splpy_o>"
        _iport["name"] = self.getName()
        _iport["connections"] = [port.getName() for port in self.getOutputPorts()]
        return _iport