__author__ = 'wcmarsha'

from topology.api.ports.OutputPort import OutputPort
from topology.api.ports.PortDeclaration import PortDeclaration
from topology.impl.ports.Port import Port
from topology.utils.decorators import overrides

class OPort(Port, OutputPort):
    def __init__(self, name, operator = None, index = None):
        self.name = name
        self.operator = operator
        self.index = index

        self.inputPorts = []

    @overrides(OutputPort)
    def getInputPorts(self):
        return self.inputPorts

    @overrides(PortDeclaration)
    def connect(self, iport):
        if not iport in self.inputPorts:
            self.inputPorts.append(iport)
        
        if not self in iport.outputPorts:
            iport.connect(self)

    def getSPLOutputPort(self):
        _oport = {}
        _oport["type"] = "tuple<blob __splpy_o>"
        _oport["name"] = self.getName()
        _oport["connections"] = [port.getName() for port in self.getInputPorts()]
        return _oport

