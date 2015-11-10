__author__ = 'wcmarsha'

from topology.api.ports.PortDeclaration import PortDeclaration
from topology.utils.decorators import overrides

class Port(PortDeclaration):
    """
    A convenient way of grouping methods that are common across port implementations.
    """

    @overrides(PortDeclaration)
    def getName(self):
        return self.name

    @overrides(PortDeclaration)
    def getIndex(self):
        return self.index

    @overrides(PortDeclaration)
    def getOperator(self):
        return self.operator
