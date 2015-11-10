__author__ = 'wcmarsha'

from topology.api.ports.PortDeclaration import PortDeclaration
from topology.utils.frameRetriever import functionId

class InputPort(PortDeclaration):

    def getOutputPorts(self):
        """
        Returns a list of output ports connected to the input port
        :return: a list of output ports connected to the input port
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))
