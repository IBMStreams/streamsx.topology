# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

from streamsx.topology.api.ports.PortDeclaration import PortDeclaration
from streamsx.topology.utils.frameRetriever import functionId

class InputPort(PortDeclaration):

    def getOutputPorts(self):
        """
        Returns a list of output ports connected to the input port
        :return: a list of output ports connected to the input port
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))
