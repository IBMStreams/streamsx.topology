__author__ = 'wcmarsha'

from topology.utils.frameRetriever import functionId

class PortDeclaration(object):
    """
    Top-level declaration of a port
    Declares functions common across all ports, both input and output
    """
    def connect(self, *args, **kwargs):
        """
        Connects to another port
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def getName(self):
        """
        Gets name of port
        :return: the name of the port
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def getIndex(self):
        """
        Gets index of port
        :return: the index of the port
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))

    def getOperator(self):
        """
        Gets the operator this port resides in.
        :return: the Operator in which this port resides
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))
