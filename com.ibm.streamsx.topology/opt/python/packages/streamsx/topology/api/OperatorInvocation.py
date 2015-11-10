__author__ = 'wcmarsha'

from topology.utils.frameRetriever import functionId

class OperatorInvocation(object):
    """
    Declaration of an OperatorInvocation.
    Keeps track of parameters, input ports, output ports, and contains a reference to the python class that will be
    used to operate on the Stream.
    """
    def getGraph(self):
        """
        returns the graph in which this operator resides
        :return: the OperatorGraph in which this operator resides
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def getName(self):
        """
        returns the name of the operator
        :return: a string containing name of the operator
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def getParameters(self):
        """
        Returns the parameters of the operator
        :return: a dictionary of parameters to the operator.
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def setParameters(self, params):
        """
        Adds the parameters contained in the params dictionary to the operator, overwriting any values which were
        previously present.
        :param param: a dictionary mapping keys of strings to values of either ints, floats, doubles, strings. The
        values can also be iterables of ints, floats, doubles, or strings.
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))

    def appendParameters(self, params):
        """
        Appends the parameters contained in the dictionary of the params arguments to the parameters of the operator.
        Does not overwrite existing values.
        :param param: a dictionary mapping keys of strings to values of either ints, floats, doubles, strings. The
        values can also be iterables of ints, floats, doubles, or strings.
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))

    def addInputPort(self, name=None, outputPort=None):
        """
        Creates and returns an inputPort. If a name is provided it is assigned, otherwise it is generated
        automatically. If an outputPort is provided, it is connected to the inputPort after creation.
        :param name: name of the inputPort
        :param outputport: the outputPort to be connected upon the creation on the inputPort
        :return: the newly created input port
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def addOutputPort(self, name=None, inputPort=None):
        """
        Creates and returns an outputPort. If a name is provided it is assigned, otherwise it is generated
        automatically. If an inputPort is provided, it is connected to the outputPort after creation.
        :param name: the name of the outputPort
        :param inputPort: the name of the inputPort to connect to after creation of the outputPort
        :return: the newly created outputPort
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))

    def getInputPorts(self):
        """
        Returns an array of the operator's input ports.
        :return: an array of the operator's input ports.
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def getOutputPorts(self):
        """
        Returns an array of the operator's output ports.
        :return: an array of the operator's output ports.
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def generateSPLOperator(self):
        """
        returns a string containing the SPL representation of the operator
        :return: a string containing the SPL representation of the operator
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))
