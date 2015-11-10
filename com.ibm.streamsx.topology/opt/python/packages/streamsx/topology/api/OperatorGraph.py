from topology.utils.frameRetriever import functionId

class OperatorGraph(object):
    """
    Declaration of a flow graph that contains Python primitive operators
    """
    def getName(self):
        """
        returns the name of the graph
        :return: a string containing name of the graph
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def addOperator(self, method, name = None):
        """
        Adds an operator to the graph the invokes the supplies method. If a name is provided, it is assigned to the
        operator.
        :param method: not quite sure yet
        :return: the newly created operator
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def getOperators(self):
        """
        Returns an array of the operators in this graph.
        :return: an array of operators in this graph
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))


    def generateSPLGraph(self):
        """
        Returns a string containing the SPL representation of the Graph.
        :return: a string containing the SPL representation of the Graph.
        """
        raise Exception("Unimplemented interface method: %s" % functionId(self, 0))
