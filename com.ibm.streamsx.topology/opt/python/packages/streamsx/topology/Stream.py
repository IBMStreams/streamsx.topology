__author__ = 'wcmarsha'

class Stream(object):
    """
    Definition of a data stream in python.
    """
    def __init__(self, topology, oport):
        self.topology = topology
        self.oport = oport

    def printStream(self):
        """
        Prints the string representation of the tuples on this stream.
        :return: None
        """
        def printFunc(tup):
            print(tup)
        op = self.topology.getGraph().addOperator(printFunc)
        op.addInputPort(outputPort=self.oport)

    def transform(self, func):
        """
        Takes a user-provided function. The provided function takes a single argument, and returns a single value
        or None if it wishes to return nothing.
        :param func: The user provided function.
        :return: A stream containing the transformed output of this stream.
        """
        op = self.topology.getGraph().addOperator(func)
        OpOport = op.addOutputPort()
        op.addInputPort(outputPort=self.oport)
        return Stream(self.topology, OpOport)

    def sink(self, func):
        """
        Takes a user provided function that does not return a value.
        :param func: The function that will use the tuples of this stream.
        :return: None
        """
        op = self.topology.getGraph().addOperator(func)
        op.addInputPort(outputPort=self.oport)