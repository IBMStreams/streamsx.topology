# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

from streamsx.topology import graph

class Topology(object):
    """Topology that contains graph + operators"""
    def __init__(self, name, files=None):
        self.name=name
        self.graph = graph.SPLGraph(name)
        self.files = []

    def source(self, func):
        """
        Takes a zero-argument function that returns an iterable.
        Each tuple that is not None from the iterator returned
        from iter(func()) is present on the returned stream.
        :param func: A source function (zero arguments)
        :return: A stream whose tuples are the result of the output obtained by periodically invoking the provided
        function.
        """
        op = self.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionSource", func)
        oport = op.addOutputPort()
        return Stream(self, oport)

    def subscribe(self, topic, schema):
        op = self.graph.addOperator(kind=schema.subscribeOp())
        oport = op.addOutputPort(schema=schema)
        topicParam = {"topic": [topic]}
        op.setParameters(topicParam)
        return Stream(self, oport)

class Stream(object):
    """
    Definition of a data stream in python.
    """
    def __init__(self, topology, oport):
        self.topology = topology
        self.oport = oport

    def sink(self, func):
        """
        Takes a user provided function that does not return a value.
        :param func: The function that will use the tuples of this stream.
        :return: None
        """
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionSink", func)
        op.addInputPort(outputPort=self.oport)

    def filter(self, func):
        """
        Filters tuples from a stream using the supplied function.
        For each tuple on the stream the function is called passing
        the tuple, if the function return evalulates to true the
        tuple will be present on the returned stream, otherwise
        the tuple is filtered out.
        """
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionFilter", func)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def transform(self, func):
        """
        Transforms each tuple from this stream into 0 or 1 tuples using the supplied function.
        For each tuple on this stream, the returned stream will contain a tuple
        that is the result of the function when the return is not None.
        If the function returns None then no tuple is submitted to the returned 
        stream.
        """
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionTransform", func)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
     
    def multiTransform(self, func):
        """
        Transforms each tuple from this stream into 0 or more tuples using the supplied function.
        For each tuple on this stream, the returned stream will contain all non-None tuples in
        the iterator that is the result of the supplied function.
        Tuples will be added to the returned stream in the order the iterator
        returns them.
        If the return is None or an empty iterator then no tuples are added to
        the returned stream.
        """     
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionMultiTransform", func)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
    
    def isolate(self):
        """
        Guarantees that the upstream operation will run in a separate process from the downstream operation
        :param: None
        :return: None
        """
        op = self.topology.graph.addOperator("$Isolate$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def lowlatency(self):
        """
	      The function is guaranteed to run in the same process as the
        upstream Stream function. All streams that are created from the returned stream 
        are also guaranteed to run in the same process until endlowlatency() 
        is called.
        """
        op = self.topology.graph.addOperator("$LowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def endlowlatency(self):
        """
        Return a Stream that is no longer guaranteed to run in the same process
        as the calling stream. 
        """
        op = self.topology.graph.addOperator("$EndLowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

