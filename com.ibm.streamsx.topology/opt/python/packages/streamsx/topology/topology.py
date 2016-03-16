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
        Fetches information from an external system and presents that information as a stream.
        Takes a zero-argument callable that returns an iterable of tuples.
        Each tuple that is not None from the iterator returned
        from iter(func()) is present on the returned stream.
        :param func: A zero-argument callable that returns an iterable of tuples.
        The callable must be either 
        * the name of a function defined at the top level of a module that takes no arguments, or
        * an instance of a callable class defined at the top level of a module that implements the method `__call__(self)`.
        Using a callable class allows state information such as user-defined parameters to be stored during class 
        initialization and utilized when the instance is called.
        A tuple is represented as a Python object that must be picklable.
        :return: A stream whose tuples are the result of the output obtained by invoking the provided callable.
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
        Sends information as a stream to an external system.
        Takes a user provided callable that does not return a value.
        :param func: A callable that takes a single parameter for the tuple and returns None.
        The callable must be either 
        * the name of a function defined at the top level of a module that takes a single parameter for the tuple, or
        * an instance of a callable class defined at the top level of a module that implements the method `__call__(self, tuple)`.
        Using a callable class allows state information such as user-defined parameters to be stored during class 
        initialization and utilized when the instance is called.
        The callable is invoked for each incoming tuple.  
        :return: None
        """
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionSink", func)
        op.addInputPort(outputPort=self.oport)

    def filter(self, func):
        """
        Filters tuples from a stream using the supplied callable `func`.
        For each tuple on the stream the callable is called passing
        the tuple, if the callable return evalulates to true the
        tuple will be present on the returned stream, otherwise
        the tuple is filtered out.
        :param func: A callable that takes a single parameter for the tuple, and returns True or False.
        If True, the tuple is included on the returned stream.  If False, the tuple is filtered out.
        The callable must be either
        * the name of a function defined at the top level of a module that takes a single parameter for the tuple, or
        * an instance of a callable class defined at the top level of a module that implements the method `__call__(self, tuple)`.
        Using a callable class allows state information such as user-defined parameters to be stored during class 
        initialization and utilized when the instance is called.
        The callable is invoked for each incoming tuple.
        :return: A stream containing tuples that have not been filtered out.
        """
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionFilter", func)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def transform(self, func):
        """
        Transforms each tuple from this stream into 0 or 1 tuples using the supplied callable `func`.
        For each tuple on this stream, the returned stream will contain a tuple
        that is the result of the callable when the return is not None.
        If the callable returns None then no tuple is submitted to the returned 
        stream.
        :param func: A callable that takes a single parameter for the tuple, and returns a tuple or None.
        The callable must be either
        * the name of a function defined at the top level of a module that takes a single parameter for the tuple, or
        * an instance of a callable class defined at the top level of a module that implements the method `__call__(self, tuple)`.
        Using a callable class allows state information such as user-defined parameters to be stored during class 
        initialization and utilized when the instance is called.
        The callable is invoked for each incoming tuple.
        :return: A stream containing transformed tuples.
        """
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.functional.python::PyFunctionTransform", func)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
     
    def multi_transform(self, func):
        """
        Transforms each tuple from this stream into 0 or more tuples using the supplied callable `func`. 
        For each tuple on this stream, the returned stream will contain all non-None tuples from
        the iterable.
        Tuples will be added to the returned stream in the order the iterable
        returns them.
        If the return is None or an empty iterable then no tuples are added to
        the returned stream.
        :param func: A callable that takes a single parameter for the tuple, and returns an iterable of tuples or None.
        The callable must return an iterable or None, otherwise a TypeError is raised.
        The callable must be either
        * the name of a function defined at the top level of a module that takes a single parameter for the tuple, or
        * an instance of a callable class defined at the top level of a module that implements the method `__call__(self, tuple)`.
        Using a callable class allows state information such as user-defined parameters to be stored during class 
        initialization and utilized when the instance is called.
        The callable is invoked for each incoming tuple.
        :return: A stream containing transformed tuples.
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

    def low_latency(self):
        """
	      The function is guaranteed to run in the same process as the
        upstream Stream function. All streams that are created from the returned stream 
        are also guaranteed to run in the same process until end_low_latency() 
        is called.
        """
        op = self.topology.graph.addOperator("$LowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def end_low_latency(self):
        """
        Return a Stream that is no longer guaranteed to run in the same process
        as the calling stream. 
        """
        op = self.topology.graph.addOperator("$EndLowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
    
    def parallel(self, width):
        """
        Marks operator as parallel output with width
        :param: width
        :returns: Stream
        """
        iop = self.isolate()
               
        op2 = self.topology.graph.addOperator("$Parallel$")
        op2.addInputPort(outputPort=iop.getOport())
        oport = op2.addOutputPort(width)
        return Stream(self.topology, oport)

    def end_parallel(self):
        """
        Marks end of operators as parallel output with width
        :returns: Stream
        """
        lastOp = self.topology.graph.getLastOperator()
        outport = self.oport
        if (isinstance(lastOp, graph.Marker)):
            if (lastOp.kind == "$Union$"):
                pto = self.topology.graph.addPassThruOperator()
                pto.addInputPort(outputPort=self.oport)
                outport = pto.addOutputPort()
        op = self.topology.graph.addOperator("$EndParallel$")
        op.addInputPort(outputPort=outport)
        oport = op.addOutputPort()
        endP = Stream(self.topology, oport)
        return endP.isolate()

    def union(self, streamSet):
        """
        The Union operator merges the outputs of the streams in the set 
        into a single stream.
        :param set of streams outputs to merge
        :returns Stream
        """
        if(not isinstance(streamSet,set)) :
            raise TypeError("The union operator parameter must be a set object")
        if(len(streamSet) == 0):
            return self        
        op = self.topology.graph.addOperator("$Union$")
        op.addInputPort(outputPort=self.getOport())
        for stream in streamSet:
            op.addInputPort(outputPort=stream.getOport())
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
    
    def getOport(self):
        return self.oport
