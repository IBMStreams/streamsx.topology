# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2017

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import super
from builtins import range
try:
  from future import standard_library
  standard_library.install_aliases()
except (ImportError,NameError):
  # nothing to do here
  pass 

import random
from streamsx.topology import graph
from streamsx.topology import schema
import streamsx.topology.functions
import json
import threading
import queue
import sys
import time
import inspect
from enum import Enum

class Topology(object):
    """The Topology class is used to define data sources, and is passed as a parameter when submittion an application.
       Topology keeps track of all sources, sinks, and data operations within your application.

       Instance variables:
           include_packages: Set of Python package names to be included in the built application. 

           exclude_packages: Set of Python package names to be excluded from the built application.
           When compiling the application using Anaconda this set is pre-loaded with Python pacakges from the 
           Anaconda pre-loaded  set of applications.

           package names in the include_packages set take precedence over package namers in the exclude_pacakges set.
    """  

    def __init__(self, name, files=None):
        self.name = name
        if sys.version_info.major == 3:
          self.opnamespace = "com.ibm.streamsx.topology.functional.python"
        elif sys.version_info.major == 2 and sys.version_info.minor == 7:
          self.opnamespace = "com.ibm.streamsx.topology.functional.python2"
        else:
          raise ValueError("Python version not supported.")
        self.include_packages = set() 
        self.exclude_packages = set() 
        if "Anaconda" in sys.version:
            import streamsx.topology.condapkgs
            self.exclude_packages.update(streamsx.topology.condapkgs._CONDA_PACKAGES)
        self.graph = graph.SPLGraph(self, name)
        if files is not None:
            self.files = files
        else:
            self.files = []

    def source(self, func, name=None):
        """
        Fetches information from an external system and presents that information as a stream.
        Tuples are obtained from an iterator obtained from the passed iterable
        or callable that returns an iterable.
        Each tuple that is not None from the iterator returned
        from iter(func()) is present on the returned stream.
        
        Args:
            func: An iterable or a zero-argument callable that returns an iterable of tuples.
            The callable must be either 
            * an iterable object
            * a function 
            * a lambda function
            * an instance of a callable class that implements the method `__call__(self)` and be picklable.
            Using a callable class allows state information such as user-defined parameters to be stored during class 
            initialization and utilized when the instance is called.
            A tuple is represented as a Python object that must be picklable.
        Returns:
            A Stream whose tuples are the result of the output obtained by invoking the provided callable or iterable.
        """
        if inspect.isroutine(func):
             pass
        elif callable(func):
             pass
        else:
             func = streamsx.topology.functions._IterableInstance(func)
        
        op = self.graph.addOperator(self.opnamespace+"::PyFunctionSource", func, name=name)
        oport = op.addOutputPort()
        return Stream(self, oport)

    def subscribe(self, topic, schema=schema.CommonSchema.Python):
        """
        Subscribe to a topic published by other Streams applications.
        A Streams application may publish a stream to allow other
        Streams applications to subscribe to it. A subscriber matches a
        publisher if the topic and schema match.

        By default a stream is subscribed as Python objects (schema.CommonSchema.Python)
        which connects to streams published to topic by Python Streams applications.

        JSON streams are subscribed to using schema.CommonSchema.Json. 
        Each tuple on the returned stream will be a Python dictionary
        object created by json.loads(tuple).
        Any publishing Streams application may have been implemented in any language.
       
        String streams are subscribed to using schema.CommonSchema.String .
        Each tuple on the returned stream will be a Python string object.
        Any publishing Streams application may have been implemented in any language.

        Args:
            topic: Topic to subscribe to.
            schema: schema.StreamSchema to subscribe to. Defaults to schema.CommonSchema.Python representing Python
                    objects.
        Returns:
            A Stream whose tuples have been published to the topic by other Streams applications.
        """
        op = self.graph.addOperator(kind="com.ibm.streamsx.topology.topic::Subscribe")
        oport = op.addOutputPort(schema=schema)
        subscribeParams = {'topic': topic, 'streamType': schema}
        op.setParameters(subscribeParams)
        return Stream(self, oport)
    

class Stream(object):
    """
    The Stream class is the primary abstraction within a streaming application. It represents a potentially infinite 
    series of tuples which can be operated upon to produce another stream, as in the case of Stream.map(), or 
    terminate a stream, as in the case of Stream.sink().
    """
    def __init__(self, topology, oport):
        self.topology = topology
        self.oport = oport

    def for_each(self, func, name=None):
        """
        Sends information as a stream to an external system.
        Takes a user provided callable that does not return a value.
        For each tuple that is on the stream `func(tuple)` is called.
        
        Args:
            func: A callable that takes a single parameter for the tuple and returns None.
            The callable must be one of: 
            * a function 
            * a lambda function
            * an instance of a callable class that implements 
              the method `__call__(self, tuple)` and be picklable.
            Using a callable class allows state information such as user-defined parameters to be stored during class 
            initialization and utilized when the instance is called.
            The callable is invoked for each incoming tuple.
        Returns:
            None
        """
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::PyFunctionSink", func, name=name)
        op.addInputPort(outputPort=self.oport)

    def sink(self, func, name=None):
        """
        Equivalent to calling the for_each() function
        """
        return self.for_each(func, name)

    def filter(self, func, name=None):
        """
        Filters tuples from a stream using the supplied callable `func`.
        For each tuple on the stream the callable is called passing
        the tuple, if the callable return evalulates to true the
        tuple will be present on the returned stream, otherwise
        the tuple is filtered out.
        
        Args:
            func: A callable that takes a single parameter for the tuple, and returns True or False.
            If True, the tuple is included on the returned stream.  If False, the tuple is filtered out.
            The callable must be one of:
            * a function
            * a lambda function
            * an instance of a callable class that implements 
              the method `__call__(self, tuple)` and be picklable.
            Using a callable class allows state information such as user-defined parameters to be stored during class 
            initialization and utilized when the instance is called.
            The callable is invoked for each incoming tuple.
        Returns:
            A Stream containing tuples that have not been filtered out.
        """
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::PyFunctionFilter", func, name=name)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport)

    def _map(self, func, schema, name=None):
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::PyFunctionTransform", func, name=name)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=schema)
        return Stream(self.topology, oport)

    def view(self, buffer_time = 10.0, sample_size = 10000, name=None):
        """
        Defines a view on a stream. Returns a view object which can be used to access the data
        :param buffer_time The window of time over which tuples will be
        :param name Name of the view. Name must be unique within the topology. Defaults to a generated name.
        """
        new_op = self._map(streamsx.topology.functions.identity,schema=schema.CommonSchema.Json)
        if name is None:
            name = ''.join(random.choice('0123456789abcdef') for x in range(16))

        port = new_op.oport.name
        new_op.oport.operator.addViewConfig({
                'name': name,
                'port': port,
                'bufferTime': buffer_time,
                'sampleSize': sample_size})
        _view = View(name, port, buffer_time, sample_size)
        self.topology.graph.get_views().append(_view)
        return _view
        

    def map(self, func, name=None):
        """
        Maps each tuple from this stream into 0 or 1 tuples using the supplied callable `func`.
        For each tuple on this stream, the returned stream will contain a tuple
        that is the result of the callable when the return is not None.
        If the callable returns None then no tuple is submitted to the returned 
        stream.
        
        Args:
            func: A callable that takes a single parameter for the tuple, and returns a tuple or None.
            The callable must be either
            * a function
            * a lambda function
            * an instance of a callable class that implements 
              the method `__call__(self, tuple)` and be picklable.
            Using a callable class allows state information such as user-defined parameters to be stored during class 
            initialization and utilized when the instance is called.
            The callable is invoked for each incoming tuple.
        Returns:
            A Stream containing transformed tuples.
        """
        return self._map(func, schema=schema.CommonSchema.Python, name=name)

    def transform(self, func, name=None):
        """
        Equivalent to calling the map() function
        """
        return self.map(func, name)
             
    def flat_map(self, func, name=None):
        """
        Transforms each tuple from this stream into 0 or more tuples using the supplied callable `func`. 
        For each tuple on this stream, the returned stream will contain all non-None tuples from
        the iterable.
        Tuples will be added to the returned stream in the order the iterable
        returns them.
        If the return is None or an empty iterable then no tuples are added to
        the returned stream.
        
        Args:
            func: A callable that takes a single parameter for the tuple, and returns an iterable of tuples or None.
            The callable must return an iterable or None, otherwise a TypeError is raised.
            The callable must be either
            * a function
            * a lambda function
            * an instance of a callable class that implements 
              the method `__call__(self, tuple)` and be picklable.
            Using a callable class allows state information such as user-defined parameters to be stored during class 
            initialization and utilized when the instance is called.
            The callable is invoked for each incoming tuple.
        Returns:
            A Stream containing transformed tuples.
        Raises:
            TypeError: if `func` does not return an iterator nor None
        """     
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::PyFunctionMultiTransform", func, name=name)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
    
    def multi_transform(self, func, name=None):
        """
        Equivalent to calling the flat_map() function
        """
        return self.flat_map(func, name)

    def isolate(self):
        """
        Guarantees that the upstream operation will run in a separate process from the downstream operation
        
        Args:
            None
        Returns:
            Stream
        """
        op = self.topology.graph.addOperator("$Isolate$")
        # does the addOperator above need the packages
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def low_latency(self):
        """
        The function is guaranteed to run in the same process as the
        upstream Stream function. All streams that are created from the returned stream 
        are also guaranteed to run in the same process until end_low_latency() 
        is called.
        
        Args:
            None
        Returns:
            Stream
        """
        op = self.topology.graph.addOperator("$LowLatency$")
        # include_packages=self.include_packages, exclude_packages=self.exclude_packages)
        # include_packages=self.include_packages, exclude_packages=self.exclude_packages)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def end_low_latency(self):
        """
        Returns a Stream that is no longer guaranteed to run in the same process
        as the calling stream.
        
        Args:
            None
        Returns:
            Stream
        """
        op = self.topology.graph.addOperator("$EndLowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)
    
    def parallel(self, width, routing=None, func=None):
        """
        Parallelizes the stream into `width` parallel channels.
        Tuples are routed to parallel channels such that an even distribution is maintained.
        Each parallel channel can be thought of as being assigned its own thread.
        As such, each parallelized stream function are separate instances and operate independently 
        from one another.
        
        parallel() will only parallelize the stream operations performed after the call to parallel() and 
        before the call to end_parallel().
        
        Parallel regions aren't required to have an output stream, and thus may be used as sinks.
        In other words, a parallel sink is created by calling parallel() and creating a sink operation.
        It is not necessary to invoke end_parallel() on parallel sinks.
        
        Nested parallelism is not currently supported.
        A call to parallel() should never be made immediately after another call to parallel() without 
        having an end_parallel() in between.
        
        Every call to end_parallel() must have a call to parallel() preceding it.
        
        Args:
            width (int): degree of parallelism
            routing - denotes what type of tuple routing to use. 
                ROUND_ROBIN: delivers tuples in round robin fashion to downstream operators
                HASH_PARTIONED: delivers to downstream operators based on the hash of the tuples being sent
                or if a function is provided the function will be called to provide the hash
            func - Optional function called when HASH_PARTIONED routing is specified.  The function provides an
                int32 value to be used as the hash that determines the tuple routing to downstream operators

        Returns:
            Stream

        """
        if (routing == None or routing == Routing.ROUND_ROBIN) :
            iop = self.isolate()                  
            op2 = self.topology.graph.addOperator("$Parallel$")
            op2.addInputPort(outputPort=iop.oport)
            oport = op2.addOutputPort(width)
            return Stream(self.topology, oport)
        elif(routing == Routing.HASH_PARTITIONED ) :
            if (func is None) :
                func = hash   
            op = self.topology.graph.addOperator(self.topology.opnamespace+"::PyFunctionHashAdder", func)
            hash_schema = self.oport.schema.extend(schema.StreamSchema("tuple<int32 __spl_hash>"))
            parentOp = op.addOutputPort(schema=hash_schema)
            op.addInputPort(outputPort=self.oport)
            iop = self.topology.graph.addOperator("$Isolate$")    
            oport = iop.addOutputPort(schema=hash_schema)
            iop.addInputPort(outputPort=parentOp)        
            op2 = self.topology.graph.addOperator("$Parallel$")
            op2.addInputPort(outputPort=oport)
            o2port = op2.addOutputPort(oWidth=width, schema=hash_schema, partitioned=True)
            # use the Functor passthru operator to effectively remove the hash attribute by removing it from output port schema 
            hrop = self.topology.graph.addPassThruOperator()
            hrop.addInputPort(outputPort=o2port)
            hrOport = hrop.addOutputPort(schema=self.oport.schema)
            return Stream(self.topology, hrOport)
        else :
            raise TypeError("Invalid routing type supplied to the parallel operator")    

    def end_parallel(self):
        """
        Ends a parallel region by merging the channels into a single stream
        
        Args:
            None
        Returns:
            A Stream for which subsequent transformations are no longer parallelized
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
        Creates a stream that is a union of this stream and other streams
        
        Args:
            streamSet: a set of Stream objects to merge with this stream
        Returns:
            Stream
        """
        if(not isinstance(streamSet,set)) :
            raise TypeError("The union operator parameter must be a set object")
        if(len(streamSet) == 0):
            return self        
        op = self.topology.graph.addOperator("$Union$")
        op.addInputPort(outputPort=self.oport)
        for stream in streamSet:
            op.addInputPort(outputPort=stream.oport)
        oport = op.addOutputPort()
        return Stream(self.topology, oport)

    def print(self):
        """
        Prints each tuple to stdout flushing after each tuple.
        :returns: None
        """
        self.sink(streamsx.topology.functions.print_flush)

    def publish(self, topic, schema=schema.CommonSchema.Python):
        """
        Publish this stream on a topic for other Streams applications to subscribe to.
        A Streams application may publish a stream to allow other
        Streams applications to subscribe to it. A subscriber
        matches a publisher if the topic and schema match.

        By default a stream is published as Python objects (CommonSchema.Python)
        which allows other Streams Python applications to subscribe to
        the stream using the same topic.

        If a stream is published with CommonSchema.Json then it is published
        as JSON, other Streams applications may subscribe to it regardless
        of their implementation language. A Python tuple is converted to
        JSON using json.dumps(tuple, ensure_ascii=False).

        If a stream is published with CommonSchema.String then it is published
        as strings, other Streams applications may subscribe to it regardless
        of their implementation language. A Python tuple is converted to
        a string using str(tuple).

        Args:
            topic: Topic to publish this stream to.
            schema: Schema to publish. Defaults to CommonSchema.Python representing Python objects.
        Returns:
            None.
        """
        if self.oport.schema.schema() != schema.schema():
            self._map(streamsx.topology.functions.identity,schema=schema).publish(topic, schema=schema);
            return None

        publishParams = {'topic': topic}
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.topic::Publish", params=publishParams)
        op.addInputPort(outputPort=self.oport)

    def autonomous(self):
        """
        Starts an autonomous region for downstream processing.
        By default IBM Streams processing is executed in an autonomous region
        where any checkpointing of operator state is autonomous (independent)
        of other operators.
        
        This function may be used to end a consistent region by starting an
        autonomous region. This may be called even if this stream is in
        an autonomous region.

        Autonomous is not applicable when a topology is submitted
        to a STANDALONE contexts and will be ignored.

        Supported since v1.5

        Args:
            None
        Returns:
            Stream
        """
        op = self.topology.graph.addOperator("$Autonomous$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport)

class Routing(Enum):
    ROUND_ROBIN=1
    KEY_PARTITIONED=2
    HASH_PARTITIONED=3    


class View(threading.Thread):
    """
    A View is an object which is associated with a Stream, and provides access to the items on the stream.
    """
    def __init__(self, name, port, buffer_time, sample_size):
        super(View, self).__init__()
        self._stop = threading.Event()
        self.items = queue.Queue()

        self.name = name
        self.port = port
        self.buffer_time = buffer_time
        self.sample_size = sample_size
        self.streams_context = None
        self.view_object = None
        self.streams_context_config = {'username': '', 'password': '', 'rest_api_url': ''}

        self._last_collection_time = -1
        self._last_collection_time_count = 0
        self.is_rest_initialized = False

    def initialize_rest(self):
        if not self.is_rest_initialized:
            if self.streams_context_config['username'] is None or \
               self.streams_context_config['password'] is None or \
               self.streams_context_config['rest_api_url'] is None:
                raise ValueError(
                    "WARNING: A username, a password, and a rest url must be present in order to access view data")
            from streamsx import rest
            rc = rest.StreamsContext(self.streams_context_config['username'],
                                     self.streams_context_config['password'],
                                     self.streams_context_config['rest_api_url'])
            self.is_rest_initialized = True
            self.set_streams_context(rc)

    def stop_data_fetch(self):
        self._stop.set()

    def start_data_fetch(self):
        self.initialize_rest()
        self._stop.clear()
        self._get_view_object()
        t = threading.Thread(target=self)
        t.start()
        return self.items

    def __call__(self):
        while not self._stopped():
            time.sleep(1)
            _items = self._get_view_items()
            if _items is not None:
                for itm in _items:
                    self.items.put(itm)

    def set_streams_context_config(self, conf):
        self.streams_context_config = conf

    def get_streams_context_config(self):
        return self.streams_context_config

    def set_streams_context(self, sc):
        self.streams_context = sc

    def get_streams_context(self):
        return self.streams_context


    # Private

    def _stopped(self):
        return self._stop.isSet()

    def _get_view_object(self):
        self.view_object = self._get_view_obj_from_name()
        if self.view_object is None:
            raise "Error finding view."

    def _get_view_items(self):
        # Retrieve the view object
        view = self.view_object
        if self.view_object is None:
            return None

        data_name = view.attributes[0]['name']
        items = view.get_view_items()
        data = []

        # The number of already seen tuples to ignore on the last millisecond time boundary
        ignore_last_collection_time_count = self._last_collection_time_count

        for item in items:
            # Ignore tuples from milliseconds we've already seen
            if item.collectionTime < self._last_collection_time:
                continue
            elif item.collectionTime == self._last_collection_time:
                # Ignore tuples within the millisecond which we've already seen.
                if ignore_last_collection_time_count > 0:
                    ignore_last_collection_time_count -= 1
                    continue

                # If we haven't seen it, continue
                data.append(json.loads(item.data[data_name]))
            else:
                data.append(json.loads(item.data[data_name]))

        if len(items) > 0:
            # Record the current millisecond time boundary.
            _last_collection_time = items[-1].collectionTime
            _last_collection_time_count = 0
            backwards_counter = len(items) - 1
            while backwards_counter >= 0 and items[backwards_counter].collectionTime == _last_collection_time:
                _last_collection_time_count += 1
                backwards_counter -= 1

            self._last_collection_time = _last_collection_time
            self._last_collection_time_count = _last_collection_time_count

        return data

    # TODO: update to use domain, instance, job *and* view name
    def _get_view_obj_from_name(self):
        for domain in self.streams_context.get_domains():
            for instance in domain.get_instances():
                for view in instance.get_views():
                    if view.name == self.name:
                        return view
        return None
