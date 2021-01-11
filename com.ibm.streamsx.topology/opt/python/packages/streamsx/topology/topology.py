# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2019

"""
Streaming application definition.

********
Overview
********

IBM Streams is an advanced analytic platform that allows user-developed
applications to quickly ingest, analyze and correlate information as it
arrives from thousands of real-time sources.
Streams can handle very high data throughput rates, millions of events
or messages per second.

With this API Python developers can build streaming applications
that can be executed using IBM Streams, including the processing
being distributed across multiple computing resources
(hosts or machines) for scalability.

********
Topology
********

A :py:class:`Topology` declares a graph of *streams* and *operations* against
tuples (data items) on those streams.

After being declared, a Topology is submitted to be compiled into
a Streams application bundle (sab file) and then executed.
The sab file is a self contained bundle that can be executed
in a distributed Streams instance either using the Streaming
Analytics service on IBM Cloud or an on-premise
IBM Streams installation.

The compilation step invokes the Streams compiler to produce a bundle.
This effectively, from a Python point of view, produces a runnable
version of the Python topology that includes application
specific Python C extensions to optimize performance.

The bundle also includes any required Python packages or modules
that were used in the declaration of the application, excluding
ones that are in a directory path containing ``site-packages``.

The Python standard package tool ``pip`` uses a directory structure
including ``site-packages`` when installing packages. Packages installed
with ``pip`` can be included in the bundle with
:py:meth:`~Topology.add_pip_package` when using a build service.
This avoids the requirement to have packages be preinstalled in cloud environments.

Local Python packages and modules containing callables used in transformations
such as :py:meth:`~Stream.map` are copied into the bundle from their
local location.  The addition of local packages to the bundle can be controlled
with :py:attr:`Topology.include_packages` and
:py:attr:`Topology.exclude_packages`.

The Streams runtime distributes the application's operations
across the resources available in the instance.

.. note::
    `Topology` represents a declaration of a streaming application that
    will be executed by a Streams instance as a `job`, either using the Streaming Analytics
    service on IBM Cloud or an on-premises distributed instance.
    `Topology` does not represent a running application, so an instance of `Stream` class does not contain
    the tuples, it is only a declaration of a stream.

.. _stream-desc:

******
Stream
******

A :py:class:`Stream` can be an infinite sequence of tuples, such as a stream for a traffic flow sensor.
Alternatively, a stream can be finite, such as a stream that is created from the contents of a file.
When a streams processing application contains infinite streams, the application runs continuously without ending.

A stream has a schema that defines the type of each tuple on the stream.
The schema for a stream is either:

* :py:const:`~streamsx.topology.schema.CommonSchema.Python` - A tuple may be any Python object. This is the default when the schema is not explictly or implicitly set.
* :py:const:`~streamsx.topology.schema.CommonSchema.String` - Each tuple is a Unicode string.
* :py:const:`~streamsx.topology.schema.CommonSchema.Binary` - Each tuple is a blob.
* :py:const:`~streamsx.topology.schema.CommonSchema.Json` - Each tuple is a Python dict that can be expressed as a JSON object.
* Structured - A stream that has a structured schema of a ordered list of attributes, with each attribute having a fixed type (e.g. float64 or int32) and a name. The schema of a structured stream is defined using typed named tuple or :py:const:`~streamsx.topology.schema.StreamSchema`.

A stream's schema is implictly dervied from type hints declared for the callable
of the transform that produces it. For example `readings` defined as follows would have a structured schema matching ``SensorReading`` ::

    class SensorReading(typing.NamedTuple):
        sensor_id: str
        ts: int
        reading: float
    
    def reading_from_json(value:dict) -> SensorReading:
        return SensorReading(value['id'], value['timestamp'], value['reading'])

    topo = Topology()
    json_readings = topo.source(HttpReadings()).as_json()
    readings = json_readings.map(reading_from_json)

Deriving schemas from type hints can be disabled by setting the topology's
``type_checking`` attribute to false, for example this would change `readings`
in the previous example to have generic Python object schema :py:const:`~streamsx.topology.schema.CommonSchema.Python` ::

    topo = Topology()
    topo.type_checking = False

*****************
Stream processing
*****************

Callables
=========

A stream is processed to produce zero or more transformed streams,
such as filtering a stream to drop unwanted tuples, producing a stream
that only contains the required tuples.

Streaming processing is per tuple based, as each tuple is submitted to a stream consuming operators
have their processing logic invoked for that tuple.

A functional operator is declared by methods on :py:class:`Stream` such as :py:meth:`~Stream.map` which
maps the tuples on its input stream to tuples on its output stream. `Stream` uses a functional model
where each stream processing operator is defined in terms a Python callable that is invoked passing
input tuples and whose return defines what output tuples are submitted for downstream processing.

The Python callable used for functional processing in this API may be:

* A Python lambda function.
* A Python function.
* An instance of a Python callable class.

For example a stream ``words`` containing only string objects can be
processed by a :py:meth:`~Stream.filter` using a lambda function::

    # Filter the stream so it only contains words starting with py
    pywords = words.filter(lambda word : word.startswith('py'))

When a callable has type hints they are used to:

   * define the schema of the resulting transformation, see  :ref:`stream-desc`.
   * type checking the correctness of the transformation at topology declaration time.

For example if the callable defining the source had type hints that indicated
it was an iterator of ``str`` objects then the schema of the resultant stream
would be :py:const:`~streamsx.topology.schema.CommonSchema.String`. If this
source stream then underwent a :py:meth:`Stream.map` transform with a callable
that had a type hint for its argument, a check is made to ensure
that the type of the argument is compatible with ``str``.

Type hints are maintained through transforms regardless of resultant schema.
For example a transform that has a return type hint of ``int`` defines
the schema as :py:const:`~streamsx.topology.schema.CommonSchema.Python`,
but the type hint is retained even though the schema is generic. Thus an
error is raised at topology declaration time if a downstream transformation
uses a callable with a type hint that is incompatible with being passed an ``int``.

How type hints are used is specific to each transformation, such as
:py:meth:`~Topology.source`, :py:meth:`~Stream.map`, :py:meth:`~Stream.filter` etc.

Type checking can be disabled by setting the topology's ``type_checking`` attribute to false.

When a callable is a lambda or defined inline (defined in the main Python script,
a notebook or an interactive session) then a serialized copy of its definition becomes part of the
topology. The supported types of captured globals for these callables is limited to
avoid increasing the size of the application and serialization failures due non-serializable
objects directly or indirectly referenced from captured globals. The supported types of captured globals
are constants (``int``, ``str``, ``float``, ``bool``, ``bytes``, ``complex``), modules, module attributes (e.g. classes, functions and variables
defined in a module), inline classes and functions. If a lambda or inline callable causes an exception due to unsupported global
capture then moving it to its own module is a solution.

Due to `Python bug 36697 <https://bugs.python.org/issue36697>`_ a lambda or inline callable can
incorrect capture a global variable. For example an inline class using a attribute of ``self.model``
will incorrectly capture the global ``model`` even if the global variable ``model`` is never used within the class.
To workaround this bug use attribute or variable names that do not shadow global variables
(e.g. ``self._model``).

Due to `issue 2336 <https://github.com/IBMStreams/streamsx.topology/issues/2336>`_ an inline class using ``super()`` will cause an ``AttributeError`` at runtime. Workaround is to call the super class's method directly, for example replace this code::

    class A(X):
        def __init__(self):
            super().__init__()

with::

    class A(X):
        def __init__(self):
            X.__init__(self)

or move the class to a module.
   

Stateful operations
===================

Use of a class instance allows the operation to be stateful by maintaining state in instance
attributes across invocations.

.. note::
    For support with consistent region or checkpointing instances should ensure that the object's state can be pickled. See https://docs.python.org/3.5/library/pickle.html#handling-stateful-objects

Initialization and shutdown
===========================

Execution of a class instance effectively run in a context manager so that an instance's ``__enter__``
method is called when the processing element containing the instance  is initialized
and its ``__exit__`` method called when the processing element is stopped. To take advantage of this
the class must define both ``__enter__`` and ``__exit__`` methods.

.. note::
    Since an instance of a class is passed to methods such as
    :py:meth:`~Stream.map` ``__init__`` is only called when the topology is `declared`, not at runtime.
    Initialization at runtime, such as opening connections, occurs through the ``__enter__`` method.

Example of using ``__enter__`` to create custom metrics::

    import streamsx.ec as ec

    class Sentiment(object):
        def __init__(self):
            pass

        def __enter__(self):
            self.positive_metric = ec.CustomMetric(self, "positiveSentiment")
            self.negative_metric = ec.CustomMetric(self, "negativeSentiment")

        def __exit__(self, exc_type, exc_value, traceback):
            pass

        def __call__(self):
            pass

When an instance defines a valid ``__exit__`` method then it will be called with an exception when:

 * the instance raises an exception during processing of a tuple
 * a data conversion exception is raised converting a value to an structured schema tuple or attribute

If ``__exit__`` returns a true value then the exception is suppressed and processing continues, otherwise the enclosing processing element will be terminated.

.. note::
    The ``__exit__`` method requires four parameters, whereas the last three parameters are set when exception is raised only:: 

        def __exit__(self, exc_type, exc_value, traceback):
            if exc_type:
                print(str(exc_type.__name__))
                ...

Tuple semantics
===============

Python objects on a stream may be passed by reference between callables (e.g. the value returned by a map callable may be passed by reference to a following filter callable). This can only occur when the functions are executing in the same PE (process). If an object is not passed by reference a deep-copy is passed. Streams that cross PE (process) boundaries  are always passed by deep-copy.

Thus if a stream is consumed by two map and one filter callables in the same PE they may receive the same object reference that was sent by the upstream callable. If one (or more) callable modifies the passed in reference those changes may be seen by the upstream callable or the other callables. The order of execution of the downstream callables is not defined. One can prevent such potential non-deterministic behavior by one or more of these techniques:

* Passing immutable objects
* Not retaining a reference to an object that will be submitted on a stream
* Not modifying input tuples in a callable
* Using copy/deepcopy when returning a value that will be submitted to a stream.

Applications cannot rely on pass-by reference,  it is a performance optimization that can be made in some situations when stream connections are within a PE.

Application log and trace
=========================

IBM Streams provides application trace and log services which are
accesible through standard Python loggers from the `logging` module.

See :ref:`streams_app_log_trc`.

SPL operators
=============

In addition an application declared by `Topology` can include stream processing defined by SPL primitive or
composite operators. This allows reuse of adapters and analytics provided by IBM Streams,
open source and third-party SPL toolkits.

See :py:mod:`streamsx.spl.op`

***************
Module contents
***************

"""

__all__ = [ 'Routing', 'SubscribeConnection', 'Topology', 'Stream', 'View', 'PendingStream', 'Window', 'Sink' ]

import streamsx._streams._version
__version__ = streamsx._streams._version.__version__

import copy
import collections
import random
import streamsx._streams._placement as _placement
import streamsx._streams._hints
import streamsx.spl.op
import streamsx.spl.types
import streamsx.spl.spl
import streamsx.topology.graph
import streamsx.topology.schema
import streamsx.topology.functions
import streamsx.topology.runtime
import dill
import types
import base64
import json
import threading
import queue
import sys
import os
import time
import inspect
import logging
import datetime
import pkg_resources
import warnings
from enum import Enum

logger = logging.getLogger('streamsx.topology')

def _source_info():
    """
    Get information from the user's code (two frames up)
    to leave breadcrumbs for file, line, class and function.
    """
    ofi = inspect.getouterframes(inspect.currentframe())[2]
    try:
        calling_class = ofi[0].f_locals['self'].__class__
    except KeyError:
        calling_class = None
    # Tuple of file,line,calling_class,function_name
    return ofi[1], ofi[2], calling_class, ofi[3]
 
class _SourceLocation(object):
    """
    Saved source info to eventually create an SPL
    annotation with the info in JSON form.
    This object's JSON is put into the JSON as "sourcelocation"
    """
    def __init__(self, source_info, method=None):
        self.source_info = source_info
        self.method = method

    def spl_json(self):
        sl = {}
        sl['file'] = self.source_info[0]
        sl['line'] = self.source_info[1]
        if self.source_info[2] is not None:
            sl['class'] = self.source_info[2].__name__
        sl['method'] = self.source_info[3]
        if self.method:
            sl['api.method'] = self.method
        return sl

"""
Determine whether a callable has state that needs to be saved during
checkpointing.  
"""
def _determine_statefulness(_callable):
    stateful = not inspect.isroutine(_callable)
    return stateful

class Routing(Enum):
    """
    Defines how tuples are routed to channels in a
    parallel region.

    A parallel region is started by :py:meth:`~Stream.parallel`
    and ended with :py:meth:`~Stream.end_parallel` or :py:meth:`~Stream.for_each`.
    """
    BROADCAST=0
    """
    Tuples are routed to every channel in the parallel region.
    """
    ROUND_ROBIN=1
    """
    Tuples are routed to maintain an even distribution of tuples to the channels.

    Each tuple is only sent to a single channel.
    """
    KEY_PARTITIONED=2
    """
    Tuples are routed based upon specified partitioning keys.
    The splitter routes tuples that have the same values for these keys (list of attributes) to the same parallel channel.
    The keys must exist in the tuple type that is specified for the input stream.
    Requires a structured stream :py:class:`StreamSchema` or named tuple as input stream.
    
    Each tuple is only sent to a single channel.
    """
    HASH_PARTITIONED=3
    """
    Tuples are routed based upon a hash value so that tuples with the same hash
    and thus same value are always routed to the same channel. When a hash function is
    specified it is passed the tuple and the return value is the hash. When no hash
    function is specified then `hash(tuple)` is used.

    Each tuple is only sent to a single channel.

    .. warning:: A consistent hash function is required to guarantee that a tuple
        with the same value is always routed to the same channel. `hash()` is not
        consistent in that for types str, bytes and datetime objects are “salted”
        with an unpredictable random value (Python 3.5). Thus if the processing element is
        restarted channel routing for a hash based upon a str, bytes or datetime will change.
        In addition code executing in the channels can see a different
        hash value to other channels and the execution that routed the tuple due to
        being in different processing elements.
    """

class SubscribeConnection(Enum):
    """Connection mode between a subscriber and matching publishers.

    .. versionadded:: 1.9
    .. seealso:: :py:meth:`~Topology.subscribe`
    """
    Direct = 0
    """Direct connection between a subscriber and and matching publishers.
    
    When connected directly a slow subscriber will cause back-pressure
    against the publishers, forcing them to slow tuple processing to
    the slowest publisher.
    """

    Buffered = 1
    """Buffered connection between a subscriber and and matching publishers.
   
    With a buffered connection tuples from publishers are placed in
    a single queue owned by the subscriber. This allows a slower
    subscriber to handle brief spikes in tuples from publishers.

    A subscriber can fully isolate itself from matching publishers
    by adding a :py:class:`CongestionPolicy` that drops tuples
    when the queue is full. In this case when the subscriber is
    not able to keep up with the tuple rate from all matching subscribers
    it will have a minimal effect on matching publishers.
    """

    def spl_json(self):
        return streamsx.spl.op.Expression.expression('com.ibm.streamsx.topology.topic::' + self.name).spl_json()

class Topology(object):
    """The Topology class is used to define data sources, and is passed as a parameter when submitting an application. Topology keeps track of all sources, sinks, and transformations within your application.

    Submission of a Topology results in a Streams application that has
    the name `namespace::name`.

    Args:
        name(str): Name of the topology. Defaults to a name dervied from the calling evironment if it can be determined, otherwise a random name.
        namespace(str): Namespace of the topology. Defaults to a name dervied from the calling evironment if it can be determined, otherwise a random name.

    Attributes:
        include_packages(set[str]): Python package names to be included in the built application. Any package in this list is copied into the bundle and made available at runtime to the Python callables used in the application. By default a ``Topology`` will automatically discover which packages and modules are required to be copied, this field may be used to add additional packages that were not automatically discovered. See also :py:meth:`~Topology.add_pip_package`. Package names in `include_packages` take precedence over package names in `exclude_packages`.

        exclude_packages(set[str]): Python top-level package names to be excluded from the built application. Excluding a top-level packages excludes all sub-modules at any level in the package, e.g. `sound` excludes `sound.effects.echo`. Only the top-level package can be defined, e.g. `sound` rather than `sound.filters`. Behavior when adding a module within a package is undefined. When compiling the application using Anaconda this set is pre-loaded with Python packages from the Anaconda pre-loaded set.

        type_checking(bool): Set to false to disable type checking, defaults to ``True``.

        name_to_runtime_id: Optional callable that returns a runtime identifier for a name. Used to override the default mapping of a name into a runtime identifer. It will be called with `name` and returns a valid SPL identifier or ``None``. If ``None`` is returned then the default mapping for `name` is used. Defaults to ``None`` indicating the default mapping is used. See :py:meth:`Stream.runtime_id <Stream.runtime_id>`.

    All declared streams in a `Topology` are available through their name
    using ``topology[name]``. The stream's name is defined by :py:meth:`Stream.name` and will differ from the name parameter passed when creating the stream if the application uses duplicate names.

    .. versionchanged:: 1.11 Declared streams available through ``topology[name]``.
    """  

    def __init__(self, name=None, namespace=None, files=None):
        if name is None or namespace is None:
            # Take the name of the calling function
            # If it starts with __ and is in a class then use the class name
            # Take the namespace from the class's module if executing from
            # a class otherwise use the name
            si = _source_info()
            if name is None:
                name = si[3]
                if name.startswith('__'):
                    if si[2] is not None:
                        name = si[2].__name__
                    
            if namespace is None:
                if si[2] is not None:
                    namespace = si[2].__module__
                elif si[0] is not None:
                    namespace = os.path.splitext(os.path.basename(si[0]))[0]
                    if namespace.startswith('<ipython-input'):
                        import streamsx.topology.graph
                        namespace = streamsx.topology.graph._get_project_name()
                        if not namespace:
                            namespace = 'notebook'
        
        if sys.version_info.major == 3:
          self.opnamespace = "com.ibm.streamsx.topology.functional.python"
        else:
          raise ValueError("Python version not supported.")
        self._streams = dict()
        self.include_packages = set() 
        self.exclude_packages = set() 
        self._pip_packages = list() 
        self._files = dict()
        if "Anaconda" in sys.version or 'PROJECT_ID' in os.environ or 'DSX_PROJECT_ID' in os.environ or os.path.exists(os.path.join(sys.prefix, 'conda-meta')):
            import streamsx.topology.condapkgs
            self.exclude_packages.update(streamsx.topology.condapkgs._CONDA_PACKAGES)
        import streamsx.topology._deppkgs
        if 'PROJECT_ID' in os.environ or 'DSX_PROJECT_ID' in os.environ:
            self.exclude_packages.update(streamsx.topology._deppkgs._ICP4D_NB_PACKAGES)
        self.exclude_packages.update(streamsx.topology._deppkgs._DEP_PACKAGES)
        
        self.graph = streamsx.topology.graph.SPLGraph(self, name, namespace)
        self._submission_parameters = dict()
        self._checkpoint_period = None
        self._consistent_region_config = None
        self._has_jcp = False
        self.type_checking = True
        self.name_to_runtime_id = None

    @property
    def name(self):
        """
        Name of the topology.

        Returns:
            str: Name of the topology.
        """
        return self.graph.name
    @property
    def namespace(self):
        """
        Namespace of the topology.

        Returns:
            str:Namespace of the topology.
        """
        return self.graph.namespace

    def __getitem__(self, name):
        return self._streams[name]

    def _set_service_annotation(self, value):
        self.graph._set_service_annotation(value)

    @property
    def streams(self):
        """
        Dict of all streams in the topology.

        Key is the name of the stream, value is the corresponding :py:obj:`Stream` instance.

        The returned value is a shallow copy of current streams
        in this topology. This allows callers to iterate over the copy
        and perform operators that would add streams.

        .. note:: Includes all streams created by composites and any internal streams created by topology.
 
        .. versionadded:: 1.14
        """
        return self._streams.copy()

    def source(self, func, name=None):
        """
        Declare a source stream that introduces tuples into the application.

        Typically used to create a stream of tuple from an external source,
        such as a sensor or reading from an external system.

        Tuples are obtained from an iterator obtained from the passed iterable
        or callable that returns an iterable.

        Each tuple that is not None from the iterator is present on the returned stream.

        Each tuple is a Python object and must be picklable to allow execution of the application
        to be distributed across available resources in the Streams instance.

        If the iterator's ``__iter__`` or ``__next__`` block then shutdown,
        checkpointing or consistent region processing may be delayed.
        Having ``__next__`` return ``None`` (no available tuples) or tuples
        to submit will allow such processing to proceed.

        A shutdown ``threading.Event`` is available through
        :py:func:`streamsx.ec.shutdown` which becomes set when a shutdown
        of the processing element has been requested. This event my be waited
        on to perform a sleep that will terminate upon shutdown.

        Args:
            func(callable): An iterable or a zero-argument callable that returns an iterable of tuples.
            name(str): Name of the stream, defaults to a generated name.

        Exceptions raised by ``func`` or its iterator will cause
        its processing element will terminate. 

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method.

        Suppressing an exception raised by ``func.__iter__`` causes the
        source to be empty, no tuples are submitted to the stream.

        Suppressing an exception raised by ``__next__`` on the iterator
        results in no tuples being submitted for that call to ``__next__``.
        Processing continues with calls to ``__next__`` to fetch subsequent tuples.

        Returns:
            Stream: A stream whose tuples are the result of the iterable obtained from `func`.

        .. rubric:: Type hints

        Type hints on `func` define the schema of the returned stream,
        defaulting to :py:const:`~streamsx.topology.schema.CommonSchema.Python`
        if no type hints are present.

        For example ``s_sensor`` has a type hint that
        defines it as an iterable of ``SensorReading`` instances (typed named tuples).
        Thus `readings` has a structured schema matching ``SensorReading`` ::

            def s_sensor() -> typing.Iterable[SensorReading] :
                ...

            topo = Topology()
            readings = topo.source(s_sensor)

        .. rubric:: Simple examples

        Finite constant source stream containing two tuples
        ``Hello`` and ``World``::

            topo = Topology()
            hw = topo.source(['Hello', 'World'])

        Use of builtin `range` to produce a finite source stream
        containing 100 `int` tuples from 0 to 99::

            topo = Topology()
            hw = topo.source(range(100))

        Use of `itertools.count` to produce an infinite stream of `int` tuples::

            import itertools
            topo = Topology()
            hw = topo.source(lambda : itertools.count())

        Use of `itertools` to produce an infinite stream of tuples
        with a constant value and a sequence number::

            import itertools
            topo = Topology()
            hw = topo.source(lambda : zip(itertools.repeat(), itertools.count()))

        .. rubric:: External system examples

        Typically sources pull data in from external systems, such as files,
        REST apis, databases, message systems etc. Such a source will typically
        be implemented as class that when called returns an iterable.

        To allow checkpointing of state standard methods ``__enter__``
        and  ``__exit__`` are implemented to allow creation of runtime
        objects that cannot be persisted, for example a file handle.

        At checkpoint time state is preserved through standard pickling
        using ``__getstate__`` and (optionally) ``__setstate__``.

        Stateless source that polls a REST API every ten seconds to
        get a JSON object (`dict`) with current time details::

        
            import requests
            import time

            class RestJsonReader(object):
                def __init__(self, url, period):
                    self.url = url
                    self.period = period
                    self.session = None

                def __enter__(self):
                    self.session = requests.Session()
                    self.session.headers.update({'Accept': 'application/json'})

                def __exit__(self, exc_type, exc_value, traceback):
                    if self.session:
                        self.session.close()
                        self.session = None

                def __call__(self):
                    return self

                def __iter__(self):
                    return self

                def __next__(self):
                    time.sleep(self.period)
                    return self.session.get(self.url).json()

                def __getstate__(self):
                    # Remove the session from the persisted state
                    return {'url':self.url, 'period':self.period}

            def main():
                utc_now = 'http://worldclockapi.com/api/json/utc/now'
                topo = Topology()
                times = topo.source(RestJsonReader(10, utc_now))


        .. warning::
            Source functions that use generators are not supported
            when checkpointing or within a consistent region. This
            is because generators cannot be pickled (even when using `dill`).

        .. versionchanged:: 1.14 
            Type hints are used to define the returned stream schema.
        """
        sl = _SourceLocation(_source_info(), "source")
        import streamsx.topology.composite
        if isinstance(func, streamsx.topology.composite.Source):
            return func._add(self, name)

        _name = self.graph._requested_name(name, action='source', func=func)
        hints = streamsx._streams._hints.schema_iterable(func, self)

        if inspect.isroutine(func) or callable(func):
            pass
        else:
            func = streamsx.topology.runtime._IterableInstance(func)

        schema = hints.schema if hints else None

        # source is always stateful
        op = self.graph.addOperator(self.opnamespace+"::Source", func, name=_name, sl=sl, nargs=0)
        op._layout(kind='Source', name=op.runtime_id, orig_name=name)
        oport = op.addOutputPort(schema=schema, name=_name)
        return Stream(self, oport)._make_placeable()._add_hints(hints)

    def subscribe(self, topic, schema=streamsx.topology.schema.CommonSchema.Python, name=None, connect=None, buffer_capacity=None, buffer_full_policy=None):
        """
        Subscribe to a topic published by other Streams applications.
        A Streams application may publish a stream to allow other
        Streams applications to subscribe to it. A subscriber matches a
        publisher if the topic and schema match.

        By default a stream is subscribed as :py:const:`~streamsx.topology.schema.CommonSchema.Python` objects
        which connects to streams published to topic by Python Streams applications.

        Structured schemas are subscribed to using an instance of
        :py:class:`StreamSchema`.  A Streams application publishing
        structured schema streams may have been implemented in any
        programming language supported by Streams.

        JSON streams are subscribed to using schema :py:const:`~streamsx.topology.schema.CommonSchema.Json`.
        Each tuple on the returned stream will be a Python dictionary
        object created by ``json.loads(tuple)``.
        A Streams application publishing JSON streams may have been implemented in any programming language
        supported by Streams.
       
        String streams are subscribed to using schema :py:const:`~streamsx.topology.schema.CommonSchema.String`.
        Each tuple on the returned stream will be a Python string object.
        A Streams application publishing string streams may have been implemented in any programming language
        supported by Streams.

        Subscribers can ensure they do not slow down matching publishers
        by using a buffered connection with a buffer full policy
        that drops tuples.

        Args:
            topic(str): Topic to subscribe to.
            schema(~streamsx.topology.schema.StreamSchema): schema to subscribe to.
            name(str): Name of the subscribed stream, defaults to a generated name.
            connect(SubscribeConnection): How subscriber will be connected to matching publishers. Defaults to :py:const:`~SubscribeConnection.Direct` connection.
            buffer_capacity(int): Buffer capacity in tuples when `connect` is set to :py:const:`~SubscribeConnection.Buffered`. Defaults to 1000 when `connect` is `Buffered`. Ignored when `connect` is `None` or `Direct`.
            buffer_full_policy(~streamsx.types.CongestionPolicy): Policy when a pulished tuple arrives and the subscriber's buffer is full. Defaults to `Wait` when `connect` is `Buffered`. Ignored when `connect` is `None` or `Direct`.

        Returns:
            Stream:  A stream whose tuples have been published to the topic by other Streams applications.

        .. versionchanged:: 1.9 `connect`, `buffer_capacity` and `buffer_full_policy` parameters added.

        .. seealso:`SubscribeConnection`
        """
        schema = streamsx.topology.schema._normalize(schema)
        _name = self.graph._requested_name(name, 'subscribe')
        sl = _SourceLocation(_source_info(), "subscribe")
        # subscribe is never stateful
        op = self.graph.addOperator(kind="com.ibm.streamsx.topology.topic::Subscribe", sl=sl, name=_name, stateful=False)
        oport = op.addOutputPort(schema=schema, name=_name)
        params = {'topic': topic, 'streamType': schema}
        if connect is not None and connect != SubscribeConnection.Direct:
            params['connect'] = connect
            if buffer_capacity:
                params['bufferCapacity'] = int(buffer_capacity)
            if buffer_full_policy:
                params['bufferFullPolicy'] = buffer_full_policy
            
        op.setParameters(params)
        op._layout_group('Subscribe', name if name else _name)
        return Stream(self, oport)._make_placeable()

    def add_file_dependency(self, path, location):
        """
        Add a file or directory dependency into an Streams application bundle.

        Ensures that the file or directory at `path` on the local system
        will be available at runtime.

        The file will be copied and made available relative to the
        application directory. Location determines where the file
        is relative to the application directory. Two values for
        location are supported `etc` and `opt`.
        The runtime path relative to application directory is returned.

        The copy is made during the submit call thus the contents of
        the file or directory must remain availble until submit returns.

        For example calling
        ``add_file_dependency('/tmp/conf.properties', 'etc')``
        will result in contents of the local file `conf.properties`
        being available at runtime at the path `application directory`/etc/conf.properties. This call returns ``etc/conf.properties``.

        Python callables can determine the application directory at
        runtime with :py:func:`~streamsx.ec.get_application_directory`.
        For example the path above at runtime is
        ``os.path.join(streamsx.ec.get_application_directory(), 'etc', 'conf.properties')``
        
        Args:
            path(str):  Path of the file on the local system.
            location(str): Location of the file in the bundle relative to the application directory.

        Returns:
            str: Path relative to application directory that can be joined at runtime with ``get_application_directory``.

        .. versionadded:: 1.7
        """
        if location not in {'etc', 'opt'}:
            raise ValueError(location)

        if not os.path.isfile(path) and not os.path.isdir(path):
            raise ValueError(path)

        path = os.path.abspath(path)

        if location not in self._files:
             self._files[location] = [path]
        else:
             self._files[location].append(path)
        return location + '/' + os.path.basename(path)

    def add_pip_package(self, requirement, name=None):
        """
        Add a Python package dependency for this topology.

        If the package defined by the requirement specifier
        is not pre-installed on the build system then the
        package is installed using `pip` and becomes part
        of the Streams application bundle (`sab` file).
        The package is expected to be available from `pypi.org`.


        If the package is already installed on the build system
        then it is not added into the `sab` file.
        The assumption is that the runtime hosts for a Streams
        instance have the same Python packages installed as the
        build machines. This is always true for IBM Cloud
        Pak for Data and the Streaming Analytics service on IBM Cloud.

        The project name extracted from the requirement
        specifier is added to :py:attr:`~exclude_packages`
        to avoid the package being added by the dependency
        resolver. Thus the package should be added before
        it is used in any stream transformation.

        When an application is run with trace level ``info``
        the available Python packages on the running system
        are listed to application trace. This includes
        any packages added by this method.

        Example::

            topo = Topology()
            # Add dependency on pint package
            # and astral at version 0.8.1
            topo.add_pip_package('pint')
            topo.add_pip_package('astral==0.8.1')

        Example for packages not provided on pypi.org::

            topo = Topology()
            # Add dependency on package using whl file
            topo.add_pip_package(requirement='https://github.com/myrepo/raw/mydir/mypkg-1.0-py3-none-any.whl', name='mypkg')
        
        Args:
            requirement(str): Package requirements specifier.
            name(str): Name added to :py:attr:`~exclude_packages`. Set this argument when adding URLs only.

        .. warning::
            Only supported when using the build service with
            a Streams instance in Cloud Pak for Data
            or Streaming Analytics service on IBM Cloud.

        .. note::
            Installing packages through `pip` is preferred to
            the automatic dependency checking performed on local
            modules. This is because `pip` will perform a full
            install of the package including any dependent packages
            and additional files, such as shared libraries, that
            might be missed by dependency discovery.

        .. versionadded:: 1.9
        """
        self._pip_packages.append(str(requirement))
        if name is None:
            pr = pkg_resources.Requirement.parse(requirement)
            self.exclude_packages.add(pr.project_name)
        else:
            self.exclude_packages.add(name)

    def create_submission_parameter(self, name, default=None, type_=None):
        """ Create a submission parameter.

        A submission parameter is a handle for a value that
        is not defined until topology submission time.  Submission
        parameters enable the creation of reusable topology bundles.
 
        A submission parameter has a `name`. The name must be unique
        within the topology.

        The returned parameter is a `callable`.
        Prior to submitting the topology, while constructing the topology,
        invoking it returns ``None``.
 
        After the topology is submitted, invoking the parameter
        within the executing topology returns the actual submission time value
        (or the default value if it was not set at submission time).

        Submission parameters may be used within functional logic. e.g.::

            threshold = topology.create_submission_parameter('threshold', 100);

            # s is some stream of integers
            s = ...
            s = s.filter(lambda v : v > threshold())

        Submission parameters may be used to specify the degree of parallelism. e.g.::

            stv_channels = topo.create_submission_parameter('num_channels', type_=int)
        
            s = topo.source(range(67)).set_parallel(stv_channels)
            s = s.filter(lambda v : v % stv_channels() == 0)
            s = s.end_parallel()
 
            jc = JobConfig()
            jc.submission_parameters['num_channels'] = 3
            jc.add(cfg)

        .. note::
            The parameter (value returned from this method) is only
            supported within a lambda expression or a callable
            that is not a function.

        The default type of a submission parameter's value is a `str`.
        When a `default` is specified
        the type of the value matches the type of the default.

        If `default` is not set, then the type can be set with `type_`.

        The types supported are ``str``, ``int``, ``float`` and ``bool``.

        Topology submission behavior when a submission parameter 
        lacking a default value is created and a value is not provided at
        submission time is defined by the underlying topology execution runtime.

           * Submission fails for contexts ``DISTRIBUTED``, ``STANDALONE``, and ``STREAMING_ANALYTICS_SERVICE``.

        Args:
            name(str): Name for submission parameter.
            default: Default parameter when submission parameter is not set.
            type_: Type of parameter value when default is not set. Supported values are `str`, `int`, `float` and `bool`.

        .. versionadded:: 1.9
        .. seealso:: :py:meth:`streamsx.ec.get_submission_time_value`
        """
        
        if name in self._submission_parameters:
            raise ValueError("Submission parameter {} already defined.".format(name))
        sp = streamsx.topology.runtime._SubmissionParam(name, default, type_)
        self._submission_parameters[name] = sp
        return sp

    @property
    def checkpoint_period(self):
        """Enable checkpointing for the topology, and define the checkpoint
        period.

        When checkpointing is enabled, the state of all stateful operators
        is saved periodically.  If the operator restarts, its state is
        restored from the most recent checkpoint.

        The checkpoint period is the frequency at which checkpoints will
        be taken.  It can either be a :py:class:`~datetime.timedelta` value
        or a floating point value in seconds.  It must be at 0.001
        seconds or greater.

        A stateful operator is an operator whose callable is an instance of a
        Python callable class.

        Examples::

            # Create a topology that will checkpoint every thirty seconds
            topo = Topology()
            topo.checkpoint_period = 30.0

        ::

            # Create a topology that will checkpoint every two minutes
            topo = Topology()
            topo.checkpoint_period = datetime.timedelta(minutes=2)

        .. versionadded:: 1.11
        """
        return self._checkpoint_period

    @checkpoint_period.setter
    def checkpoint_period(self, period):
        if (isinstance(period, datetime.timedelta)):
            self._checkpoint_period = period.total_seconds()
        else:
            self._checkpoint_period = float (period)

        # checkpoint period must be greater or equal to 0.001
        if self._checkpoint_period < 0.001:
            raise ValueError("checkpoint_period must be 0.001 or greater")

    def _prepare(self):
        """Prepare object prior to SPL generation."""
        self._generate_requirements()

    def _generate_requirements(self):
        """Generate the info to create requirements.txt in the toookit."""
        if not self._pip_packages:
            return

        reqs = ''
        for req in self._pip_packages:
                reqs += "{}\n".format(req)
        reqs_include = {
            'contents': reqs,
            'target':'opt/python/streams',
            'name': 'requirements.txt'}

        if 'opt' not in self._files:
             self._files['opt'] = [reqs_include]
        else:
             self._files['opt'].append(reqs_include)

    def _add_job_control_plane(self):
        """
        Add a JobControlPlane operator to the topology, if one has not already
        been added.  If a JobControlPlane operator has already been added,
        this has no effect.
        """
        if not self._has_jcp:
            jcp = self.graph.addOperator(kind="spl.control::JobControlPlane", name="JobControlPlane")
            jcp.viewable = False
            self._has_jcp = True


class Stream(_placement._Placement, object):
    """
    The Stream class is the primary abstraction within a streaming application. It represents a potentially infinite 
    series of tuples which can be operated upon to produce another stream, as in the case of :py:meth:`map`, or
    terminate a stream, as in the case of :py:meth:`for_each`.

    .. versionchanged::1.14 
        Type hints are used to define stream schemas and verify transformations
        at declaration time.
    """
    def __init__(self, topology, oport, other=None):
        self.topology = topology
        self.oport = oport
        self._placeable = False
        self._alias = None
        self._hints = None
        topology._streams[self.oport.name] = self
        self._json_stream = None
        if other:
            self._add_hints(other._hints)

    def _op(self):
        if not self._placeable:
            raise TypeError()
        return self.oport.operator

    def _add_hints(self, hints):
        self._hints = hints
        return self

    @property
    def name(self):
        """
        Unique name of the stream.

        When declaring a stream a `name` parameter can be provided.
        If the supplied name is unique within its topology then
        it will be used as-is, otherwise a variant will be provided
        that is unique within the topology.

        If a `name` parameter was not provided when declaring a stream
        then the stream is assigned a unique generated name.

        Returns:
            str: Name of the stream.

        .. seealso:: :py:meth:`aliased_as`

        .. warning::
            If the name is not a valid SPL identifier or longer than
            80 characters then the name will be
            converted to a valid SPL identifier at compile and runtime.
            This identifier will be the name used in the REST api and log/trace.

            Visualizations of the runtime graph uses `name` rather
            than the converted identifier.

            A valid SPL identifier consists only of 
            characters ``A-Z``, ``a-z``, ``0-9``, ``_`` and
            must not start with a number or be an SPL keyword.

            See :py:meth:`runtime_id <runtime_id>`.
        """
        return self._alias if self._alias else self.oport.name

    @property
    def runtime_id(self):
        """
        Return runtime identifier.

        If :py:meth:`name <name>` is not a valid SPL identifier then the
        runtime identifier will be valid SPL identifier that represents `name`.
        Otherwise `name` is returned.

        The runtime identifier is how the underlying SPL operator
        or output port is named in the REST api and trace/log files.

        If a topology unique name is supplied when creating a stream then runtime
        identifier is fixed regardless of other changes in the topology.

        The algorithm to determine the runtime name (for clients that
        cannot call this method, for example, remote REST clients gathering
        metrics) is as follows.

        If the length of :py:meth:`name <name>` is less than or equal
        to 80 and ``name`` is an SPL identifier then ``name`` is used.
        An SPL identifier consists only of the characters ``A-Z``, ``a-z``
        ``0-9`` and ``_``, must not start with ``0-9`` and must not be
        an SPL keyword.

        Otherwise the identifier has the form ``prefix_suffix``.

        ``prefix`` is the kind of the SPL operator stripped of
        its namespace and ``::``.  For all functional methods
        the operator kind is the method name with the first
        character upper-cased.

        For example, ``Filter`` for :py:meth:`filter`, ``Beacon`` for
        ``spl::utility::Beacon``.

        ``suffix`` is a hashed version of name, an MD5 digest
        ``d`` is calculated from the UTf-8 encoding of ``name``.
        ``d`` is shortened by having its first eight bytes xor folded
        with its last eight bytes. ``d`` is then base64 encoded
        to produce a string. Padding ``=`` and ``+`` and ``/`` characters
        are removed from the string.

        For example, ``s.filter(lambda x : True, name='你好')``
        results in a runtime identifier of ``Filter_oGwCfhWRg4``.

        The default mapping can be overridden by setting :py:attr:`Topology.name_to_runtime_id` to a callable that returns a valid identifier for its single argument. The returned identifier should be unique with the topology. For example usinig a pre-populated `dict` as the mapper::

            topo = Topology()
            names = {'你好', 'Buses', '培养':'Trains'}
            topo.name_to_runtime_id = names.get

            buses = toopo.source(..., name='你好')
            trains = topo.source(..., name='培养'}

            // buses.runtime_id will be Buses
            // trains.runtime_id will be Trains


        Returns:
            str: Runtime identifier of the stream.

        .. versionadded:: 1.14
        """
        return self.oport.runtime_id
     
    def aliased_as(self, name):
        """
        Create an alias of this stream.

        Returns an alias of this stream with name `name`.
        When invocation of an SPL operator requires an
        :py:class:`~streamsx.spl.op.Expression` against
        an input port this can be used to ensure expression
        matches the input port alias regardless of the name
        of the actual stream.

        Example use where the filter expression for a ``Filter`` SPL operator
        uses ``IN`` to access input tuple attribute ``seq``::

            s = ...
            s = s.aliased_as('IN')

            params =  {'filter': op.Expression.expression('IN.seq % 4ul == 0ul')}
            f = op.Map('spl.relational::Filter', stream, params = params)

        Args:
            name(str): Name for returned stream.

        Returns:
            Stream: Alias of this stream with ``name`` equal to `name`.
        
        .. versionadded:: 1.9
        """
        stream = copy.copy(self)
        stream._alias = name
        return stream

    def for_each(self, func, name=None, process_punct=None):
        """
        Sends information as a stream to an external system.

        The transformation defined by `func` is a callable
        or a composite transformation.

        .. rubric:: Callable transformation

        If `func` is callable then for each tuple `t` on this
        stream ``func(t)`` is called.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no further processing occurs for the
        input tuple that caused the exception.

        Example with class handling punctuations in the Sink operator::

            class FEClass(object):
                def __call__(self, t):
                    return None

                def on_punct(self):
                    print ('window punctuation marker received')
                    ...

            ...
            s.for_each(FEClass(), name='SinkHandlingPunctuations', process_punct=True)

        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.



        .. rubric:: Composite transformation

        A composite transformation is an instance of :py:class:`~streamsx.topology.composite.ForEach`. Composites allow the application developer to use
        the standard functional style of the topology api while allowing
        allowing expansion of a `for_each` transform to multiple basic
        transformations.
        
        Args:
            func: A callable that takes a single parameter for the tuple and returns None.
            name(str): Name of the stream, defaults to a generated name.
            process_punct(bool): Specifies if ``on_punct`` on callable ``func`` is called when window punctuation markers are received.

        Returns:
            streamsx.topology.topology.Sink: Stream termination.

        .. rubric:: Type hints

        The argument type hint on `func` is used (if present) to verify
        at topology declaration time that it is compatible with the
        type of tuples on this stream.

        .. versionchanged:: 1.7
            Now returns a :py:class:`Sink` instance.
        .. versionchanged:: 1.14 
            Support for type hints and composite transformations.
        .. versionchanged:: 1.16 
            New parameter process_punct to support handling of window punctuation markers in callable.
        """
        import streamsx.topology.composite
        if isinstance(func, streamsx.topology.composite.ForEach):
            return func._add(self, name)

        streamsx._streams._hints.check_for_each(func, self)
        sl = _SourceLocation(_source_info(), 'for_each')
        _name = self.topology.graph._requested_name(name, action='for_each', func=func)
        stateful = _determine_statefulness(func)
        params = {}
        if process_punct is not None:
           if process_punct:
               params = {'processPunctuations': True} # sets parameter of ForEach operator
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::ForEach", func, name=_name, sl=sl, stateful=stateful, params=params)
        op.addInputPort(outputPort=self.oport)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        op._layout(kind='ForEach', name=op.runtime_id, orig_name=name)
        return Sink(op)

    def punctor(self, func, before=True, replace=False, name=None):
        """
        Adds window punctuation to this stream using the supplied callable `func` as condition that determines when a window punctuation is to be generated.

        For each stream tuple `t` on the stream ``func(t)`` is called, if the return evaluates to ``True`` the
        window punctuation will be generated and the tuple is forwarded, otherwise the tuple is just forwarded.

        .. note::
             Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

             The :py:meth:`punctor` punctuation mode is "generating" and inserts punctuation into the output stream according to custom logic. Incoming window punctuation is not forwarded.
        
        Args:
            func: Punctor callable that takes a single parameter for the stream tuple.
            before(bool): If the value is `True`, the punctuation is generated before the output tuple; otherwise it is generated after the output tuple. If the parameter ``replace`` is set to ``True`` then the parameter ``before`` is ignored.
            replace(bool): If the value is `True`, then in case ``func(t)`` returns ``True`` the window punctuation will be generated and the tuple is discarded (not forwarded). The parameter ``before`` is ignored in this case.
            name(str): Name of the stream, defaults to a generated name.

        If invoking ``func`` for a stream tuple raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuple is submitted to the output
        stream corresponding to the input tuple that caused the exception.

        Example with adding punctuation after each tuple::

            topo = Topology()
            s = topo.source([1,2,3,4])
            s = s.punctor(lambda x: True, before=False)

        Example with sending punctuation before a tuple::

            topo = Topology()
            s = topo.source([1,2,3,4])
            s = s.punctor(lambda t : 2 < t)

        Returns:
            Stream: A Stream containing tuples with generated punctuation. The schema of the returned stream is the same as this stream's schema.

        .. rubric:: Type hints

        The argument type hint on `func` is used (if present) to verify
        at topology declaration time that it is compatible with the
        type of tuples on this stream.

        .. versionadded:: 1.16
        """
        streamsx._streams._hints.check_punctor(func, self)
        sl = _SourceLocation(_source_info(), 'punctor')
        _name = self.topology.graph._requested_name(name, action="punctor", func=func)
        stateful = _determine_statefulness(func)
        params = {}
        _replace = False
        if replace is not None:
            if replace: 
                _replace = True
        if before is not None:
           if before:
               params = {'before': True, 'replace': _replace}
           else:
               params = {'before': False, 'replace': _replace}
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Punctor", func, name=_name, sl=sl, stateful=stateful, params=params)
        op.addInputPort(outputPort=self.oport)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        op._layout(kind='Punctor', name=op.runtime_id, orig_name=name)
        oport = op.addOutputPort(schema=self.oport.schema, name=_name)
        return Stream(self.topology, oport)._make_placeable()


    def filter(self, func, non_matching=False, name=None):
        """
        Filters tuples from this stream using the supplied callable `func`.

        For each stream tuple `t` on the stream ``func(t)`` is called, if the return evaluates to ``True`` the
        tuple will be present on the returned stream, otherwise the tuple is filtered out.
        
        Args:
            func: Filter callable that takes a single parameter for the stream tuple.
            non_matching(bool): Non-matching tuples are sent to a second optional output stream
            name(str): Name of the stream, defaults to a generated name.

        If invoking ``func`` for a stream tuple raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuple is submitted to the filtered
        stream corresponding to the input tuple that caused the exception.

        Example with matching and non matching streams::

            topo = Topology()
            s = topo.source(['Hello', 'World'])
            matches, non_matches = s.filter((lambda t : "Wor" in t), non_matching=True)

        Returns:
            Stream: A Stream containing tuples that have not been filtered out. The schema of the returned stream is the same as this stream's schema. Optional second stream is returned for non matching tuples, if parameter non_matching is set to True.

        .. rubric:: Type hints

        The argument type hint on `func` is used (if present) to verify
        at topology declaration time that it is compatible with the
        type of tuples on this stream.

        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

            The :py:meth:`filter` punctuation mode is "preserving". Incoming window punctuations are forwarded.

        """
        streamsx._streams._hints.check_filter(func, self)
        sl = _SourceLocation(_source_info(), 'filter')
        _name = self.topology.graph._requested_name(name, action="filter", func=func)
        stateful = _determine_statefulness(func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Filter", func, name=_name, sl=sl, stateful=stateful)
        op.addInputPort(outputPort=self.oport)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        op._layout(kind='Filter', name=op.runtime_id, orig_name=name)
        if non_matching:
            oport = op.addOutputPort(schema=self.oport.schema, name=_name+'_matching')
            oport_non_matching = op.addOutputPort(schema=self.oport.schema, name=_name+'_non_matching')
            return Stream(self.topology, oport)._make_placeable(), Stream(self.topology, oport_non_matching)._make_placeable()
        else:
            oport = op.addOutputPort(schema=self.oport.schema, name=_name)
            return Stream(self.topology, oport)._make_placeable()

    def split(self, into, func, names=None, name=None):
        """
        Splits tuples from this stream into multiple independent streams
        using the supplied callable `func`.

        For each tuple on the stream ``int(func(tuple))`` is called, if the
        return is zero or positive then the (unmodified) tuple will be
        present on one, and only one, of the output streams.
        The specific stream will
        be at index ``int(func(tuple)) % N`` in the returned list,
        where ``N`` is the number of output
        streams. If the return is negative then the tuple is dropped.

        ``split`` is used to declare disparate transforms on each
        split stream. This differs to :py:meth:`parallel` where
        each channel has the same logic transforms.
        
        Args:
            into(int): Number of streams the input is split into, must be greater than zero.
            func: Split callable that takes a single parameter for the tuple.
            names(list[str]): Names of the returned streams, in order. If not supplied or a stream doesn't have an entry in `names` then a generated name is used. Entries are used to generated the field names of the returned named tuple.
            name(str): Name of the split transform, defaults to a generated name.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuple is submitted to the filtered
        stream corresponding to the input tuple that caused the exception.

        Returns:
            namedtuple: Named tuple of streams this stream is split across. All returned streams have the same schema as this stream.

        .. rubric:: Type hints

        The argument type hint on `func` is used (if present) to verify
        at topology declaration time that it is compatible with the
        type of tuples on this stream.

        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

            The :py:meth:`split` punctuation mode is "preserving". Incoming window punctuations are forwarded to each output stream.


        .. rubric:: Examples

        Example of splitting a stream based upon message severity, dropping
        any messages with unknown severity, and then performing different
        transforms for each severity::

            msgs = topo.source(ReadMessages())
            SEVS = {'H':0, 'M':1, 'L':2}
            severities = msgs.split(3, lambda t:SEVS.get(t.sev),
               names=['high','medium','low'], name='SeveritySplit')

            high_severity = severities.high
            high_severity.for_each(SendAlert())

            medium_severity = severities.medium
            medium_severity.for_each(LogMessage())

            low_severity = severities.low
            low_severity.for_each(Archive())


        .. seealso:: :py:meth:`parallel`

        .. versionadded:: 1.13
        """
        streamsx._streams._hints.check_split(func, self)
        sl = _SourceLocation(_source_info(), 'split')
        _name = self.topology.graph._requested_name(name, action="split", func=func)
        stateful = _determine_statefulness(func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Split", func, name=_name, sl=sl, stateful=stateful)
        op.addInputPort(outputPort=self.oport)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        op._layout(kind='Split', name=op.runtime_id, orig_name=name)
        streams = []
        nt_names = []
        op_name = name if name else _name
        for port_id in range(into):
            # logical name
            lsn = names[port_id] if names and len(names) > port_id else op_name + '_' + str(port_id)
            sn = self.topology.graph._requested_name(lsn)
            oport = op.addOutputPort(schema=self.oport.schema, name=sn)
            streams.append(Stream(self.topology, oport)._make_placeable())
            nt_names.append(lsn)
            op._layout(name=oport.runtime_id, orig_name=lsn)
        nt = collections.namedtuple(op_name, nt_names, rename=True)
        return nt._make(streams)

    def _map(self, func, schema, name=None):
        schema = streamsx.topology.schema._normalize(schema)
        _name = self.topology.graph._requested_name(name, action="map", func=func)
        stateful = _determine_statefulness(func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Map", func, name=_name, stateful=stateful)
        op.addInputPort(outputPort=self.oport)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        oport = op.addOutputPort(schema=schema, name=_name)
        op._layout(name=op.runtime_id, orig_name=name)
        return Stream(self.topology, oport)._make_placeable()

    def view(self, buffer_time = 10.0, sample_size = 10000, name=None, description=None, start=False):
        """
        Defines a view on a stream.

        A view is a continually updated sampled buffer of a streams's tuples.
        Views allow visibility into a stream from external clients such
        as Jupyter Notebooks, the Streams console,
        `Microsoft Excel <https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.excel.doc/doc/excel_overview.html>`_ or REST clients.

        The view created by this method can be used by external clients
        and through the returned :py:class:`~streamsx.topology.topology.View` object after the topology is submitted. For example a Jupyter Notebook can
        declare and submit an application with views, and then
        use the resultant `View` objects to visualize live data within the streams.

        When the stream contains Python objects then they are converted
        to JSON.

        Args:
            buffer_time: Specifies the buffer size to use measured in seconds.
            sample_size: Specifies the number of tuples to sample per second.
            name(str): Name of the view. Name must be unique within the topology. Defaults to a generated name.
            description: Description of the view.
            start(bool): Start buffering data when the job is submitted.
                If `False` then the view starts buffering data when the first
                remote client accesses it to retrieve data.
 
        Returns:
            streamsx.topology.topology.View: View object which can be used to access the data when the
            topology is submitted.

        .. note:: Views are only supported when submitting to distributed
            contexts including Streaming Analytics service.
        """
        if name is None:
            name = ''.join(random.choice('0123456789abcdef') for x in range(16))

        if self.oport.schema == streamsx.topology.schema.CommonSchema.Python:
            if self._json_stream:
                view_stream = self._json_stream
            else:
                self._json_stream = self.as_json(force_object=False)._layout(hidden=True)
                view_stream = self._json_stream
                # colocate map operator with stream that is being viewed.
                if self._placeable:
                    self._colocate(view_stream, 'view')
        else:
            view_stream = self

        port = view_stream.oport.runtime_id
        view_config = {
                'name': name,
                'port': port,
                'description': description,
                'bufferTime': buffer_time,
                'sampleSize': sample_size}
        if start:
            view_config['activateOption'] = 'automatic'
        view_stream.oport.operator.addViewConfig(view_config)
        _view = View(name)
        self.topology.graph._views.append(_view)
        return _view

    def map(self, func=None, name=None, schema=None):
        """
        Maps each tuple from this stream into 0 or 1 stream tuples.

        The transformation defined by `func` is a callable
        or a composite transformation.

        .. rubric:: Callable transformation

        For each tuple on this stream ``result = func(tuple)`` is called.
        If `result` is not `None` then the result will be submitted
        as a tuple on the returned stream. If `result` is `None` then
        no tuple submission will occur.

        By default the submitted tuple is ``result`` without modification
        resulting in a stream of picklable Python objects. Setting the
        `schema` parameter changes the type of the stream and
        modifies each ``result`` before submission.

        * ``object`` or :py:const:`~streamsx.topology.schema.CommonSchema.Python` - The default:  `result` is submitted.
        * ``str`` type or :py:const:`~streamsx.topology.schema.CommonSchema.String` - A stream of strings: ``str(result)`` is submitted.
        * ``json`` or :py:const:`~streamsx.topology.schema.CommonSchema.Json` - A stream of JSON objects: ``result`` must be convertable to a JSON object using `json` package.
        * :py:const:`~streamsx.topology.schema.StreamSchema` - A structured stream. `result` must be a `dict` or (Python) `tuple`. When a `dict` is returned the outgoing stream tuple attributes are set by name, when a `tuple` is returned stream tuple attributes are set by position.
        * string value - Equivalent to passing ``StreamSchema(schema)``

        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

            The :py:meth:`map` punctuation mode is "preserving". Incoming window punctuations are forwarded.


        .. rubric:: Composite transformation

        A composite transformation is an instance of :py:class:`~streamsx.topology.composite.Map`. Composites allow the application developer to use
        the standard functional style of the topology api while allowing
        allowing expansion of a `map` transform to multiple basic
        transformations.

        Args:
            func: A callable that takes a single parameter for the tuple.
                If not supplied then a function equivalent to ``lambda tuple_ : tuple_`` is used.
            name(str): Name of the mapped stream, defaults to a generated name.
            schema(StreamSchema|CommonSchema|str): Schema of the resulting stream.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuple is submitted to the mapped
        stream corresponding to the input tuple that caused the exception.
       

        Returns:
            Stream: A stream containing tuples mapped by `func`.

        .. rubric:: Type hints

        If `schema` is not set then the return type hint on `func` define the
        schema of the returned stream, defaulting to
        :py:const:`~streamsx.topology.schema.CommonSchema.Python` if no
        type hints are present.

        For example `reading_from_json` has a type hint that
        defines it as returning ``SensorReading`` instances (typed named tuples).
        Thus `readings` has a structured schema matching ``SensorReading`` ::

            def reading_from_json(value:dict) -> SensorReading:
                return SensorReading(value['id'], value['timestamp'], value['reading'])

            topo = Topology()
            json_readings = topo.source(HttpReadings()).as_json()
            readings = json_readings.map(reading_from_json)

        The argument type hint on `func` is used (if present) to verify
        at topology declaration time that it is compatible with the
        type of tuples on this stream.

        .. versionadded:: 1.7 `schema` argument added to allow conversion to
            a structured stream.
        .. versionadded:: 1.8 Support for submitting `dict` objects as stream tuples to a structured stream (in addition to existing support for `tuple` objects).
        .. versionchanged:: 1.11 `func` is optional.
        """
        import streamsx.topology.composite
        if isinstance(func, streamsx.topology.composite.Map):
            return func._add(self, schema, name)

        # Schema mapping only, if no change then return original
        if func is None and name is None and (schema is not None and
            streamsx.topology.schema._normalize(schema) == self.oport.schema):
            return self

        hints = None
        if func is not None:
            hints = streamsx._streams._hints.check_map(func, self)
        if schema is None:
            schema = hints.schema if hints else streamsx.topology.schema.CommonSchema.Python
        if func is None:
            func = streamsx.topology.runtime._identity
            if name is None:
               name = 'identity'
     
        ms = self._map(func, schema=schema, name=name)._layout('Map')
        ms.oport.operator.sl = _SourceLocation(_source_info(), 'map')
        return ms._add_hints(hints)

    def flat_map(self, func=None, name=None):
        """
        Maps and flatterns each tuple from this stream into 0 or more tuples.


        For each tuple on this stream ``func(tuple)`` is called.
        If the result is not `None` then the the result is iterated over
        with each value from the iterator that is not `None` will be submitted
        to the return stream.

        If the result is `None` or an empty iterable then no tuples are submitted to
        the returned stream.
        
        Args:
            func: A callable that takes a single parameter for the tuple.
                If not supplied then a function equivalent to ``lambda tuple_ : tuple_`` is used.
                This is suitable when each tuple on this stream is an iterable to be flattened.
                
            name(str): Name of the flattened stream, defaults to a generated name.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuples are submitted to the flattened
        and mapped stream corresponding to the input tuple
        that caused the exception.

        Example: For a list of dict the ``flat_map`` emits **n** tuples for each input tuple received, with **n** the number of elements in the list::
            
            from typing import Iterable, List, NamedTuple

            class SampleSchema(NamedTuple):
                id: str
                flag: bool

            def flatten_dict(tpl) -> Iterable[SampleSchema]:
                return tpl

            # list_stream is a stream of list from dict as Python object, for example [{'id': '0', 'flag':True}]       
            sample_stream = list_stream.flat_map(flatten_dict) # sample_stream is a named tuple stream of SampleSchema

        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation.

            The :py:meth:`flat_map` punctuation mode is "preserving". Incoming window punctuations are forwarded.


        Returns:
            Stream: A Stream containing flattened and mapped tuples.
        Raises:
            TypeError: if `func` does not return an iterator nor None

        .. versionchanged:: 1.11 `func` is optional.
        """     
        hints = None
        if func is None:
            func = streamsx.topology.runtime._identity
            if name is None:
               name = 'flatten'
        else:
            hints = streamsx._streams._hints.check_flat_map(func, self)
     
        sl = _SourceLocation(_source_info(), 'flat_map')
        _name = self.topology.graph._requested_name(name, action='flat_map', func=func)
        stateful = _determine_statefulness(func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::FlatMap", func, name=_name, sl=sl, stateful=stateful)
        op.addInputPort(outputPort=self.oport)
        schema = hints.schema if hints else streamsx.topology.schema.CommonSchema.Python
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        oport = op.addOutputPort(name=_name, schema=schema)
        return Stream(self.topology, oport)._make_placeable()._layout('FlatMap', name=op.runtime_id, orig_name=name)._add_hints(hints)
    
    def catch_exceptions(self, exception_type:str='streams', tuple_trace:bool=False, stack_trace:bool=False):
        """ When applied to a primitive operator, exceptions of the specified type that are thrown by the operator while processing a tuple are caught. 

        .. note:: You cannot use this on an operator without input streams.

        Example using default values (tuple trace and stack trace disabled) and catch exceptions thrown by the Python primitive operator calling the ``map()`` transformation. This **map** callable raises a ValueError (*"invalid literal for int() with base 10: 'five'"*) when processing the sixt tuple. With ``catch_exceptions()`` applied the application is able to process all 10 tuples and does not stop processing.::

           from typing import NamedTuple
           class NumbersSchema(NamedTuple):
              num: int

           topo = Topology()
           str_stream = topo.source(['0','1','2','3','4','five','6','7','8','9']).as_string()

           num_stream = str_stream.map(lambda t: {'num': int(t)}, schema=NumbersSchema)
           num_stream.catch_exceptions()

           num_stream.print()

        Example using the SPL operator Functor and enabled tuple trace::

           topo = Topology()
           str_stream = topo.source(['0','1','2','3','4','five','6','7','8','9']).as_string()

           f = op.Map('spl.relational::Functor', s, schema='tuple<int64 num>')
           f.num = f.output('(int64) string')
           num_stream = f.stream
           num_stream.catch_exceptions(tuple_trace=True)

           num_stream.print()

        Args:
            exception_type(str): Indicates the type of exceptions to be caught by the run time environment. Supported options include:

                * ``none``: No exceptions of any type are caught.

                * ``streams``: Only IBM® Streams exceptions are caught. This includes exceptions that are thrown by SPL native functions from the standard toolkit, other exceptions that extend from the C++ SPL::SPLRuntimeException, and exceptions that extend from the Java com.ibm.streams.operator.DataException (extending from java.lang.RuntimeException).

                * ``std``: Both IBM Streams and standard exceptions are caught. For C++, standard exception means std::exception. For Java, standard exception means all checked exceptions that inherit from java.lang.Exception.

                * ``all``: In C++, any thrown exception is caught. For Java, any checked and unchecked exception that inherits from java.lang.Exception is caught.

            tuple_trace(bool): Enables or disables the tracing of tuple data. Tracing of data can be enabled when tuples do not contain sensitive data and the data can show up into PE logs. Tuples are logged to the trace facility with the ERROR trace level.
            stack_trace(bool): Enables or disables the printout of the stack trace to the Streams trace facility. Stack traces are printed to the Streams trace facility with the trace level ERROR.

        Returns:
            Stream: Returns this stream.

        .. versionadded:: 2.1
        """
        if exception_type is None:
           raise ValueError("Parameter exception_type must not be None.")
        else:
           if exception_type not in {'none', 'all', 'streams', 'std'}:
              raise ValueError("Invalid value for parameter exception_type (supported options: 'none', 'all', 'streams', 'std').")

        props = {'exception':exception_type}

        if tuple_trace is not None:
           if tuple_trace:
              props['tupleTrace'] = 'true'
           else:
              props['tupleTrace'] = 'false'
        if stack_trace is not None:
           if not stack_trace:
              props['stackTrace'] = 'false'
           else:
              props['stackTrace'] = 'true'

        # create annotation dict
        annotation = {'type':'catch', 'properties': props}
        self.oport.operator._annotation(annotation)
        return self._make_placeable()

    def isolate(self):
        """
        Guarantees that the upstream operation will run in a separate processing element from the downstream operation

        Returns:
            Stream: Stream whose subsequent immediate processing will occur in a separate processing element.
        """
        op = self.topology.graph.addOperator("$Isolate$")
        # does the addOperator above need the packages
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport, other=self)

    def low_latency(self):
        """
        The function is guaranteed to run in the same process as the
        upstream Stream function. All streams that are created from the returned stream 
        are also guaranteed to run in the same process until end_low_latency() 
        is called.

        Returns:
            Stream
        """
        op = self.topology.graph.addOperator("$LowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport, other=self)

    def end_low_latency(self):
        """
        Returns a Stream that is no longer guaranteed to run in the same process
        as the calling stream.

        Returns:
            Stream
        """
        op = self.topology.graph.addOperator("$EndLowLatency$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport, other=self)
    
    def parallel(self, width, routing=Routing.ROUND_ROBIN, func=None, keys=None, name=None):
        """
        Split stream into channels and start a parallel region.

        Returns a new stream that will contain the contents of
        this stream with tuples distributed across its channels.

        The returned stream starts a parallel region where all
        downstream transforms are replicated across `width` channels.
        A parallel region is terminated by :py:meth:`end_parallel`
        or :py:meth:`for_each`.

        Any transform (such as :py:meth:`map`, :py:meth:`filter`, etc.) in
        a parallel region has a copy of its callable executing
        independently in parallel. Channels remain independent
        of other channels until the region is terminated.

        For example with this topology fragment a parallel region
        of width 3 is created::

            s = ...
            p = s.parallel(3)
            p = p.filter(F()).map(M())
            e = p.end_parallel()
            e.for_each(E())

        Tuples from ``p`` (parallelized ``s``)  are distributed
        across three channels, 0, 1 & 2
        and are independently processed by three instances of ``F`` and ``M``.
        The tuples that pass the filter ``F`` in channel 0 are then mapped
        by the instance of ``M`` in channel 0, and so on for channels 1 and 2.

        The channels are combined by ``end_parallel`` and so a single instance
        of ``E`` processes all the tuples from channels 0, 1 & 2.

        This stream instance (the original) is outside of the parallel region
        and so any downstream transforms are executed normally.
        Adding this `map` transform would result in tuples
        on ``s`` being processed by a single instance of ``N``::

            n = s.map(N())

        The number of channels is set by `width` which may be an `int` greater
        than zero or a submission parameter created by
        :py:meth:`Topology.create_submission_parameter`.

        With IBM Streams 4.3 or later the number of channels can be
        dynamically changed at runtime.

        Tuples are routed to channels based upon `routing`, see :py:class:`Routing`.

        A parallel region can have multiple termination points, for
        example when a stream within the stream has multiple transforms
        against it::

            s = ...
            p = s.parallel(3)
            m1p = p.map(M1())
            m2p = p.map(M2())
            p.for_each(E())

            m1 = m1p.end_parallel()
            m2 = m2p.end_parallel()

        Parallel regions can be nested, for example::

            s = ...
            m = s.parallel(2).map(MO()).parallel(3).map(MI()).end_parallel().end_parallel()

        In this case there will be two instances of ``MO`` (the outer region) and six (2x3) instances of ``MI`` (the inner region).
         
        Streams created by :py:meth:`~Topology.source` or
        :py:meth:`~Topology.subscribe` are placed in a parallel region
        by :py:meth:`set_parallel`.
        
        Args:
            width(int|submission parameter created by :py:meth:`Topology.create_submission_parameter`): Degree of parallelism.
            routing(Routing): Denotes what type of tuple routing to use.
            func: Optional function called when :py:const:`Routing.HASH_PARTITIONED` routing is specified.
                The function provides an integer value to be used as the hash that determines
                the tuple channel routing.
            keys([str]): Optional list of keys required when :py:const:`Routing.KEY_PARTITIONED` routing is specified. Each key represents a tuple attribute.
            name (str): The name to display for the parallel region.

        Returns:
            Stream: A stream for which subsequent transformations will be executed in parallel.

        .. seealso:: :py:meth:`set_parallel`, :py:meth:`end_parallel`, :py:meth:`split`
        """
        _name = name
        if _name is None:
            _name = self.name + '_parallel'
            
        _name = self.topology.graph._requested_name(_name, action='parallel', func=func)

        if routing is None or routing == Routing.ROUND_ROBIN or routing == Routing.BROADCAST:
            op2 = self.topology.graph.addOperator("$Parallel$", name=_name)
            if name is not None:
                op2.config['regionName'] = _name
            op2.addInputPort(outputPort=self.oport)
            if routing == Routing.BROADCAST:
                oport = op2.addOutputPort(width, schema=self.oport.schema, routing="BROADCAST", name=_name)
            else:
                oport = op2.addOutputPort(width, schema=self.oport.schema, routing="ROUND_ROBIN", name=_name)
                
            return Stream(self.topology, oport, other=self)
        elif routing == Routing.HASH_PARTITIONED:

            if (func is None):
                if self.oport.schema == streamsx.topology.schema.CommonSchema.String:
                    keys = ['string']
                    parallel_input = self.oport
                elif self.oport.schema == streamsx.topology.schema.CommonSchema.Python:
                    func = hash
                else:
                    raise NotImplementedError("HASH_PARTITIONED for schema {0} requires a hash function.".format(self.oport.schema))

            if func is not None:
                keys = ['__spl_hash']
                stateful = _determine_statefulness(func)
                hash_adder = self.topology.graph.addOperator(self.topology.opnamespace+"::HashAdder", func, stateful=stateful)
                hash_adder._op_def['hashAdder'] = True
                hash_adder._layout(hidden=True)
                hash_schema = self.oport.schema.extend(streamsx.topology.schema.StreamSchema("tuple<int64 __spl_hash>"))
                hash_adder.addInputPort(outputPort=self.oport)
                streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, hash_adder, 'pyStyle')
                parallel_input = hash_adder.addOutputPort(schema=hash_schema)

            parallel_op = self.topology.graph.addOperator("$Parallel$", name=_name)
            if name is not None:
                parallel_op.config['regionName'] = _name
            parallel_op.addInputPort(outputPort=parallel_input)
            parallel_op_port = parallel_op.addOutputPort(oWidth=width, schema=parallel_input.schema, partitioned_keys=keys, routing="HASH_PARTITIONED")

            if func is not None:
                # use the Functor passthru operator to remove the hash attribute by removing it from output port schema
                hrop = self.topology.graph.addPassThruOperator()
                hrop._layout(hidden=True)
                hrop.addInputPort(outputPort=parallel_op_port)
                parallel_op_port = hrop.addOutputPort(schema=self.oport.schema)

            return Stream(self.topology, parallel_op_port, other=self)
        elif routing == Routing.KEY_PARTITIONED:
            if (keys is None):        
                raise NotImplementedError("KEY_PARTITIONED for schema {0} requires a array of keys set in keys parameter.".format(self.oport.schema))
                
            if False == isinstance(keys, list):
                raise TypeError("Invalid keys {0}, list type required.".format(keys))

            if False == isinstance(self.oport.schema, streamsx.topology.schema.StreamSchema):
                raise TypeError("Routing type KEY_PARTITIONED requires structured schema, use StreamsSchema or named tuple.")
                
            for key in keys:
                if key not in str(self.oport.schema):
                    raise ValueError("Invalid keys {0} for routing type KEY_PARTITIONED and schema {1}.".format(keys, self.oport.schema))
            
            op2 = self.topology.graph.addOperator("$Parallel$", name=_name)
            if name is not None:
                op2.config['regionName'] = _name
            op2.addInputPort(outputPort=self.oport)
            oport = op2.addOutputPort(oWidth=width, schema=self.oport.schema, partitioned_keys=keys, routing="KEY_PARTITIONED", name=_name)
                
            return Stream(self.topology, oport, other=self)
        else :
            raise TypeError("Invalid routing type supplied to the parallel operator")    

    def end_parallel(self):
        """
        Ends a parallel region by merging the channels into a single stream.

        Returns:
            Stream: Stream for which subsequent transformations are no longer parallelized.

        .. seealso:: :py:meth:`set_parallel`, :py:meth:`parallel`
        """
        outport = self.oport
        if isinstance(self.oport.operator, streamsx.topology.graph.Marker):
            if self.oport.operator.kind == "$Union$":
                pto = self.topology.graph.addPassThruOperator()
                pto.addInputPort(outputPort=self.oport)
                outport = pto.addOutputPort(schema=self.oport.schema)
        op = self.topology.graph.addOperator("$EndParallel$")
        op.addInputPort(outputPort=outport)
        oport = op.addOutputPort(schema=self.oport.schema)
        endP = Stream(self.topology, oport, other=self)
        return endP

    def set_parallel(self, width, name=None):
        """
        Set this source stream to be split into multiple channels
        as the start of a parallel region.

        Calling ``set_parallel`` on a stream created by
        :py:meth:`~Topology.source` results in the stream
        having `width` channels, each created by its own instance
        of the callable::

           s = topo.source(S())
           s.set_parallel(3)
           f = s.filter(F())
           e = f.end_parallel()

        Each channel has independent instances of ``S`` and ``F``. Tuples
        created by the instance of ``S`` in channel 0 are passed to the
        instance of ``F`` in channel 0, and so on for channels 1 and 2.

        Callable transforms instances within the channel can use
        the runtime functions
        :py:func:`~streamsx.ec.channel`, 
        :py:func:`~streamsx.ec.local_channel`, 
        :py:func:`~streamsx.ec.max_channels` &
        :py:func:`~streamsx.ec.local_max_channels`
        to adapt to being invoked in parallel. For example a
        source callable can use its channel number to determine
        which partition to read from in a partitioned external system.

        Calling ``set_parallel`` on a stream created by
        :py:meth:`~Topology.subscribe` results in the stream
        having `width` channels. Subscribe ensures that the
        stream will contain all published tuples matching the
        topic subscription and type. A published tuple will appear
        on one of the channels though the specific channel is not known
        in advance.

        A parallel region is terminated by :py:meth:`end_parallel`
        or :py:meth:`for_each`.

        The number of channels is set by `width` which may be an `int` greater
        than zero or a submission parameter created by
        :py:meth:`Topology.create_submission_parameter`.

        With IBM Streams 4.3 or later the number of channels can be
        dynamically changed at runtime.

        Parallel regions are started on non-source streams using
        :py:meth:`parallel`.

        Args:
            width(int|submission parameter created by :py:meth:`Topology.create_submission_parameter`): The degree of parallelism for the parallel region.
            name(str): Name of the parallel region. Defaults to the name of this stream.

        Returns:
            Stream: Returns this stream.

        .. seealso:: :py:meth:`parallel`, :py:meth:`end_parallel`

        .. versionadded:: 1.9
        .. versionchanged:: 1.11 `name` parameter added.
        """
        self.oport.operator.config['parallel'] = True
        self.oport.operator.config['width'] = streamsx.topology.graph._as_spl_json(width, int)
        if name:
            name = self.topology.graph._requested_name(str(name), action='set_parallel')
            self.oport.operator.config['regionName'] = name
        return self

    def set_consistent(self, consistent_config):
        """ Indicates that the stream is the start of a consistent region.

        Args:
            consistent_config(consistent.ConsistentRegionConfig): the configuration of the consistent region.

        Returns:
            Stream: Returns this stream.

        .. versionadded:: 1.11
        """

        # add job control plane if needed
        self.topology._add_job_control_plane()
        self.oport.operator.consistent(consistent_config)
        return self._make_placeable()

    def set_event_time(self, name, lag=None, minimum_gap=None, resolution=None):
        """ Emit a stream with event-time values and watermarks. 

        Defines the stream attribute with the parameter `name` that is used as event-time attribute in the event-time graph.
        An event-time graph starts with this stream and inserts watermarks into the stream from time to time.

        * Event-time connectivity extends only downstream.
        * The event-time graph ends at a sink or at an operator which does not output the event-time attribute.

        A sample application creating an event_time stream and using a *time-interval* window (:py:func:`streamsx.topology.topology.Stream.time_interval`) is located in the samples directory: `Event-Time-Sample <https://github.com/IBMStreams/streamsx.topology/tree/develop/samples/python/topology/spl/vwap_event_time>`_

        Sample with an attribute named ``ts`` of type ``timestamp`` used as event-time attribute::

           from streamsx.spl.types import Timestamp
           ts1 = Timestamp(1608196, 235000000, 0)
           s = topo.source([(1,ts1)])

           # transform to structured schema
           ts_schema = StreamSchema('tuple<int64 num, timestamp ts>').as_tuple(named=True)
           s = s.map(lambda x : x, schema=ts_schema, name='event_time_source')

           # add event-time annotation for attribute ts to the "event_time_source"
           s = s.set_event_time('ts')

        Args:
            name(str): Name of the event-time attribute.
            lag(float|submission parameter created by :py:meth:`Topology.create_submission_parameter`): Defines the duration in seconds between the maximum event-time of submitted tuples and the value of the watermark to submit. If it is not specified, the default value is 0.0.
            minimum_gap(float|submission parameter created by :py:meth:`Topology.create_submission_parameter`): Defines the minimum event-time duration in seconds between subsequent watermarks. If it is not specified, the default value is 0.1 (100 milliseconds).
            resolution(str): Specifies the resolution of the event-time attribute in: Milliseconds, Microseconds, Nanoseconds. If the event-time attribute is of type SPL timestamp, the default resolution value is nanoseconds. If the event-time attribute is of type int, the default resolution value is milliseconds.

        Returns:
            Stream: Returns this stream.

        .. versionadded:: 2.1
        """
        if name is None:
           raise ValueError("Attribute name must not be None.")
        
        props = {'eventTimeAttribute':name}
        if lag is not None:
           if isinstance(lag, streamsx.topology.runtime._SubmissionParam):
              props['lag'] = lag.spl_json()
           else:
              if isinstance(lag, float):
                 props['lag'] = lag
              else:
                 raise TypeError("Float type expected for parameter: lag")
        if minimum_gap is not None:
           if isinstance(minimum_gap, streamsx.topology.runtime._SubmissionParam):
              props['minimumGap'] = minimum_gap.spl_json()
           else:
              if isinstance(minimum_gap, float):
                 props['minimumGap'] = minimum_gap
              else:
                 raise TypeError("Float type expected for parameter: minimum_gap")
        if resolution is not None:
           if isinstance(resolution, str):
              props['resolution'] = resolution
           else:
              raise TypeError("String type expected for parameter: resolution")

        # create eventtime annotation dict
        annotation = {'type':'eventTime', 'properties': props}
        self.oport.operator._annotation(annotation)
        return self._make_placeable()

    def last(self, size=1):
        """ Declares a slding window containing most recent tuples
        on this stream.

        The number of tuples maintained in the window is defined by `size`.

        If `size` is an `int` then it is the count of tuples in the window.
        For example, with ``size=10`` the window always contains the
        last (most recent) ten tuples.

        If `size` is an `datetime.timedelta` then it is the duration
        of the window. With a `timedelta` representing five minutes
        then the window contains any tuples that arrived in the last
        five minutes.

        If `size` is an `submission parameter` created by :py:meth:`Topology.create_submission_parameter` then it is the count of tuples in the window.
        For specifying the duration of the window with a submission parameter use :py:meth:`~Stream.lastSeconds`.
 
        Args:
            size(int|datetime.timedelta|submission parameter created by :py:meth:`Topology.create_submission_parameter`): The size of the window, either an `int` to define the
                number of tuples or `datetime.timedelta` to define the
                duration of the window or
                submission parameter created by :py:meth:`Topology.create_submission_parameter`
                to define the number of tuples.

        Examples::

            # Create a window against stream s of the last 100 tuples
            w = s.last(size=100)

        ::

            # Create a window against stream s of the last n tuples specified by submission parameter
            count = topo.create_submission_parameter('count', 100)
            w = s.last(size=count)

        ::

            # Create a window against stream s of tuples
            # arrived on the stream in the last five minutes
            w = s.last(size=datetime.timedelta(minutes=5))

        Returns:
            Window: Window of the last (most recent) tuples on this stream.
        """
        win = Window(self, 'SLIDING')
        if isinstance(size, datetime.timedelta):
            win._evict_time(size)
        elif isinstance(size, int):
            win._evict_count(size)
        elif isinstance(size, streamsx.topology.runtime._SubmissionParam):
            win._evict_count_stv(size)
        else:
            raise ValueError(size)
        return win

    def lastSeconds(self, size):
        """ Declares a slding window containing most recent tuples
        on this stream using a submission parameter created by
        :py:meth:`Topology.create_submission_parameter`.

        The number of tuples maintained in the window is defined by `size` in seconds.
 
        Args:
            size(submission parameter created by :py:meth:`Topology.create_submission_parameter`): The size of the window in seconds.

        Examples::

            # Create a window against stream s of the last with submission parameter `time` and the default value 10 seconds
            time = topo.create_submission_parameter('time', 10)
            w = s.lastSeconds(time)

        ::

            # Create a window with submission parameter `secs` and no default value 
            time = topo.create_submission_parameter(name='secs', type_=int)
            w = s.lastSeconds(time)

        Returns:
            Window: Window of the last (most recent) tuples on this stream.
        """
        win = Window(self, 'SLIDING')
        if isinstance(size, streamsx.topology.runtime._SubmissionParam):
            win._evict_time_stv(size)
        else:
            raise ValueError(size)
        return win

    def batch(self, size):
        """ Declares a tumbling window to support batch processing
        against this stream.

        The number of tuples in the batch is defined by `size`.

        If `size` is an ``int`` then it is the count of tuples in the batch.
        For example, with ``size=10`` each batch will nominally
        contain ten tuples. Thus processing against the returned
        :py:class:`Window`, such as :py:meth:`~Window.aggregate` will be
        executed every ten tuples against the last ten tuples on the stream.
        For example the first three aggregations would be against
        the first ten tuples on the stream, then the next ten tuples
        and then the third ten tuples, etc.

        If `size` is an `datetime.timedelta` then it is the duration
        of the batch using wallclock time.
        With a `timedelta` representing five minutes
        then the window contains any tuples that arrived in the last
        five minutes.  Thus processing against the returned :py:class:`Window`,
        such as :py:meth:`~Window.aggregate` will be executed every five minutes tuples
        against the batch of tuples arriving in the last five minutes
        on the stream. For example the first three aggregations would be
        against any tuples on the stream in the first five minutes,
        then the next five minutes and then minutes ten to fifteen.
        A batch can contain no tuples if no tuples arrived on the stream
        in the defined duration.

        For specifying the duration of the window with a submission parameter use :py:meth:`~Stream.batchSeconds`.

        Each tuple on the stream appears only in a single batch.

        The number of tuples seen by processing against the
        returned window may be less than `size` (count or time based)
        when:

            * the stream is finite, the final batch may contain less tuples than the defined size,
            * the stream is in a consistent region, drain processing will complete the current batch without waiting for it to batch to reach its nominal size.

        Examples::

            # Create batches against stream s of 100 tuples each
            w = s.batch(size=100)

        ::

            # Create a window size specified by submission parameter
            count = topo.create_submission_parameter('count', 100)
            w = s.batch(size=count)

        ::

            # Create batches against stream s every five minutes
            w = s.batch(size=datetime.timedelta(minutes=5))

        ::

            # Create a tumbling punctuation-based window 
            w = s.batch('punct')

        Args:
            size(int|datetime.timedelta|submission parameter created by :py:meth:`Topology.create_submission_parameter`|'punct'): The size of each batch, either an `int` to define the
                number of tuples or `datetime.timedelta` to define the
                duration of the batch or
                submission parameter created by :py:meth:`Topology.create_submission_parameter`
                to define the number of tuples or string 'punct' to create a punctuation-based window.

        Returns:
            Window: Window allowing batch processing on this stream.

        .. versionadded:: 1.11
        """
        win = Window(self, 'TUMBLING')
        if isinstance(size, datetime.timedelta):
            win._evict_time(size)
        elif isinstance(size, int):
            win._evict_count(size)
        elif isinstance(size, streamsx.topology.runtime._SubmissionParam):
            win._evict_count_stv(size)
        elif type(size) == str:
            if (size.lower() == 'punct') or (size.lower() == 'punctuation'):
                win._evict_punct()
            else:
                raise ValueError(size)
        else:
            raise ValueError(size)
        return win

    def batchSeconds(self, size):
        """ Declares a tumbling window to support batch processing
        against this stream using a submission parameter created by
        :py:meth:`Topology.create_submission_parameter`.

        The size of the window is defined by the parameter `size` in seconds.
 
        Args:
            size(submission parameter created by :py:meth:`Topology.create_submission_parameter`): The size of the window in seconds.

        Examples::

            # Create a tumbling window with submission parameter `time` and the default value 10 seconds
            time = topo.create_submission_parameter('time', 10)
            w = s.batchSeconds(time)

        ::

            # Create a window with submission parameter `secs` and no default value 
            time = topo.create_submission_parameter(name='secs', type_=int)
            w = s.batchSeconds(time)

        Returns:
            Window: Window allowing batch processing on this stream.
        """
        win = Window(self, 'TUMBLING')
        if isinstance(size, streamsx.topology.runtime._SubmissionParam):
            win._evict_time_stv(size)
        else:
            raise ValueError(size)
        return win


    def time_interval(self, interval_duration, creation_period=None, discard_age=None, interval_offset=None):
        """ Declares a *time-interval* window and specifies that the window-kind tuples are placed into panes which correspond to equal intervals in the event-time domain.

        A *time-interval* window collects tuples into fixed-duration intervals defined over event time.
        *Time-interval* windows collect tuples into window panes specified by event-time intervals.
        A pane includes tuples with an event time greater or equal to the start time of the pane and lower than the end time.

        Find a sample application creating an *event-time* stream (:py:func:`streamsx.topology.topology.Stream.set_event_time`) and using a *time-interval* window in the samples directory: `Event-Time-Sample <https://github.com/IBMStreams/streamsx.topology/tree/develop/samples/python/topology/spl/vwap_event_time>`_

        Args:
            interval_duration(float): Specifies the required duration between the lower and upper interval endpoints. It must be greater than zero (0.0). The parameter value represents seconds.
            creation_period(float): Specifies the duration between adjacent intervals. The default value is equal to interval_duration. It must be greater than zero (0.0). The parameter value represents seconds.
            discard_age(float): Defines the duration between the point in time when a window pane becomes complete and the point in time when the window does not accept late tuples any longer. It must be greater or equal to zero (0.0). The default value is zero (0.0). The parameter value represents seconds.
            interval_offset(float): Defines a point-in-time value which coincides with an interval start time. Panes partition the event time domain into intervals of the form: ``[N * creation_period + interval_offset, N * creation_period + interval_duration + interval_offset)`` where 0.0 is the Unix Epoch: 1970-01-01T00:00:00Z UTC. The parameter value represents seconds.

        Examples::

            w = s.time_interval(interval_duration=60.0, creation_period=1.0)

        Returns:
            Window: Event-time window on this stream.

        .. versionadded:: 2.1
        """
        win = Window(self, 'TIME_INTERVAL')
        if interval_duration is None:
           raise ValueError("Parameter interval_duration must be greater than zero (0.0)")
        else:
           if not isinstance(interval_duration, float):
              raise TypeError("Parameter interval_duration must be float")
        if creation_period is not None:
           if not isinstance(creation_period, float):
              raise TypeError("Parameter creation_period must be float")
        if discard_age is not None:
           if not isinstance(discard_age, float):
              raise TypeError("Parameter discard_age must be float")
        if interval_offset is not None:
           if not isinstance(interval_offset, float):
              raise TypeError("Parameter interval_offset must be float")
        
        win._evict_time_interval(interval_duration, creation_period, discard_age, interval_offset)

        return win


    def union(self, streamSet):
        """
        Creates a stream that is a union of this stream and other streams
        
        Args:
            streamSet: a set of Stream objects to merge with this stream
        Returns:
            Stream:
        """
        if(not isinstance(streamSet,set)) :
            raise TypeError("The union operator parameter must be a set object")
        if(len(streamSet) == 0):
            return self        
        op = self.topology.graph.addOperator("$Union$")
        op.addInputPort(outputPort=self.oport)
        for stream in streamSet:
            op.addInputPort(outputPort=stream.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport)

    def print(self, tag=None, name=None, write_punctuations=None):
        """
        Prints each tuple to stdout flushing after each tuple.

        If `tag` is not `None` then each tuple has "tag: " prepended
        to it before printing.

        .. note::
             Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. One example is the :py:meth:`~Window.aggregate`, which inserts a window marker into the output stream after each aggregation. 
             
             There are two kinds of punctuation markers, which are written to stdout when `write_punctuations` is set to `True`:

             * Window punctuation: indicates breaks in the data, which can be used by the transformation logic
             * Final punctuation: indicates the end of a stream


        Args:
            tag: A tag to prepend to each tuple.
            name(str): Name of the resulting stream.
                When `None` defaults to a generated name.
            write_punctuations(bool): Specifies to write punctuations to stdout
        Returns:
            streamsx.topology.topology.Sink: Stream termination.

        .. versionadded:: 1.6.1 `tag`, `name` parameters.

        .. versionchanged:: 1.7
            Now returns a :py:class:`Sink` instance.

        .. versionadded:: 1.16 `write_punctuations` parameter.
        """
        _name = name
        if _name is None:
            _name = 'print'
        fn = streamsx.topology.functions.print_flush
        if tag is not None:
            tag = str(tag) + ': '
            fn = lambda v : streamsx.topology.functions.print_flush(tag + str(v))
        sp = self.for_each(fn, name=_name)
        sp._op().sl = _SourceLocation(_source_info(), 'print')
        if write_punctuations is not None:
           if write_punctuations:
               sp._op().params['writePunctuations'] = True
               if tag is not None:
                   sp._op().params['writeTag'] = tag
        return sp

    def publish(self, topic, schema=None, name=None):
        """
        Publish this stream on a topic for other Streams applications to subscribe to.
        A Streams application may publish a stream to allow other
        Streams applications to subscribe to it. A subscriber
        matches a publisher if the topic and schema match.

        By default a stream is published using its schema.

        A stream of :py:const:`Python objects <streamsx.topology.schema.CommonSchema.Python>` can be subscribed to by other Streams Python applications.

        If a stream is published setting `schema` to
        ``json`` or :py:const:`~streamsx.topology.schema.CommonSchema.Json`
        then it is published as a stream of JSON objects.
        Other Streams applications may subscribe to it regardless
        of their implementation language.

        If a stream is published setting `schema` to
        ``str`` or :py:const:`~streamsx.topology.schema.CommonSchema.String`
        then it is published as strings.
        Other Streams applications may subscribe to it regardless
        of their implementation language.

        Supported values of `schema` are only
        ``json``, :py:const:`~streamsx.topology.schema.CommonSchema.Json`
        and
        ``str``, :py:const:`~streamsx.topology.schema.CommonSchema.String`.

        Args:
            topic(str): Topic to publish this stream to.
            schema: Schema to publish. Defaults to the schema of this stream.
            name(str): Name of the publish operator, defaults to a generated name.
        Returns:
            streamsx.topology.topology.Sink: Stream termination.

        .. versionadded:: 1.6.1 `name` parameter.

        .. versionchanged:: 1.7
            Now returns a :py:class:`Sink` instance.
        """
        sl = _SourceLocation(_source_info(), 'publish')
        _name = self.topology.graph._requested_name(name, action="publish")
        schema = streamsx.topology.schema._normalize(schema)
        group_id = None
        if schema is not None and self.oport.schema.schema() != schema.schema():
            nc = None
            if schema == streamsx.topology.schema.CommonSchema.Json:
                pub_stream = self.as_json()
            elif schema == streamsx.topology.schema.CommonSchema.String:
                pub_stream = self.as_string()
            else:
                raise ValueError(schema)
            # See #https://github.com/IBMStreams/streamsx.topology.issues/2161
            # group_id = pub_stream._op()._layout_group('Publish', name if name else _name)
            if self._placeable:
                self._colocate(pub_stream, 'publish')
        else:
            pub_stream = self

        # publish is never stateful
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.topic::Publish", params={'topic': topic}, sl=sl, name=_name, stateful=False)
        op.addInputPort(outputPort=pub_stream.oport)
        op._layout_group('Publish', name if name else _name, group_id=group_id)
        sink = Sink(op)

        if pub_stream._placeable:
            pub_stream._colocate(sink, 'publish')
        return sink

    def autonomous(self):
        """
        Starts an autonomous region for downstream processing.
        By default IBM Streams processing is executed in an autonomous region
        where any checkpointing of operator state is autonomous (independent)
        of other operators.
        
        This method may be used to end a consistent region by starting an
        autonomous region. This may be called even if this stream is in
        an autonomous region.

        Autonomous is not applicable when a topology is submitted
        to a STANDALONE contexts and will be ignored.

        .. versionadded:: 1.6

        Returns:
            Stream: Stream whose subsequent downstream processing is in an autonomous region.
        """
        op = self.topology.graph.addOperator("$Autonomous$")
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport, other=self)

    def as_string(self, name=None):
        """
        Declares a stream converting each tuple on this stream
        into a string using `str(tuple)`.

        The stream is typed as a :py:const:`string stream <streamsx.topology.schema.CommonSchema.String>`.

        If this stream is already typed as a string stream then it will
        be returned (with no additional processing against it and `name`
        is ignored).

        Args:
            name(str): Name of the resulting stream.
                When `None` defaults to a generated name.

        .. versionadded:: 1.6
        .. versionadded:: 1.6.1 `name` parameter added.

        Returns:
            Stream: Stream containing the string representations of tuples on this stream.
        """
        sas = self._change_schema(streamsx.topology.schema.CommonSchema.String, 'as_string', name)._layout('AsString')
        sas.oport.operator.sl = _SourceLocation(_source_info(), 'as_string')
        return sas._add_hints(streamsx._streams._hints.STR_HINTS)

    def as_json(self, force_object=True, name=None):
        """
        Declares a stream converting each tuple on this stream into
        a JSON value.

        The stream is typed as a :py:const:`JSON stream <streamsx.topology.schema.CommonSchema.Json>`.

        Each tuple must be supported by `JSONEncoder`.

        If `force_object` is `True` then each tuple that not a `dict` 
        will be converted to a JSON object with a single key `payload`
        containing the tuple. Thus each object on the stream will
        be a JSON object.

        If `force_object` is `False` then each tuple is converted to
        a JSON value directly using `json` package.

        If this stream is already typed as a JSON stream then it will
        be returned (with no additional processing against it and
        `force_object` and `name` are ignored).

        Args:
            force_object(bool): Force conversion of non dicts to JSON objects.
            name(str): Name of the resulting stream.
                When `None` defaults to a generated name.

        .. versionadded:: 1.6.1

        Returns:
            Stream: Stream containing the JSON representations of tuples on this stream.

        """
        force_dict = False
        if isinstance(self.oport.schema, streamsx.topology.schema.StreamSchema):
            func = None
            if self.oport.schema.style != dict:
                force_dict = True
        else:
            func = streamsx.topology.runtime._json_force_object if force_object else None
        saj = self._change_schema(streamsx.topology.schema.CommonSchema.Json, 'as_json', name, func)._layout('AsJson')
        saj.oport.operator.sl = _SourceLocation(_source_info(), 'as_json')
        if force_dict:
            saj.oport.operator.params['pyStyle'] = 'dict'
        return saj

    def _change_schema(self, schema, action, name=None, func=None):
        """Internal method to change a schema.
        """
        if self.oport.schema.schema() == schema.schema():
            return self

        if func is None:
            func = streamsx.topology.functions.identity

        _name = name
        if _name is None:
            _name = action 
        css = self._map(func, schema, name=_name)
        if self._placeable:
            self._colocate(css, action)
        return css

    def _make_placeable(self):
        self._placeable = True
        return self

    def _layout(self, kind=None, hidden=None, name=None, orig_name=None):
        self._op()._layout(kind, hidden, name, orig_name)
        return self

class View(object):
    """
    The View class provides access to a continuously updated sampling of data items on a :py:class:`Stream` after submission.
    A view object is produced by :py:meth:`~Stream.view`, and will access data items from the stream on which it is invoked.

    For example, a `View` object could be created and used as follows:

        >>> topology = Topology()
        >>> rands = topology.source(lambda: iter(random.random, None))
        >>> view = rands.view()       
        >>> submit(ContextTypes.DISTRIBUTED, topology)
        >>> queue = view.start_data_fetch()
        >>> for val in iter(queue.get, 60):
        ...     print(val)
        ...
        0.6527
        0.1963
        0.0512

    """
    def __init__(self, name):
        self.name = name

        self._view_object = None
        self._submit_context = None

    def _initialize_rest(self):
        """Used to initialize the View object on first use.
        """
        if self._submit_context is None:
            raise ValueError("View has not been created.")
        job = self._submit_context._job_access()
        self._view_object = job.get_views(name=self.name)[0]

    def stop_data_fetch(self):
        """Terminates the background thread fetching stream data items.
        """
        if self._view_object:
            self._view_object.stop_data_fetch()
            self._view_object = None

    def start_data_fetch(self):
        """Starts a background thread which begins accessing data from the remote Stream.
        The data items are placed asynchronously in a queue, which is returned from this method.

        Returns:
            queue.Queue: A Queue object which is populated with the data items of the stream.
        """
        self._initialize_rest()
        return self._view_object.start_data_fetch()

    def fetch_tuples(self, max_tuples=20, timeout=None):
        """
        Fetch a number of tuples from this view.

        Fetching of data must have been started with
        :py:meth:`start_data_fetch` before calling this method.

        If ``timeout`` is ``None`` then the returned list will
        contain ``max_tuples`` tuples. Otherwise if the timeout is reached
        the list may contain less than ``max_tuples`` tuples.

        Args:
            max_tuples(int): Maximum number of tuples to fetch.
            timeout(float): Maximum time to wait for ``max_tuples`` tuples.

        Returns:
            list: List of fetched tuples.
        .. versionadded:: 1.12
        """
        return self._view_object.fetch_tuples(max_tuples, timeout)

    def display(self, duration=None, period=2):
        """Display a view within a Jupyter or IPython notebook.

        Provides an easy mechanism to visualize data on a stream
        using a view.

        Tuples are fetched from the view and displayed in a table
        within the notebook cell using a ``pandas.DataFrame``.
        The table is continually updated with the latest tuples from the view.

        This method calls :py:meth:`start_data_fetch` and will call
        :py:meth:`stop_data_fetch` when completed if `duration` is set.

        Args:
            duration(float): Number of seconds to fetch and display tuples. If ``None`` then the display will be updated until :py:meth:`stop_data_fetch` is called.
            period(float): Maximum update period.

        .. note::
            A view is a sampling of data on a stream so tuples that
            are on the stream may not appear in the view.

        .. note::
            Python modules `ipywidgets` and `pandas` must be installed
            in the notebook environment.

        .. warning::
            Behavior when called outside a notebook is undefined.

        .. versionadded:: 1.12
        """
        self._initialize_rest()
        return self._view_object.display(duration, period)


class PendingStream(object):
        """Pending stream connection.

        A pending stream is an initially `disconnected` stream. The `stream` attribute
        can be used as an input stream when the required stream is not yet available. Once the required
        stream is available the connection is made using :py:meth:`complete`.

        The schema of the pending stream is defined by the stream passed into `complete`.

        A simple example is creating a source stream after the filter that will use it::

            # Create the pending or placeholder stream
            pending_source = PendingStream(topology)

            # Create a filter against the placeholder stream
            f = pending_source.stream.filter(lambda : t : t.startswith("H"))

            source = topology.source(['Hello', 'World'])

            # Now complete the connection
            pending_source.complete(source)

        Streams allows feedback loops in its flow graphs, where downstream processing can produce a stream that is
        fed back into the input port of an upstream operator. Typically, feedback loops are
        used to modify the state of upstream transformations, rather than repeat processing of tuples.

        A feedback loop can be created by using a `PendingStream`. The upstream transformation or operator
        that will end the feedback loop uses :py:attr:`~PendingStream.stream` as one of its inputs. A processing
        pipeline is then created and once the downstream starting point of the feedback loop is available,
        it is passed to :py:meth:`complete` to create the loop.

        """
        def __init__(self, topology):
            self.topology = topology
            self._marker = topology.graph.addOperator(kind="$Pending$")
            self._pending_schema = streamsx.topology.schema.StreamSchema(streamsx.topology.schema._SCHEMA_PENDING)

            self.stream = Stream(topology, self._marker.addOutputPort(schema=self._pending_schema))

        def complete(self, stream):
            """Complete the pending stream.

            Any connections made to :py:attr:`stream` are connected to `stream` once
            this method returns.

            Args:
                stream(Stream): Stream that completes the connection.
            """
            assert not self.is_complete()
            self._marker.addInputPort(outputPort=stream.oport)
            self.stream.oport.schema = stream.oport.schema
            # Update the pending schema to the actual schema
            # Any downstream filters that took the reference
            # will be automatically updated to the correct schema
            self._pending_schema._set(self.stream.oport.schema)

            # Mark the operator with the pending stream
            # a start point for graph travesal
            stream.oport.operator._start_op = True

        def is_complete(self):
            """Has this connection been completed.
            """
            return self._marker.inputPorts


class Window(object):
    """Declaration of a window of tuples on a `Stream`.

    A `Window` enables transforms against collection (or window)
    of tuples on a stream rather than per-tuple transforms.
    Windows are created against a stream using :py:meth:`Stream.batch`, :py:meth:`Stream.batchSeconds`
    or :py:meth:`Stream.last`, :py:meth:`Stream.lastSeconds` or :py:meth:`Stream.time_interval`.

    Supported transforms are:

        * :py:meth:`aggregate` - Aggregate the window contents into a single tuple.

    A window is optionally :py:meth:`partitioned <partition>` to create
    independent sub-windows per partition key.
 
    A `Window` can be also passed as the input of an SPL
    operator invocation to indicate the operator's
    input port is windowed.

    Example invoking the SPL `Aggregate` operator with a sliding window of
    the last two minutes, triggering every five tuples::
   
        win = s.last(datetime.timedelta(minutes=2)).trigger(5)

        agg = op.Map('spl.relational::Aggregate', win,
                    schema = 'tuple<uint64 sum, uint64 max>')
        agg.sum = agg.output('Sum(val)')
        agg.max = agg.output('Max(val)')
    """
    def __init__(self, stream, window_type):
        self.topology = stream.topology
        self._hints = stream._hints
        self.stream = stream
        self._config = {'type': window_type}

    def _copy(self):
        wc = Window(self.stream, None)
        wc._config.update(self._config)
        return wc

    def _evict_time_interval(self, interval_duration, creation_period, discard_age, interval_offset):
        self._config['intervalDuration'] = interval_duration
        if creation_period is not None:
           self._config['creationPeriod'] = creation_period
        if discard_age is not None:
           self._config['discardAge'] = discard_age
        if interval_offset is not None:
           self._config['intervalOffset'] = interval_offset

    def _evict_punct(self):
        self._config['evictPolicy'] = 'PUNCTUATION'
        self._config['evictConfig'] = 0

    def _evict_count(self, size):
        self._config['evictPolicy'] = 'COUNT'
        self._config['evictConfig'] = size

    def _evict_count_stv(self, size):
        self._config['evictPolicy'] = 'COUNT'
        self._config['evictConfig'] = size.spl_json()

    def _evict_time_stv(self, duration):
        self._config['evictPolicy'] = 'TIME'
        self._config['evictConfig'] = duration.spl_json()
        self._config['evictTimeUnit'] = 'SECONDS'

    def _evict_time(self, duration):
        self._config['evictPolicy'] = 'TIME'
        self._config['evictConfig'] = int(duration.total_seconds() * 1000.0)
        self._config['evictTimeUnit'] = 'MILLISECONDS'

    def _partition_by_attribute(self, attribute):
        # We cannot always get the list of tuple attributes here
        # because it might be a named type.  Validation of the attribute
        # will be done in code generation.  We only support partition
        # by attribute for StreamSchema (not CommonSchema).
        # Our input schema is the output schema of the previous operator.
        if not isinstance(self.stream.oport.schema, streamsx.topology.schema.StreamSchema):
            raise ValueError("Partition by attribute is supported only for a structured schema")

        self._config['partitioned'] = True
        self._config['partitionBy'] = attribute

    def _partition_by_callable(self, function):
        dilled_callable = None
        
        stateful = _determine_statefulness(function)

        # This is based on graph._addOperatorFunction.
        if isinstance(function, types.LambdaType) and function.__name__ == "<lambda>" :
            function = streamsx.topology.runtime._Callable1(function, no_context=True)
        elif function.__module__ == '__main__':
            # Function/Class defined in main, create a callable wrapping its
            # dill'ed form
            function = streamsx.topology.runtime._Callable1(function,
                no_context = True if inspect.isroutine(function) else None)
         
        if inspect.isroutine(function):
            # callable is a function
            name = function.__name__
        else:
            # callable is a callable class instance
            name = function.__class__.__name__
            # dill format is binary; base64 encode so it is json serializable 
            dilled_callable = base64.b64encode(dill.dumps(function, recurse=None)).decode("ascii")

        self._config['partitioned'] = True
        if dilled_callable is not None:
            self._config['partitionByCallable'] = dilled_callable
        self._config['partitionByName'] = name
        self._config['partitionByModule'] = function.__module__
        self._config['partitionIsStateful'] = bool(stateful)

    def partition(self, key):
        """Declare a window with this window's eviction and trigger policies, and a partition.

        In a partitioned window, a subwindow will be created for each distinct
        value received for the attribute used for partitioning.  Each subwindow
        is treated as if it were a separate window, and each subwindow shares
        the same trigger and eviction policy.

        The key may either be a string containing the name of an attribute,
        or a python callable.

        The `key` parameter may be a string only with a structured schema, 
        and the value of the `key` parameter must be the name of a single
        attribute in the schema.

        The `key` parameter may be a python callable object.  If it is, the
        callable is evaluated for each tuple, and the return from the callable
        determines the partition into which the tuple is placed.  The return
        value must have a ``__hash__`` method.  If checkpointing is enabled, 
        and the callable object has a state, the state of the callable object 
        will be saved and restored in checkpoints.  However, ``__enter__`` and
        ``__exit__`` methods may not be called on the callable object.
        

        Args:
            key: The name of the attribute to be used for partitioning, or
              the python callable object used for partitioning.

        Returns:
            Window: Window that will be triggered.

        .. versionadded:: 1.13
        """

        pw = self._copy()

        # Remove any existing partition.  It will be replaced by the new
        # partition
        for k in {'partitioned','partitionBy','partitionByName','partitionByModule','partitionIsStateful'}:
            pw._config.pop(k, None)

        if callable(key):
            pw._partition_by_callable(key)
        else:
            pw._partition_by_attribute(key)
        return pw

    def trigger(self, when=1):
        """Declare a window with this window's size and a trigger policy.

        When the window is triggered is defined by `when`.

        If `when` is an `int` then the window is triggered every
        `when` tuples.  For example, with ``when=5`` the window
        will be triggered every five tuples.

        If `when` is an `datetime.timedelta` then it is the period
        of the trigger. With a `timedelta` representing one minute
        then the window is triggered every minute.

        By default, when `trigger` has not been called on a `Window`
        it triggers for every tuple inserted into the window
        (equivalent to ``when=1``).

        Args:
            when: The size of the window, either an `int` to define the
                number of tuples or `datetime.timedelta` to define the
                duration of the window.

        Returns:
            Window: Window that will be triggered.

        .. warning:: A trigger is only supported for a sliding window
            such as one created by :py:meth:`last`.
        """
        tw = self._copy();

        if isinstance(when, datetime.timedelta):
            tw._config['triggerPolicy'] = 'TIME'
            tw._config['triggerConfig'] = int(when.total_seconds() * 1000.0)
            tw._config['triggerTimeUnit'] = 'MILLISECONDS'
        elif isinstance(when, int):
            tw._config['triggerPolicy'] = 'COUNT'
            tw._config['triggerConfig'] = when
        else:
            raise ValueError(when)
        return tw

    def aggregate(self, function, name=None):
        """Aggregates the contents of the window when the window is
        triggered.
        
        Upon a window trigger, the supplied function is passed a list containing 
        the contents of the window: ``function(items)``. The order of the window 
        items in the list are the order in which they were each received by the 
        window. If the function's return value is not `None` then the result will
        be submitted as a tuple on the returned stream. If the return value is 
        `None` then no tuple submission will occur.

        For example, a window that calculates a moving average of the
        last 10 tuples could be written as follows::
        
            win = s.last(10).trigger(1)
            moving_averages = win.aggregate(lambda tuples: sum(tuples)/len(tuples))

        When the window is :py:meth:`partitioned <partition>`
        then each partition is triggered and aggregated using
        `function` independently.

        For example, this partitioned window aggregation will independently
        call ``summarize_sensors`` with ten tuples all having the same `id`
        when triggered. Each partition triggers independently so that
        ``summarize_sensors`` is invoked for a specific `id` every time 
        two tuples with that `id` have been inserted into the window partition::
        
            win = s.last(10).trigger(2).partition(key='id')
            moving_averages = win.aggregate(summarize_sensors)

        Example for building a rolling average window aggregation with stream tuples passed as a `named tuple`::
        
            from streamsx.topology.topology import Topology
            from streamsx.topology import context
            from streamsx.topology.context import submit, ContextTypes, ConfigParams
            import random
            import itertools
            from typing import Iterable, NamedTuple

            class AggregateSchema(NamedTuple):
                count: int = 0
                avg: float = 0.0
                min: int = 0
                max: int = 0

            class Average:
                def __call__(self, tuples_in_window) -> AggregateSchema:
                    values = [tpl.value for tpl in tuples_in_window]
                    mn = min(values)
                    mx = max(values)
                    num_of_tuples = len(tuples_in_window)
                    average = sum(values) / len(tuples_in_window)
                    output_event = AggregateSchema(
                        count = num_of_tuples,
                        avg = average,
                        min = mn,
                        max = mx
                    )
                    return output_event

            class NumbersSchema(NamedTuple):
                value: int = 0

            class Numbers(object):
                def __call__(self) -> Iterable[NumbersSchema]:
                    for num in itertools.count(1):
                        yield {"value": num}

            topo = Topology("Rolling Average")
            src = topo.source(Numbers())
            # sliding window with eviction count as submission parameter
            window = src.last(size=topo.create_submission_parameter('count', 10))
            rolling_average = window.aggregate(Average())
        

        .. note:: If a tumbling (:py:meth:`~Stream.batch`) window's stream
            is finite then a final aggregation is performed if the
            window is not empty. Thus ``function`` may be passed fewer tuples
            for a window sized using a count. For example a stream with 105
            tuples and a batch size of 25 tuples will perform four aggregations
            with 25 tuples each and a final aggregation of 5 tuples.


        .. note::
            Punctuation marks are in-band signals that are inserted between tuples in a stream. If sources or stream transforms insert window markers at all, and when they insert them depends on the source or the semantic of the stream transformation. 

            The :py:meth:`~Window.aggregate` inserts a window marker into the output stream after each aggregation.

           
        Args:
            function: The function which aggregates the contents of the window
            name(str): The name of the returned stream. Defaults to a generated name.

        Returns: 
            Stream: A `Stream` of the returned values of the supplied function.

        .. warning::
            In Python 3.5 or later if the stream being aggregated has a
            structured schema that contains a ``blob`` type then any ``blob``
            value will not be maintained in the window. Instead its
            ``memoryview`` object will have been released. If the ``blob``
            value is required then perform a :py:meth:`map` transformation
            (without setting ``schema``) copying any required
            blob value in the tuple using ``memoryview.tobytes()``.

        .. versionadded:: 1.8
        .. versionchanged:: 1.11 Support for aggregation of streams with structured schemas.
        .. versionchanged:: 1.13 Support for partitioned aggregation.
        """

        if self._config is not None:
           if 'type' in self._config:
              if self._config['type'] == 'TIME_INTERVAL':
                 raise TypeError('Time-interval window is not supported.')

        hints = streamsx._streams._hints.check_aggregate(function, self)
        schema = hints.schema if hints else streamsx.topology.schema.CommonSchema.Python
        
        sl = _SourceLocation(_source_info(), "aggregate")
        _name = self.topology.graph._requested_name(name, action="aggregate", func=function)
        stateful = _determine_statefulness(function)

        params = {}
        # if _config contains 'partitionBy', add a parameter 'pyPartitionBy'
        if 'partitionBy' in self._config:
            params['pyPartitionBy'] = self._config['partitionBy']
        if 'partitionByCallable' in self._config:
            params['pyPartitionByCallable'] = self._config['partitionByCallable']
        if 'partitionByName' in self._config:
            params['pyPartitionByName'] = self._config['partitionByName']
            params['pyPartitionByModule'] = self._config['partitionByModule']
            params['pyPartitionIsStateful'] = self._config['partitionIsStateful']
            params['toolkitDir'] = streamsx.topology.param.toolkit_dir()

        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Aggregate", function, name=_name, sl=sl, stateful=stateful, params=params)
            
        op.addInputPort(outputPort=self.stream.oport, window_config=self._config)
        streamsx.topology.schema.StreamSchema._fnop_style(self.stream.oport.schema, op, 'pyStyle')
        oport = op.addOutputPort(schema=schema, name=_name)
        op._layout(kind='Aggregate', name=op.runtime_id, orig_name=name)
        return Stream(self.topology, oport)._make_placeable()._add_hints(hints)


class Sink(_placement._Placement, object):
    """
    Termination of a `Stream`.
    
    A :py:class:`Stream` is terminated by processing that typically
    sends the tuples to an external system.

    .. note:: A `Stream` may have multiple terminations.

    .. seealso:: :py:meth:`~Stream.for_each`, :py:meth:`~Stream.publish`, :py:meth:`~Stream.print`

    .. versionadded:: 1.7
    """
    def __init__(self, op):
        self.__op = op

    def _op(self):
        return self.__op

