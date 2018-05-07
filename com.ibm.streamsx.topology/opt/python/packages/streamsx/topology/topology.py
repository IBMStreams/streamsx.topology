# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2017

"""
Streaming application definition.

Overview
########

IBM Streams is an advanced analytic platform that allows user-developed
applications to quickly ingest, analyze and correlate information as it
arrives from thousands of real-time sources.
Streams can handle very high data throughput rates, millions of events
or messages per second.

With this API Python developers can build streaming applications
that can be executed using IBM Streams, including the processing
being distributed across multiple computing resources
(hosts or machines) for scalability.

Topology
########

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
that were used in the declaration of the application. For example
the Python module containing the callable used in a
:py:meth:`~Stream.map` invocation. These modules are copied into
the bundle from their local location. This allows the bundle to
be self-contained, and thus not the Streams instance have all the required
Python packages pre-installed. The addition of packages to the bundle
can be controlled with :py:attr:`Topology.include_packages` and
:py:attr:`Topology.exclude_packages`.

The Streams runtime distributes the application's operations
across the resources available in the instance.

.. note::
    `Topology` represents a declaration of a streaming application that
    will be executed by a Streams instance as a `job`, either using the Streaming Analytics
    service on IBM Cloud or an on-premises distributed instance.
    `Topology` does not represent a running application, so an instance of `Stream` class does not contain
    the tuples, it is only a declaration of a stream.

Stream
######

A :py:class:`Stream` can be an infinite sequence of tuples, such as a stream for a traffic flow sensor.
Alternatively, a stream can be finite, such as a stream that is created from the contents of a file.
When a streams processing application contains infinite streams, the application runs continuously without ending.

A stream has a schema that defines the type of each tuple on the stream.
The schema for a Python Topology is either:

* :py:const:`~streamsx.topology.schema.CommonSchema.Python` - A tuple may be any Python object. This is the default.
* :py:const:`~streamsx.topology.schema.CommonSchema.String` - Each tuple is a Unicode string.
* :py:const:`~streamsx.topology.schema.CommonSchema.Binary` - Each tuple is a blob.
* :py:const:`~streamsx.topology.schema.CommonSchema.Json` - Each tuple is a Python dict that can be expressed as a JSON object.
* Structured - A stream that has a structured schema of a ordered list of attributes, with each attribute having a fixed type (e.g. float64 or int32) and a name. The schema of a structured stream is defined using :py:const:`~streamsx.topology.schema.StreamSchema`.

Stream processing
#################

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
    pywords = words.filter(lambda word : tuple.startswith('py'))

Stateful operations
===================

Use of a class instance allows the operation to be stateful by maintaining state in instance
attributes across invocations.

.. note::
    For future compatibility instances should ensure that the object's
    state can be pickled. See https://docs.python.org/3.5/library/pickle.html#handling-stateful-objects

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
 * a data conversion exception is raised converting a value to an structutured schema tuple or attribute

If ``__exit__`` returns a true value then the exception is suppressed and processing continues, otherwise the enclosing processing element will be terminated.

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

Module contents
###############

"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future.builtins import *
try:
    from future import standard_library
    standard_library.install_aliases()
except (ImportError,NameError):
    pass

import copy
import random
import streamsx._streams._placement as _placement
import streamsx.spl.op
import streamsx.spl.types
import streamsx.topology.graph
import streamsx.topology.schema
import streamsx.topology.functions
import streamsx.topology.runtime
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
        """Internal method."""
        return streamsx.spl.op.Expression.expression('com.ibm.streamsx.topology.topic::' + self.name).spl_json()

class Topology(object):
    """The Topology class is used to define data sources, and is passed as a parameter when submitting an application.
       Topology keeps track of all sources, sinks, and data operations within your application.

       Submission of a Topology results in a Streams application that has
       the name `namespace::name`.

       Arguments:
           name(str): Name of the topology. Defaults to a name dervied
              from the calling evironment if it can be determined, otherwise
              a random name.
           namespace(str): Namespace of the topology. Defaults to a name dervied
              from the calling evironment if it can be determined, otherwise
              a random name.

       Attributes:
           include_packages(set[str]): Python package names to be included in the built application. Any package in this list is copied into the bundle and made available at runtime to the Python callables used in the application. By default a ``Topology`` will automatically discover which packages and modules are required to be copied, this field may be used to add additional packages that were not automatically discovered.

           exclude_packages(set[str]): Python top-level package names to be excluded from the built application. Excluding a top-level packages excludes all sub-modules at any level in the package, e.g. `sound` excludes `sound.effects.echo`. Only the top-level package can be defined, e.g. `sound` rather than `sound.filters`. Behavior when adding a module within a package is undefined.
               When compiling the application using Anaconda this set is pre-loaded with Python packages from the Anaconda pre-loaded set.

               Package names in `include_packages` take precedence over package names in `exclude_packages`.
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
        
        if sys.version_info.major == 3:
          self.opnamespace = "com.ibm.streamsx.topology.functional.python"
        elif sys.version_info.major == 2 and sys.version_info.minor == 7:
          self.opnamespace = "com.ibm.streamsx.topology.functional.python2"
        else:
          raise ValueError("Python version not supported.")
        self.include_packages = set() 
        self.exclude_packages = set() 
        self._pip_packages = list() 
        self._files = dict()
        if "Anaconda" in sys.version:
            import streamsx.topology.condapkgs
            self.exclude_packages.update(streamsx.topology.condapkgs._CONDA_PACKAGES)
        import streamsx.topology._deppkgs
        self.exclude_packages.update(streamsx.topology._deppkgs._DEP_PACKAGES)
        
        self.graph = streamsx.topology.graph.SPLGraph(self, name, namespace)
        self._submission_parameters = dict()

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
        """
        _name = name
        if inspect.isroutine(func):
            pass
        elif callable(func):
            pass
        else:
            if _name is None:
                _name = type(func).__name__
            func = streamsx.topology.runtime._IterableInstance(func)

        sl = _SourceLocation(_source_info(), "source")
        _name = self.graph._requested_name(_name, action='source', func=func)
        op = self.graph.addOperator(self.opnamespace+"::Source", func, name=_name, sl=sl)
        op._layout(kind='Source', name=_name, orig_name=name)
        oport = op.addOutputPort(name=_name)
        return Stream(self, oport)._make_placeable()

    def subscribe(self, topic, schema=streamsx.topology.schema.CommonSchema.Python, name=None, connect=None, buffer_capacity=None, buffer_full_policy=None):
        """
        Subscribe to a topic published by other Streams applications.
        A Streams application may publish a stream to allow other
        Streams applications to subscribe to it. A subscriber matches a
        publisher if the topic and schema match.

        By default a stream is subscribed as :py:const:`~streamsx.topology.schema.CommonSchema.Python` objects
        which connects to streams published to topic by Python Streams applications.

        JSON streams are subscribed to using schema :py:const:`~streamsx.topology.schema.CommonSchema.Json`.
        Each tuple on the returned stream will be a Python dictionary
        object created by ``json.loads(tuple)``.
        A Streams application publishing JSON streams may have been implemented in any programming language
        supported by Streams.
       
        String streams are subscribed to using schema :py:const:`~streamsx.topology.schema.CommonSchema.String`.
        Each tuple on the returned stream will be a Python string object.
        A Streams application publishing JSON streams may have been implemented in any programming language
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
        _name = self.graph._requested_name(name, 'subscribe')
        sl = _SourceLocation(_source_info(), "subscribe")
        op = self.graph.addOperator(kind="com.ibm.streamsx.topology.topic::Subscribe", sl=sl, name=_name)
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

    def add_pip_package(self, requirement):
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
        build machines. This is always true for the Streaming
        Analytics service on IBM Cloud.

        The project name extracted from the requirement
        specifier is added to :py:attr:`~exclude_packages`
        to avoid the package being added by the dependency
        resolver. Thus the package should be added before
        it is used in any stream transformation.

        Example::

            topo = Topology()
            # Add dependency on pint package
            # and astral at version 0.8.1
            topo.add_pip_package('pint')
            topo.add_pip_package('astral==0.8.1')
        
        Args:
            requirement(str): Package requirements specifier.

        .. warning::
            Only supported when using the remote build service with
            the Streaming Analytics service.

        .. versionadded:: 1.9
        """
        self._pip_packages.append(str(requirement))
        pr = pkg_resources.Requirement.parse(requirement) 
        self.exclude_packages.add(pr.project_name)

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

        .. note::
            The parameter (value returned from this method) is only
            supported within a lambda expression or a callable
            that is not a function.

        The default type of a submission parameter's value is a `str`
        (`unicode` on Python 2.7). When a `default` is specified
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
        """
        
        if name in self._submission_parameters:
            raise ValueError("Submission parameter {} already defined.".format(name))
        sp = streamsx.topology.runtime._SubmissionParam(name, default, type_)
        self._submission_parameters[name] = sp
        return sp

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


class Stream(_placement._Placement, object):
    """
    The Stream class is the primary abstraction within a streaming application. It represents a potentially infinite 
    series of tuples which can be operated upon to produce another stream, as in the case of :py:meth:`map`, or
    terminate a stream, as in the case of :py:meth:`for_each`.
    """
    def __init__(self, topology, oport):
        self.topology = topology
        self.oport = oport
        self._placeable = False
        self._alias = None

    def _op(self):
        if not self._placeable:
            raise TypeError()
        return self.oport.operator

    @property
    def name(self):
        """
        Name of the stream.

        Returns:
            str: Name of the stream.

        .. seealso:: :py:meth:`aliased_as`
        """
        return self._alias if self._alias else self.oport.name

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

    def for_each(self, func, name=None):
        """
        Sends information as a stream to an external system.

        For each tuple `t` on the stream ``func(t)`` is called.
        
        Args:
            func: A callable that takes a single parameter for the tuple and returns None.
            name(str): Name of the stream, defaults to a generated name.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no further processing occurs for the
        input tuple that caused the exception.

        Returns:
            streamsx.topology.topology.Sink: Stream termination.

        .. versionchanged:: 1.7
            Now returns a :py:class:`Sink` instance.
        """
        sl = _SourceLocation(_source_info(), 'for_each')
        _name = self.topology.graph._requested_name(name, action='for_each', func=func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::ForEach", func, name=_name, sl=sl)
        op.addInputPort(outputPort=self.oport, name=self.name)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        op._layout(kind='ForEach', name=_name, orig_name=name)
        return Sink(op)

    def sink(self, func, name=None):
        """
        Equivalent to calling :py:meth:`for_each`.
        
        .. deprecated:: 1.7
            Replaced by :py:meth:`for_each`.
        """
        return self.for_each(func, name)

    def filter(self, func, name=None):
        """
        Filters tuples from this stream using the supplied callable `func`.

        For each tuple on the stream ``func(tuple)`` is called, if the return evaluates to ``True`` the
        tuple will be present on the returned stream, otherwise the tuple is filtered out.
        
        Args:
            func: Filter callable that takes a single parameter for the tuple.
            name(str): Name of the stream, defaults to a generated name.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuple is submitted to the filtered
        stream corresponding to the input tuple that caused the exception.

        Returns:
            Stream: A Stream containing tuples that have not been filtered out.
        """
        sl = _SourceLocation(_source_info(), 'filter')
        _name = self.topology.graph._requested_name(name, action="filter", func=func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Filter", func, name=_name, sl=sl)
        op.addInputPort(outputPort=self.oport, name=self.name)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        op._layout(kind='Filter', name=_name, orig_name=name)
        oport = op.addOutputPort(schema=self.oport.schema, name=_name)
        return Stream(self.topology, oport)._make_placeable()

    def _map(self, func, schema, name=None):
        _name = self.topology.graph._requested_name(name, action="map", func=func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Map", func, name=_name)
        op.addInputPort(outputPort=self.oport, name=self.name)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        oport = op.addOutputPort(schema=schema, name=_name)
        op._layout(name=_name, orig_name=name)
        return Stream(self.topology, oport)._make_placeable()

    def view(self, buffer_time = 10.0, sample_size = 10000, name=None, description=None, start=False):
        """
        Defines a view on a stream.

        A view is a continually updated sampled buffer of a streams's tuples.
        Views allow visibility into a stream from external clients such
        as the Streams console,
        `Microsoft Excel <https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.excel.doc/doc/excel_overview.html>`_ or REST clients.

        The view created by this method can be used by external clients
        and through the returned object after the topology is submitted. 

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
            View object which can be used to access the data when the
            topology is submitted.
        """
        if name is None:
            name = ''.join(random.choice('0123456789abcdef') for x in range(16))

        if self.oport.schema == streamsx.topology.schema.CommonSchema.Python:
            view_stream = self.as_json(force_object=False)._layout(hidden=True)
            # colocate map operator with stream that is being viewed.
            self._colocate(view_stream, 'view')
        else:
            view_stream = self

        port = view_stream.oport.name
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
        self.topology.graph.get_views().append(_view)
        return _view

    def map(self, func, name=None, schema=None):
        """
        Maps each tuple from this stream into 0 or 1 stream tuples.

        For each tuple on this stream ``result = func(tuple)`` is called.
        If `result` is not `None` then the result will be submitted
        as a tuple on the returned stream. If `result` is `None` then
        no tuple submission will occur.

        By default the submitted tuple is ``result`` without modification
        resulting in a stream of picklable Python objects. Setting the
        `schema` parameter changes the type of the stream and
        modifies each ``result`` before submission.

        * :py:const:`~streamsx.topology.schema.CommonSchema.Python` - The default:  `result` is submitted.
        * :py:const:`~streamsx.topology.schema.CommonSchema.String` - A stream of strings: ``str(result)`` is submitted.
        * :py:const:`~streamsx.topology.schema.CommonSchema.Json` - A stream of JSON objects: ``result`` must be convertable to a JSON object using `json` package.
        * :py:const:`~streamsx.topology.schema.StreamSchema` - A structured stream. `result` must be a `dict` or (Python) `tuple`. When a `dict` is returned the outgoing stream tuple attributes are set by name, when a `tuple` is returned stream tuple attributes are set by position.

        Args:
            func: A callable that takes a single parameter for the tuple.
            name(str): Name of the mapped stream, defaults to a generated name.
            schema(StreamSchema): Schema of the resulting stream.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuple is submitted to the mapped
        stream corresponding to the input tuple that caused the exception.
       

        Returns:
            Stream: A stream containing tuples mapped by `func`.

        .. versionadded:: 1.7 `schema` argument added to allow conversion to
            a structured stream.
        .. versionadded:: 1.8 Support for submitting `dict` objects as stream tuples to a structured stream (in addition to existing support for `tuple` objects).
        """
        if schema is None:
            schema = streamsx.topology.schema.CommonSchema.Python
     
        ms = self._map(func, schema=schema, name=name)._layout('Map')
        ms.oport.operator.sl = _SourceLocation(_source_info(), 'map')
        return ms

    def transform(self, func, name=None):
        """
        Equivalent to calling :py:meth:``map(func, name)``.

        .. deprecated:: 1.7
            Replaced by :py:meth:`map`.
        """
        return self.map(func, name)
             
    def flat_map(self, func, name=None):
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
            name(str): Name of the flattened stream, defaults to a generated name.

        If invoking ``func`` for a tuple on the stream raises an exception
        then its processing element will terminate. By default the processing
        element will automatically restart though tuples may be lost.

        If ``func`` is a callable object then it may suppress exceptions
        by return a true value from its ``__exit__`` method. When an
        exception is suppressed no tuples are submitted to the flattened
        and mapped stream corresponding to the input tuple
        that caused the exception.

        Returns:
            Stream: A Stream containing flattened and mapped tuples.
        Raises:
            TypeError: if `func` does not return an iterator nor None
        """     
        sl = _SourceLocation(_source_info(), 'flat_map')
        _name = self.topology.graph._requested_name(name, action='flat_map', func=func)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::FlatMap", func, name=_name, sl=sl)
        op.addInputPort(outputPort=self.oport, name=self.name)
        streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, op, 'pyStyle')
        oport = op.addOutputPort(name=_name)
        return Stream(self.topology, oport)._make_placeable()._layout('FlatMap', name=_name, orig_name=name)
    
    def multi_transform(self, func, name=None):
        """
        Equivalent to calling :py:meth:`flat_map`.

        .. deprecated:: 1.7
            Replaced by :py:meth:`flat_map`.
        """
        return self.flat_map(func, name)

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
        return Stream(self.topology, oport)

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
        # include_packages=self.include_packages, exclude_packages=self.exclude_packages)
        # include_packages=self.include_packages, exclude_packages=self.exclude_packages)
        op.addInputPort(outputPort=self.oport)
        oport = op.addOutputPort(schema=self.oport.schema)
        return Stream(self.topology, oport)

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
        return Stream(self.topology, oport)
    
    def parallel(self, width, routing=Routing.ROUND_ROBIN, func=None, name=None):
        """
        Parallelizes the stream into `width` parallel channels.
        Tuples are routed to parallel channels such that an even distribution is maintained.
        Each parallel channel can be thought of as being assigned its own thread.
        As such, each parallelized stream function are separate instances and operate independently 
        from one another.
        
        parallel() will only parallelize the stream operations performed after the call to parallel() and 
        before the call to :py:meth:`~Stream.end_parallel`.
        
        Parallel regions aren't required to have an output stream, and thus may be used as sinks.
        In other words, a parallel sink is created by calling parallel() and creating a sink operation.
        It is not necessary to invoke end_parallel() on parallel sinks.
        
        Every call to end_parallel() must have a call to parallel() preceding it.
        
        Args:
            width (int): Degree of parallelism.
            routing(Routing): denotes what type of tuple routing to use.
            func: Optional function called when :py:const:`Routing.HASH_PARTITIONED` routing is specified.
                The function provides an integer value to be used as the hash that determines
                the tuple channel routing.
            name (str): The name to display for the parallel region.

        Returns:
            Stream: A stream for which subsequent transformations will be executed in parallel.

        """
        _name = name
        if _name is None:
            _name = self.name
            
        _name = self.topology.graph._requested_name(name, action='parallel', func=func)

        if routing is None or routing == Routing.ROUND_ROBIN or routing == Routing.BROADCAST:
            op2 = self.topology.graph.addOperator("$Parallel$", name=_name)
            op2.addInputPort(outputPort=self.oport)
            if routing == Routing.BROADCAST:
                oport = op2.addOutputPort(width, schema=self.oport.schema, routing="BROADCAST")
            else:
                oport = op2.addOutputPort(width, schema=self.oport.schema, routing="ROUND_ROBIN")

            return Stream(self.topology, oport)
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
                hash_adder = self.topology.graph.addOperator(self.topology.opnamespace+"::HashAdder", func)
                hash_adder._layout(hidden=True)
                hash_schema = self.oport.schema.extend(streamsx.topology.schema.StreamSchema("tuple<int64 __spl_hash>"))
                hash_adder.addInputPort(outputPort=self.oport, name=self.name)
                streamsx.topology.schema.StreamSchema._fnop_style(self.oport.schema, hash_adder, 'pyStyle')
                parallel_input = hash_adder.addOutputPort(schema=hash_schema)

            parallel_op = self.topology.graph.addOperator("$Parallel$", name=_name)
            parallel_op.addInputPort(outputPort=parallel_input)
            parallel_op_port = parallel_op.addOutputPort(oWidth=width, schema=parallel_input.schema, partitioned_keys=keys, routing="HASH_PARTITIONED")

            if func is not None:
                # use the Functor passthru operator to remove the hash attribute by removing it from output port schema
                hrop = self.topology.graph.addPassThruOperator()
                hrop._layout(hidden=True)
                hrop.addInputPort(outputPort=parallel_op_port)
                parallel_op_port = hrop.addOutputPort(schema=self.oport.schema)

            return Stream(self.topology, parallel_op_port)
        else :
            raise TypeError("Invalid routing type supplied to the parallel operator")    

    def end_parallel(self):
        """
        Ends a parallel region by merging the channels into a single stream.

        Returns:
            Stream: Stream for which subsequent transformations are no longer parallelized.
        """
        lastOp = self.topology.graph.getLastOperator()
        outport = self.oport
        if (isinstance(lastOp, streamsx.topology.graph.Marker)):
            if (lastOp.kind == "$Union$"):
                pto = self.topology.graph.addPassThruOperator()
                pto.addInputPort(outputPort=self.oport)
                outport = pto.addOutputPort(schema=self.oport.schema)
        op = self.topology.graph.addOperator("$EndParallel$")
        op.addInputPort(outputPort=outport)
        oport = op.addOutputPort(schema=self.oport.schema)
        endP = Stream(self.topology, oport)
        return endP

    def set_parallel(self, width):
        """
        Indicates that the stream is the start of a parallel region. Should only be invoked on source operators.
        Args:
            width: The degree of parallelism for the parallel region.

        Returns:
            Stream: Returns this stream.

        .. versionadded:: 1.9
        """
        self.oport.operator.config['parallel'] = True
        self.oport.operator.config['width'] = width
        return self

    def last(self, size=1):
        """ Declares a window containing most recent tuples on this stream.

        The number of tuples maintained in the window is defined by `size`.

        If `size` is an `int` then it is the count of tuples in the window.
        For example, with ``size=10`` the window always contains the
        last (most recent) ten tuples.

        If `size` is an `datetime.timedelta` then it is the duration
        of the window. With a `timedelta` representing five minutes
        then the window contains any tuples that arrived in the last
        five minutes.
 
        Args:
            size: The size of the window, either an `int` to define the
                number of tuples or `datetime.timedelta` to define the
                duration of the window.

        Returns:
            Window: Window of the last (most recent) tuples on this stream.
        """
        win = Window(self, 'SLIDING')
        if isinstance(size, datetime.timedelta):
            win._evict_time(size)
        elif isinstance(size, int):
            win._evict_count(size)
        else:
            raise ValueError(size)
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

    def print(self, tag=None, name=None):
        """
        Prints each tuple to stdout flushing after each tuple.

        If `tag` is not `None` then each tuple has "tag: " prepended
        to it before printing.

        Args:
            tag: A tag to prepend to each tuple.
            name(str): Name of the resulting stream.
                When `None` defaults to a generated name.
        Returns:
            streamsx.topology.topology.Sink: Stream termination.

        .. versionadded:: 1.6.1 `tag`, `name` parameters.

        .. versionchanged:: 1.7
            Now returns a :py:class:`Sink` instance.
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
        :py:const:`~streamsx.topology.schema.CommonSchema.Json`
        then it is published as a stream of JSON objects.
        Other Streams applications may subscribe to it regardless
        of their implementation language.

        If a stream is published setting `schema` to
        :py:const:`~streamsx.topology.schema.CommonSchema.String`
        then it is published as strings
        Other Streams applications may subscribe to it regardless
        of their implementation language.

        Supported values of `schema` are only
        :py:const:`~streamsx.topology.schema.CommonSchema.Json`
        and
        :py:const:`~streamsx.topology.schema.CommonSchema.String`.

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
        if schema is not None and self.oport.schema.schema() != schema.schema():
            nc = None
            if schema == streamsx.topology.schema.CommonSchema.Json:
                schema_change = self.as_json()
            elif schema == streamsx.topology.schema.CommonSchema.String:
                schema_change = self.as_string()
            else:
                raise ValueError(schema)
               
            if self._placeable:
                self._colocate(schema_change, 'publish')
            sp = schema_change.publish(topic, schema=schema, name=name)
            sp._op().sl = sl
            return sp

        _name = self.topology.graph._requested_name(name, action="publish")
        op = self.topology.graph.addOperator("com.ibm.streamsx.topology.topic::Publish", params={'topic': topic}, sl=sl, name=_name)
        op.addInputPort(outputPort=self.oport)
        op._layout_group('Publish', name if name else _name)
        sink = Sink(op)

        if self._placeable:
            self._colocate(sink, 'publish')
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
        return Stream(self.topology, oport)

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
        return sas

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
        func = streamsx.topology.runtime._json_force_object if force_object else None
        saj = self._change_schema(streamsx.topology.schema.CommonSchema.Json, 'as_json', name, func)._layout('AsJson')
        saj.oport.operator.sl = _SourceLocation(_source_info(), 'as_json')
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
    The View class provides access to a continuously updated sampling of data items on a Stream after submission.
    A view object is produced by the view method, and will access data items from the stream on which it is invoked.

    For example, a View object could be created and used as follows:

        >>> topology = Topology()
        >>> rands = topology.source(lambda: random.random())
        >>> view = rands.view()       
        >>> submit(ContextTypes.DISTRIBUTED, topology)
        >>> queue = view.start_data_fetch()
        >>> for val in iter(queue.get, None):
        ... print(val)
        ...
        0.6527
        0.1963
        0.0512

    """
    def __init__(self, name):
        self.name = name

        self._view_object = None
        self._submit_context = None
        self._streams_connection = None

    def initialize_rest(self):
        """Used to initialize the View object on first use.
        """
        if self._streams_connection is None:
            if self._submit_context is None:
                raise ValueError("View has not been created.")

            self._streams_connection = self._submit_context.streams_connection()

    def stop_data_fetch(self):
        """Terminates the background thread fetching stream data items.
        """
        self._view_object.stop_data_fetch()

    def start_data_fetch(self):
        """Starts a background thread which begins accessing data from the remote Stream.
        The data items are placed asynchronously in a queue, which is returned from this method.

        Returns:
            A Queue object which is populated with the data items of the stream.
        """
        self.initialize_rest()
        sc = self._streams_connection
        instance = sc.get_instance(id=self._submit_context.submission_results['instanceId'])
        job = instance.get_job(id=self._submit_context.submission_results['jobId'])
        self._view_object = job.get_views(name=self.name)[0]

        return self._view_object.start_data_fetch()


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

    A `Window` can be passed as the input of an SPL
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
        self.stream = stream
        self._config = {'type': window_type}

    def _evict_count(self, size):
        self._config['evictPolicy'] = 'COUNT'
        self._config['evictConfig'] = size

    def _evict_time(self, duration):
        self._config['evictPolicy'] = 'TIME'
        self._config['evictConfig'] = int(duration.total_seconds() * 1000.0)
        self._config['evictTimeUnit'] = 'MILLISECONDS'

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
    """
        tw = Window(self.stream, self._config['type'])
        tw._config['evictPolicy'] = self._config['evictPolicy']
        tw._config['evictConfig'] = self._config['evictConfig']
        if self._config['evictPolicy'] == 'TIME':
            tw._config['evictTimeUnit'] = 'MILLISECONDS'

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
        `None` then no tuple submission will occur. For example, a window that 
        calculates a moving average of the last 10 tuples could be written as follows::
        
            win = s.last(10).trigger(1)
            moving_averages = win.aggregate(lambda tuples: sum(tuples)/len(tuples))
            
        Args:
            function: The function which aggregates the contents of the window
            name(str): The name of the returned stream. Defaults to a generated name.

        Returns: 
            Stream: A `Stream` of the returned values of the supplied function.                                                                                                                                                             
        .. versionadded:: 1.8
        """
        schema = streamsx.topology.schema.CommonSchema.Python
        
        sl = _SourceLocation(_source_info(), "aggregate")
        name = self.topology.graph._requested_name(name, action="aggregate", func=function)
        op = self.topology.graph.addOperator(self.topology.opnamespace+"::Aggregate", function, name=name, sl=sl)
        op.addInputPort(outputPort=self.stream.oport, name=self.stream.name, window_config=self._config)
        oport = op.addOutputPort(schema=schema, name=name)

        return Stream(self.topology, oport)._make_placeable()


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
