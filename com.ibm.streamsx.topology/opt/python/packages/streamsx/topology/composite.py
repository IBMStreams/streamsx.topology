# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019,2020

"""
Composite transformations.

.. versionadded:: 1.14
"""


__all__ = [ 'Source', 'Map', 'ForEach']

import streamsx._streams._version
__version__ = streamsx._streams._version.__version__

from abc import ABC, abstractmethod
from typing import Optional
import streamsx.typing
import streamsx.topology.topology

class Composite(ABC):
    """
    Composite transformations support a single logical transformation
    being a composite of one or more basic transformations.

    A composite transformation is implemented as a sub-class
    of :py:class:`Source`, :py:class:`Map` or :py:class:`ForEach`
    whose ``populate`` method populates the topology with the
    required basic transformations. For example a ``Source`` composite
    might have use :py:meth:`~Topology.source` followed by a
    :py:meth:`~Stream.filter` to filter out unwanted events
    and then a :py:meth:`~Stream.map` to parse the event into
    a structured schema.

    Composites may use other composites during ``populate``.

    """
    def __init__(self):
        self.group = True
        ns = self.__class__.__module__
        if ns == '__main__':
            self.kind = self.__class__.__name__
        else:
            self.kind = ns + '::' + self.__class__.__name__

    @classmethod
    def _check_type(cls, obj, type_):
        if not isinstance(obj, type_):
            raise TypeError()

    def _set_group(self, name):
        if self.group:
            pass


class Source(Composite):
    """
    Abstract composite source.

    An instance of a subclass can be passed to :py:meth:`~Topology.source`
    to create a source stream that is composed of one or more basic transformations.

    Example assuming ``RawTweets`` is Python iterable that produces
    raw tweets::

        class Tweets(streamsx.topology.composite.Source):
            def __init__(self, track):
                self.track = track
     
            def populate(self, topology, name, **options):
                # get all the tweets
                tweets = topology.source(RawTweets(track=self.track), name=name)
                # filter so that only with a message are returned
                return tweets.filter(lambda tweet : tweet['text'])

    This class can then be used as follows::

        topo = Topology()
        gf_tweets = topo.source(Tweets(track=['glutenfree', 'gf']))

    """

    def _add(self, topology, name, **options):
        s = self.populate(topology, name, **options)
        Source._check_type(s, streamsx.topology.topology.Stream)
        return s

    @abstractmethod
    def populate(self, topology:streamsx.topology.topology.Topology, name:Optional[str], **options) -> streamsx.topology.topology.Stream:
        """
        Populate the topology with this composite source.

        Args:
            topology: Topology containing the source.
            name: Name passed into ``source``.
            **options: Future options passed to ``source``.

        Returns:
            Stream: Single stream representing the source.
        """
        pass


class Map(Composite):
    """
    Abstract composite map transformation.

    An instance of a subclass can be passed to :py:meth:`~streamsx.topology.topology.Stream.map`
    to create a stream that is composed of one or more basic transformations
    of an input stream.

    Example::

        class WordCount(streamsx.topology.composite.Map):
            def __init__(self, period, update):
                self.period = period
                self.update = update
     
            def populate(self, topology, stream, schema, name, **options):
                words = stream.flat_map(lambda line : line.split())
                win = words.last(size=self.period).trigger(self.update).partition(lambda s : s)
                return win.aggregate(lambda values : (values[0], len(values)))
    """

    def _add(self, stream, schema, name, **options):
        s = self.populate(stream.topology, stream, schema, name, **options)
        Map._check_type(s, streamsx.topology.topology.Stream)
        if schema:
            s = s.map(schema=schema)
        return s

    @abstractmethod
    def populate(self, topology:streamsx.topology.topology.Topology, stream:streamsx.topology.topology.Stream, schema:streamsx.typing.Schema, name:Optional[str], **options) -> streamsx.topology.topology.Stream:
        """
        Populate the topology with this composite map transformation.

        Args:
            topology: Topology containing the composite map.
            stream: Stream to be transformed.
            schema: Schema passed into ``map``.
            name: Name passed into ``map``.
            **options: Future options passed to ``map``.

        Returns:
            Stream: Single stream representing the transformation of `stream`.
        """
        pass

class ForEach(Composite):
    """
    Abstract composite for each transformation.

    An instance of a subclass can be passed to :py:meth:`~streamsx.topology.topology.Stream.for_each` to create a sink (stream termination) that is
    composed of one or more basic transformations of an input stream.
    """

    def _add(self, stream, name, **options):
        s = self.populate(stream.topology, stream, name, **options)
        ForEach._check_type(s, streamsx.topology.topology.Sink)
        return s

    @abstractmethod
    def populate(self, topology:streamsx.topology.topology.Topology, stream:streamsx.topology.topology.Stream, name:Optional[str], **options) -> streamsx.topology.topology.Sink:
        """
        Populate the topology with this composite for each transformation.

        Args:
            topology: Topology containing the composite map.
            stream: Stream to be transformed.
            name: Name passed into ``for_each``.
            **options: Future options passed to ``for_each``.

        Returns:
            ~streamsx.topology.topology.Sink: Termination for this composite transformation of `stream`.
        """
        pass
