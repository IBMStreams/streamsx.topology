__author__ = 'wcmarsha'

from topology.Topology import Topology
from topology.context.StreamsContextFactory import StreamsContextFactory, Types


def transformationFunction(tuple):
    """Takes one object as an argument, the tuple, and returns one object as output."""
    output = str(tuple) + " appended by transformationFunction"
    return output

if __name__ == "__main__":
    """This example demonstrates the use of a user-provided transformation function. Notice how this file,
    TransformationExample.py, needs to be added to the list of files included in the application bundle to
    ensure that transformationFunction can be found at runtime"""
    top = Topology("transTopology", ["TransformationExample.py"])
    count = top.counter()
    transformed = count.transform(transformationFunction)
    transformed.printStream()
    StreamsContextFactory.get_streams_context(Types.STANDALONE).submit(top)