from topology.Topology import Topology
from topology.context.StreamsContextFactory import StreamsContextFactory, Types

__author__ = 'wcmarsha'

if __name__ == "__main__":
    """This sample application defines a stream source that creates two tuples "Hello", and "world!".
    The tuples are printed to output."""
    top = Topology("helloTopology")
    stream = top.strings(["Hello", "world!"])
    stream.printStream()
    StreamsContextFactory.get_streams_context(Types.STANDALONE).submit(top)
