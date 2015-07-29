__author__ = 'wcmarsha'

from topology.Topology import Topology
from topology.context.StreamsContextFactory import StreamsContextFactory, Types

if __name__ == "__main__":
    """Transformations aren't limited to regular Python functions. Lambda functions may also be used when your logic
    is both stateless and may be performed in a single logical statement."""
    top = Topology("lambdaTopology")
    count = top.counter()
    # Here, a lambda is used to square the counter.
    transformed = count.transform(lambda x: x*x)
    transformed.printStream()
    StreamsContextFactory.get_streams_context(Types.STANDALONE).submit(top)