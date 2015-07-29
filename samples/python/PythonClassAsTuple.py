from UserDefinedClass.ComplexNumber import ComplexNumber
from topology.Topology import Topology
from topology.context.StreamsContextFactory import StreamsContextFactory, Types

def complexNumberCreator(x):
    """The function that creates the class must be defined in a module and cannot be a lambda, otherwise it will result
    in a class not found error."""
    return ComplexNumber(x + x/2, x - x/2)

if __name__ == "__main__":
    """This example demonstrates how classes can be passed as a tuple on a stream."""
    top = Topology("pythonClassTopology", ["UserDefinedClass", "PythonClassAsTuple.py"])
    count = top.counter()
    complex_stream = count.transform(complexNumberCreator)
    conjugate_stream = complex_stream.transform(lambda r: r.complexConjugate())
    conjugate_stream.printStream()
    StreamsContextFactory.get_streams_context(Types.STANDALONE).submit(top)