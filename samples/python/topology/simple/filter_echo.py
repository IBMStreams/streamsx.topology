import sys
from streamsx.topology.topology import Topology
import streamsx.topology.context
import filter_echo_functions


def main():
    """
    Sample filtering echo topology application. This Python application builds a
    simple topology that echos its command line arguments to standard output.

    This demonstrates use of Python functional logic to filter the tuples.
    A user-defined function implements the filtering logic, in this
    case only echo tuples that start with the letter `d`.

    Args:
        a list of values
        
    Example:
        python3 filter_echo.py cat dog mouse door
    Output:
        dog
        door
    """
    
    topo = Topology("filter_echo")
    source = topo.source(filter_echo_functions.SysArgv(sys.argv[1:]))
    
    # Declare a stream that will execute functional logic
    # against tuples on the echo stream.
    # For each tuple that will appear on echo, the below
    # `starts_with_d` method will be called.  If it returns
    # True then the tuple will appear on the filtered
    # stream, otherwise the tuple is discarded.
    filtered = source.filter(filter_echo_functions.starts_with_d)
    
    filtered.print()
    
    streamsx.topology.context.submit("STANDALONE", topo.graph)
     
if __name__ == '__main__':
    main()
    