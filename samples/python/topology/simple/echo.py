# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import sys
from streamsx.topology.topology import Topology
import streamsx.topology.context
import echo_functions


def main():
    """
    Sample echo topology application. This Python application builds a
    simple topology that echoes its command line arguments to standard output.

    The application implements the typical pattern
    of code that declares a topology followed by
    submission of the topology to a Streams context.
    
    Args:
        a list of values to print to stdout
        
    Example:
        python3 echo.py hello1 hello2 hello3
    Output:
        hello1
        hello2
        hello3
    """
    
    topo = Topology("echo")
    # The command line arguments (sys.argv) are captured by the SysArgv
    # callable class and will be used at runtime as the contents of the
    # echo stream.
    echo = topo.source(echo_functions.SysArgv(sys.argv[1:]))
    
    # print the echo stream to stdout
    echo.print()
    
    # At this point the topology is declared with a single
    # stream that is printed to stdout
    
    # execute the topology by submitting to a standalone context
    streamsx.topology.context.submit("STANDALONE", topo.graph)
     
if __name__ == '__main__':
    main()
    
