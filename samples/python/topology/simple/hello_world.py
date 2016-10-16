# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import sys
from streamsx.topology.topology import Topology
import streamsx.topology.context

def main():
    """
    Sample Hello World topology application. This Python application builds a
    simple topology that prints Hello World to standard output.

    The application implements the typical pattern
    of code that declares a topology followed by
    submission of the topology to a Streams context.
    
    This demonstrates the mechanics of declaring a topology and executing it.
            
    Example:
        python3 hello_world.py
    Output:
        Hello
        World!
    """
    
    # Create the container for the topology that will hold the streams of tuples.
    topo = Topology("hello_world")
    
    # Declare a source stream (hw) with string tuples containing two tuples,
    # "Hello" and "World!".
    hw = topo.source(["Hello", "World!"])
    
    # Sink hw by printing each of its tuples to standard output
    hw.print()
    
    # At this point the topology is declared with a single
    # stream that is printed to standard output
    
    # Now execute the topology by submitting to a standalone context.
    streamsx.topology.context.submit("STANDALONE", topo)

if __name__ == '__main__':
    main()
