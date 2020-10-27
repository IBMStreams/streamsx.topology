# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019

from __future__ import print_function
from streamsx.topology.topology import Topology
import streamsx.topology.context
import transform_sample_functions

def main():
    """
    Sample transform application.  This Python application builds a topology that
    * transforms a stream of string tuples from a source operator to a stream of integer tuples 
    * uses `transform` to perform addition on the integer tuples
    * prints the stream to stdout
    * submits the topology in standalone mode (compiles and executes it as a standalone application)
    
    Example:
        > python3 transform_sample.py
    Output:
        342
        474
        9342
    """
    
    # create the container for the topology that will hold the streams
    topo = Topology("transform_sample")
    
    # declare a source stream (`source`) that contains string tuples
    source = topo.source(transform_sample_functions.int_strings_transform)
    
    # transform the stream of string tuples (`source`) to a stream of integer tuples (`i1`)
    i1 = source.map(transform_sample_functions.string_to_int)
    
    # adds 17 to each integer tuple 
    i2 = i1.map(transform_sample_functions.AddNum(17))
    
    # terminate the stream by printing each tuple to stdout
    i2.print()
    
    # execute the application in standalone mode
    streamsx.topology.context.submit("STANDALONE", topo)

if __name__ == '__main__':
    main()
