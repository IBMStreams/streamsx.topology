# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
from streamsx.topology.topology import Topology
import streamsx.topology.context
import temperature_sensor_functions

def main():
    """
    Sample temperature sensor topology application.  This Python application builds a 
    simple topology that prints an infinite stream of random numbers to standard
    output.

    The application implements the typical pattern
    of code that declares a topology followed by
    submission of the topology to a Streams context.
               
    Example:
        python3 temperature_sensor.py
    Output:
        ...
        0.3235259780332219
        1.7694181431337437
        0.27741668353194443
        -0.18827948813268522
        0.9576092897071428
        -0.8918033752738117
        -1.4946580133821907
        ...
        (Ctlr-C to exit)
    """
    
    # Create the container for the topology that will hold the streams of tuples.
    topo = Topology("temperature_sensor")
    
    # Declare an infinite stream of random numbers
    source = topo.source(temperature_sensor_functions.readings)
    
    # Sink the stream by printing each of its tuples to standard output
    source.print()
    
    # Now execute the topology by submitting to a standalone context.
    streamsx.topology.context.submit("STANDALONE",topo)
     
if __name__ == '__main__':
    main()
