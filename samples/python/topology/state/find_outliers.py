# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
from streamsx.topology.topology import Topology
import streamsx.topology.context
import find_outliers_functions

def main():
    """
    Finds outliers from a sequence of floats (e.g. simulating a sensor reading).
    Demonstrates function logic that maintains state across tuples.
    
    Example:
        python3 find_outliers.py
    Example Output:
        2.753064082105016
        -2.210758753960355
        1.9847958795117937
        2.661689193901883
        2.468061723082693
        ...
    """
    topo = Topology("find_outliers")
    
    # Produce a stream of random float values with a normal
    # distribution, mean 0.0 and standard deviation 1.
    values = topo.source(find_outliers_functions.readings)
    

    # Filters the values based on calculating the mean and standard
    # deviation from the incoming data. In this case only outliers are
    # present in the output stream outliers. An outlier is defined as 
    # more than (threshold * standard deviation) from the mean.  The
    # threshold in this example is 2.0.
    # This demonstrates a functional logic class that is
    # stateful. The threshold, sum_x, and sum_x_squared maintain 
    # their values across multiple invocations.
    outliers = values.filter(find_outliers_functions.IsOutlier(2.0))
    
    outliers.print()
    
    streamsx.topology.context.submit("STANDALONE", topo)
     
if __name__ == '__main__':
    main()
