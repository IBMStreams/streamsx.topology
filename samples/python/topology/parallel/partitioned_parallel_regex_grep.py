# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import sys
from streamsx.topology.topology import Topology
from streamsx.topology.topology import Routing
import streamsx.topology.context
import partitioned_parallel_regex_grep_functions
import util_functions

def main():
    """
    Sample continuous (streaming) regular expression grep topology application.
    This Python application builds a simple topology that periodically polls a 
    directory for files, reads each file and output lines that match a regular
    expression.
    
    The matching is done on a stream parallelized into 5 parallel channels.
    Tuples will be consistently routed to the same channel based upon their 
    hash value. Each channel of the parallel region only receives tuples that 
    have the same hash value using the built-in hash() function.
    
    For this sample, if you read from a file that contains the following:
    Apple
    Orange
    Banana
    Banana
    Apple
    Apple
    you will notice that the lines containing Apple will always be sent to the 
    same channel of the parallel region.  Similarly, lines containing Banana
    will be sent to the same channel, and lines containing Orange will be
    sent to the same channel.
    
    Args:
        directory (string): a directory that contains files to process
        search_pattern (string): a search pattern
        
    Example:
        * In addition to including the `com.ibm.streamsx.topology/opt/python/packages`
          directory in the PYTHONPATH environment variable, also include the
          `samples/python/topology/simple` directory.
        * Create a subdirectory "dir"
        * Create file3.txt in subdirectory "dir" with the following contents:
          Apple
          Orange
          Banana
          Banana
          Apple
          Apple
        * python3 partitioned_parallel_regex_grep.py dir 'Apple|Banana'
        
    Example Output (intermixed):
          Apple
          Apple
          Apple
          Banana
          Banana

                                                                                          3 filter operators are created:
          1. FilterLine@139865292777904 testing string "Apple" for the pattern.           <== 1 operator processes tuples containing "Apple"
             FilterLine@139865292777904 testing string "Apple" for the pattern.
             FilterLine@139865292777904 testing string "Apple" for the pattern.

          2. FilterLine@139865292777792 testing string "Orange" for the pattern.          <== 1 operator processes tuples containing "Orange"
          
          3. FilterLine@139865298606120 testing string "Banana" for the pattern.          <== 1 operator processes tuples containing "Banana"
             FilterLine@139865298606120 testing string "Banana" for the pattern.
    """
    if len(sys.argv) != 3:
        print("Usage: python3 partitioned_parallel_regex_grep.py <directory> <search_pattern>")
        return
    directory = sys.argv[1]
    pattern = sys.argv[2]
    
    # Define the topology
    topo = Topology("partitioned_parallel_regex_grep")
    
    # Declare a stream with tuples that are string objects
    # All files in a directory are read, resulting in lines of text
    # Each line is a tuple in the stream
    lines = topo.source(util_functions.DirectoryWatcher(directory))
    
    # Parallelize the stream into 5 parallel channels
    # The hash value of the tuple is used to route the tuple to a corresponding 
    # channel, so that all tuples with the same hash value are sent to the same
    # channel.
    lines_parallel = lines.parallel(5, Routing.HASH_PARTITIONED)
    
    # Filter for the matched string, and print the number strings that have
    # been tested. This is happening in parallel.
    filtered_parallel = lines_parallel.filter(partitioned_parallel_regex_grep_functions.FilterLine(pattern))
    
    # Join the results of each parallel filter into one stream,
    # merging the parallel streams back into one stream.
    filtered_condensed = filtered_parallel.end_parallel();
    
    # Print the combined results
    filtered_condensed.print()
    
    # Execute the topology
    streamsx.topology.context.submit("STANDALONE", topo)
     
if __name__ == '__main__':
    main()
