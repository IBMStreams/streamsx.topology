# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import sys
from streamsx.topology.topology import Topology
import streamsx.topology.context
import parallel_regex_grep_functions
import util_functions

def main():
    """
    Sample continuous (streaming) regular expression grep topology application.
    This Python application builds a simple topology that periodically polls a 
    directory for files, reads each file and output lines that match a regular
    expression.
    The matching is done on a stream parallelized into 5 parallel channels.
    Tuples are routed to parallel channels such that an even distribution is
    maintained.
    
    Args:
        directory (string): a directory that contains files to process
        search_pattern (string): a search pattern
        
    Example:
        * In addition to including the `com.ibm.streamsx.topology/opt/python/packages`
          directory in the PYTHONPATH environment variable, also include the
          `samples/python/topology/simple` directory.
        * Create a subdirectory "dir"
        * Create file1.txt in subdirectory "dir" with the following contents:
            file1 line1
            file1 line2
            file1 line3
        * Create file2.txt in subdirectory "dir" with the following contents:
            file2 line1
            file2 line2
            file2 line3
        * python3 parallel_regex_grep.py dir line[1-2]
        
    Example Output (intermixed):
        file2 line1
        file2 line2
        file1 line1
        file1 line2
        
        LineCounter@139676451944432 has sent ...
        LineCounter@139676451944432 has sent 6 lines to be filtered.                   <== The source operator produced a total of 6 tuples
        
        1. FilterLine@139676451362072 has received 1 lines on this parallel channel.   <== 5 filter operators are created, one for each parallel channel.
        2. FilterLine@139676441656064 has received 1 lines on this parallel channel.       4 operators processed 1 tuple each.
        3. FilterLine@139676441211568 has received 1 lines on this parallel channel.       1 operator processed 2 tuples.
        4. FilterLine@139676441211848 has received 1 lines on this parallel channel.
        5. FilterLine@139676441655728 has received ...                                  
           FilterLine@139676441655728 has received 2 lines on this parallel channel.
           
    """
    if len(sys.argv) != 3:
        print("Usage: python3 parallel_regex_grep.py <directory> <search_pattern>")
        return
    directory = sys.argv[1]
    pattern = sys.argv[2]
    
    # Define the topology
    topo = Topology("parallel_regex_grep")
    
    # Declare a stream with tuples that are string objects
    # All files in a directory are read, resulting in lines of text
    # Each line is a tuple in the stream
    lines = topo.source(util_functions.DirectoryWatcher(directory))
    
    # Count the total number of lines before they are split between
    # different parallel channels.
    lines_counter = lines.transform(parallel_regex_grep_functions.LineCounter())

    # Parallelize the Stream.
    # Since there are 5 channels of the stream, the approximate number of
    # lines sent to each channel should be numSentStrings/5. This can be
    # verified by comparing the outputs of the lines_counter stream to that
    # of the parallel channels.
    lines_parallel = lines_counter.parallel(5);
    
    # Filter for the matched string, and print the number strings that have
    # been tested. This is happening in parallel.
    filtered_parallel = lines_parallel.filter(parallel_regex_grep_functions.FilterLine(pattern))
    
    # Join the results of each parallel filter into one stream,
    # merging the parallel streams back into one stream.
    filtered_condensed = filtered_parallel.end_parallel();
    
    # Print the combined results
    filtered_condensed.print()
    
    # Execute the topology
    streamsx.topology.context.submit("STANDALONE", topo)
     
if __name__ == '__main__':
    main()
    
