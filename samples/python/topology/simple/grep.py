# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
import sys
from streamsx.topology.topology import Topology
import streamsx.topology.context
import grep_functions
import util_functions

def main():
    """
    Sample continuous (streaming) grep topology application. This Python application builds a
    simple topology that periodically polls a directory for files, reads each file and
    output lines that contain the search term.
    Thus as each file is added to the directory, the application will read
    it and output matching lines.
    
    Args:
        directory (string): a directory that contains files to process
        search_string (string): a search term
        
    Example:
        * Create a subdirectory "dir"
        * Create file1.txt in subdirectory "dir" with the following contents:
            file1 line1
            file1 line2
            file1 line3
        * Create file2.txt in subdirectory "dir" with the following contents:
            file2 line1
            file2 line2
            file2 line3
        * python3 grep.py dir line2
        
    Output:
        file1 line2
        file2 line2
    """
    
    if len(sys.argv) != 3:
        print("Usage: python3 grep.py <directory> <search_string>")
        return
    directory = sys.argv[1]
    term = sys.argv[2]
    topo = Topology("grep")
    
    # Declare a stream that will contain the contents of the files.
    # For each input file, DirectoryWatcher opens the file and reads its contents 
    # as a text file, producing a tuple for each line of the file. The tuple contains
    # the contents of the line, as a string.
    lines = topo.source(util_functions.DirectoryWatcher(directory))
    
    # Filter out non-matching lines. FilterLine is a callable class 
    # that will be executed for each tuple on lines, that is each line
    # read from a file.  Only lines that contain the string `term` will
    # be included in the output stream.
    matching = lines.filter(grep_functions.FilterLine(term))
    
    # print the matching lines to standard out
    matching.print()
    
    # execute the topology
    streamsx.topology.context.submit("STANDALONE", topo)
     
if __name__ == '__main__':
    main()
    
