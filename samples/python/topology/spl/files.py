# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import sys
import string
from streamsx.topology.topology import Topology
from streamsx.topology import spl_ops
from streamsx.topology.spl_ops import *
from streamsx.topology.spl_types import *
from streamsx.topology.schema import *
import streamsx.topology.context

def main():
    """
    This demonstrates the invocation of SPL operators from
    the SPL standard toolkit.
            
    Example:
        python3 files.py
    Output:
      Capitalized words from the contents of files in /tmp/work
    """
    
    # Create the container for the topology that will hold the streams of tuples.
    topo = Topology("Files")
    
    # Invoke an SPL DirectoryScan operator as a source.
    # This one scans /tmp/work for files.
    # Note the full kind of the operator is required.
    files = source("spl.adapter::DirectoryScan", topology=topo,
        schema=CommonSchema.String, params = {'directory': '/tmp/work'})

    # Follow it with a FileSource operator
    # If no schema is provided then the input schema is used.
    lines = map("spl.adapter::FileSource", files)

    # Feed the lines into a Python function
    lines = lines.map(string.capwords)
    
    # Sink lines by printing each of its tuples to standard output
    lines.print()
    
    # Now execute the topology by submitting to a standalone context.
    streamsx.topology.context.submit("STANDALONE", topo.graph)
     
if __name__ == '__main__':
    main()
    
