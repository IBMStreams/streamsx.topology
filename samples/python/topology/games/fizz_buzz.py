# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019
from __future__ import print_function
from streamsx.topology.topology import Topology
import streamsx.topology.context
import fizz_buzz_functions


def main():
    """
    Plays Fizz Buzz (https://en.wikipedia.org/wiki/Fizz_buzz)
    
    Example:
        python3 fizz_buzz.py
    Output:
        1
        2
        Fizz!
        4
        Buzz!
        Fizz!
        7
        8
        Fizz!
        Buzz!
        11
        Fizz!
        13
        14
        FizzBuzz!
        ...

    """
    topo = Topology("fizz_buzz")
    
    # Declare a stream of int values
    counting = topo.source(fizz_buzz_functions.int_tuples)
    
    # Print the tuples to standard output
    play_fizz_buzz(counting).print()
    
    # At this point the streaming topology (streaming) is
    # declared, but no data is flowing. The topology
    # must be submitted to a context to be executed.
    
    # execute the topology by submitting to a standalone context
    streamsx.topology.context.submit("STANDALONE", topo)

def play_fizz_buzz(counting):
    """
    Return a stream that plays Fizz Buzz based
    upon the values in the input stream.
    
    Transform an input stream of integers to a
    stream of strings that follows
    the Fizz Buzz rules based upon each value in the
    input stream.
         
    Args:
        counting: input stream
    Returns:
        transformed output stream
    """
    shouts = counting.map(fizz_buzz_functions.fizz_buzz)
    return shouts

if __name__ == '__main__':
    main()
    
