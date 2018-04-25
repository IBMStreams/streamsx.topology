# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
from streamsx.topology.topology import *
import streamsx.topology.context
import pubsub_functions;

# Application that publishes a stream
# of integers with the topic 'simple'.
#
# This app is submitted to a distributed
# Streams instance using
#
# python3 publish.py
#
# This is the companion application to subscribe.py
#
# Any number of these applications can be submitted
# as publish subscribe model is a many publisher
# to many subscriber model

def main():
  topo = Topology("PublishSimple")

  # Create a source that is a sequence 0..inifinity
  ts = topo.source(pubsub_functions.sequence)

  # Delay to reduce cpu overhead for the demo
  ts = ts.filter(pubsub_functions.delay)

  # Publish the stream as Python objects with
  # topic simple.
  ts.publish('simple')

  # Also print to see some output (in the PE Console log)
  ts.print()

  # Submit application to a distributed instance
  streamsx.topology.context.submit('DISTRIBUTED', topo)

if __name__ == '__main__':
    main()
