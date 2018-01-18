# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
from streamsx.topology.topology import *
import streamsx.topology.context

# Application that subscribes to a stream
# with the topic 'simple'.
#
# This app is submitted to a distributed
# Streams instance using
#
# python3 subscribe.py
#
# This is the companion application to publish.py
#
# Any number of these applications can be submitted
# as publish subscribe model is a many publisher
# to many subscriber model

def main():
  topo = Topology("SubscribeSimple")

  # Subscribe to streams with Python objects and topic 'simple'
  ts = topo.subscribe('simple')

  # Print to see some output (in the PE Console log)
  ts.print()

  streamsx.topology.context.submit("DISTRIBUTED", topo)

if __name__ == '__main__':
    main()
