# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("str_filter_str")

  ts = topo.subscribe("pytest/string/filter", schema=CommonSchema.String)

  ts = ts.filter(pytest_funcs.string_filter)

  ts.publish("pytest/string/filter/result", schema=CommonSchema.String)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
