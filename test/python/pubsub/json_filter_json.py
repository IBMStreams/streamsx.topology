# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("json_filter_json")

  ts = topo.subscribe("pytest/json/filter", schema=CommonSchema.Json)

  ts = ts.filter(pytest_funcs.json_filter)

  ts.publish("pytest/json/filter/result", schema=CommonSchema.Json)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
