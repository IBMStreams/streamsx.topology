# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("str_flatmap_str")

  ts = topo.subscribe("pytest/string/flatmap", schema=CommonSchema.String)

  ts = ts.flat_map(pytest_funcs.string_fm)

  ts.publish("pytest/string/flatmap/result", schema=CommonSchema.String)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
