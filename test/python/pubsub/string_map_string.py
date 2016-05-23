from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("str_map_str")

  ts = topo.subscribe("pytest/string/map", schema=CommonSchema.String)

  ts = ts.map(pytest_funcs.string_add)

  ts.publish("pytest/string/map/result", schema=CommonSchema.String)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
