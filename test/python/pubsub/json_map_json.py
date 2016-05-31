from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("json_map_json")

  ts = topo.subscribe("pytest/json/map", schema=CommonSchema.Json)

  ts = ts.map(pytest_funcs.json_add)

  ts.publish("pytest/json/map/result", schema=CommonSchema.Json)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
