from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("json_flatmap_str")

  ts = topo.subscribe("pytest/json/flatmap", schema=CommonSchema.Json)

  ts = ts.flat_map(pytest_funcs.json_fm)

  ts.publish("pytest/json/flatmap/result", schema=CommonSchema.String)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
