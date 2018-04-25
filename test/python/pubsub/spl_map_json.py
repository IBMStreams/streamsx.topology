# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys

import pytest_schema
import pytest_funcs

def main():
  tkdir = sys.argv[1]

  topo = Topology("spl_map_json")

  ts = topo.subscribe("pytest/spl/map", schema=pytest_schema.all_spl_types)

  ts = ts.map(lambda x : x, schema=pytest_schema.all_spl_types.as_tuple())
  ts = ts.map(lambda x : x, schema=pytest_schema.all_spl_types)

  ts = ts.isolate().map(pytest_funcs.remove_complex).isolate()
  ts = ts.map(pytest_funcs.change_set_to_list).isolate()

  ts.publish("pytest/spl/map/result", schema=CommonSchema.Json)

  config = {}
  config['topology.toolkitDir'] = tkdir

  streamsx.topology.context.submit("TOOLKIT", topo.graph, config)

if __name__ == '__main__':
    main()
