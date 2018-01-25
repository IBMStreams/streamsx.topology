# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
from typing import Any, List

from streamsx.topology.topology import Topology
from streamsx.topology.schema import _AnySchema

class ExtensionOperator(object):
    def __init__(self, topology: Topology, kind: str, inputs: Any=None, schemas: List[_AnySchema]=None, params: Any=None, name: str=None) -> None: ...
    def params(self) -> Any: ...
