__author__ = 'wcmarsha'

import uuid
from topology.api.OperatorGraph import OperatorGraph
from topology.impl.OpInvocation import OpInvocation


class OpGraph(OperatorGraph):

    def __init__(self, name=None):
        if name is None:
            name = str(uuid.uuid1()).replace("-", "")
        self.name = name
        self.operators = []

    def getName(self):
        return self.name

    def addOperator(self, method, name=None):
        if name is None:
            name = self.getName() + "_OP"+str(len(self.getOperators()))
        op = OpInvocation(len(self.getOperators()), name,  method, {}, self)
        self.operators.append(op)
        return op

    def getOperators(self):
        return self.operators

    def generateSPLGraph(self):
        _graph = {}
        _graph["name"] = self.getName()
        _graph["namespace"] = self.getName()
        _graph["public"] = True
        _graph["config"] = {}
        _ops = []
        for op in self.getOperators():
            _ops.append(op.generateSPLOperator())

        _graph["operators"] = _ops
        return _graph
