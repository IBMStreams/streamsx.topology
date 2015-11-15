# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import uuid
import json
import inspect

from streamsx.topology.impl.ports.IPort import IPort
from streamsx.topology.impl.ports.OPort import OPort

class SPLGraph(object):

    def __init__(self, name=None):
        if name is None:
            name = str(uuid.uuid1()).replace("-", "")
        self.name = name
        self.operators = []
        self.modules = set()

    def getName(self):
        return self.name

    def addOperator(self, kind, function, name=None):
        if name is None:
            name = self.getName() + "_OP"+str(len(self.getOperators()))
        op = SPLInvocation(len(self.getOperators()), kind, function, name, {}, self)
        self.operators.append(op)
        if not inspect.isbuiltin(function):
            self.modules.add(inspect.getmodule(function))
        return op

    def getOperators(self):
        return self.operators

    def generateSPLGraph(self):
        _graph = {}
        _graph["name"] = self.getName()
        _graph["namespace"] = self.getName()
        _graph["public"] = True
        _graph["config"] = {}
        _graph["config"]["includes"] = []
        _ops = []
        self.addModules(_graph["config"]["includes"])
        for op in self.getOperators():
            _ops.append(op.generateSPLOperator())

        _graph["operators"] = _ops
        return _graph

    def addModules(self, includes):
        for module in self.modules:
           mf = {}
           mf["source"] = module.__file__
           mf["target"] = "opt/python/modules"
           includes.append(mf)
        
    def printJSON(self):
      print(json.dumps(self.generateSPLGraph(), sort_keys=True, indent=4, separators=(',', ': ')))

class SPLInvocation(object):

    def __init__(self, index, kind, function, name, params, graph):
        self.index = index
        self.kind = kind
        self.function = function
        self.name = name
        self.params = {}
        self.setParameters(params)
        self._addOperatorFunction(self.function)
        self.graph = graph

        self.inputPorts = []
        self.outputPorts = []

    def getGraph(self):
        return self.graph

    def addOutputPort(self, name=None, inputPort=None):
        if name is None:
            name = self.getName() + "_OUT"+str(len(self.getOutputPorts()))
        oport = OPort(name, self, len(self.getOutputPorts()))
        self.outputPorts.append(oport)

        if not inputPort is None:
            oport.connect(inputPort)
        return oport

    def getParameters(self):
        return self.params

    def setParameters(self, params):
        for param in params:
            self.params[param] = params[param]


    def appendParameters(self, params):
        for param in params:
            if self.params.get(param) is None:
                self.params[param] = params[param]
            else:
                for innerParam in param:
                    self.params[param].append(innerParam)

    def addInputPort(self, name=None, outputPort=None):
        if name is None:
            name = self.getName() + "_IN"+ str(len(self.getInputPorts()))
        iport = IPort(name, self, len(self.getInputPorts()))
        self.inputPorts.append(iport)

        if not outputPort is None:
            iport.connect(outputPort)
        return iport


    def getInputPorts(self):
        return self.inputPorts


    def getOutputPorts(self):
        return self.outputPorts


    def getName(self):
        return self.name


    def generateSPLOperator(self):
        _op = {}
        _op["name"] = self.getName()

        _op["kind"] = self.kind
        _op["partitioned"] = False

        _outputs = []
        _inputs = []

        for output in self.outputPorts:
            _outputs.append(output.getSPLOutputPort())

        for input in self.inputPorts:
            _inputs.append(input.getSPLInputPort())
        _op["outputs"] = _outputs
        _op["inputs"] = _inputs
        _op["config"] = {}

        _params = {}
        for param in self.params:
            _value = {}
            _value["value"] = self.params[param]
            _params[param] = _value

        _op["parameters"] = _params

        return _op

    def _addOperatorFunction(self, function):
        if not hasattr(function, "__call__"):
            raise "argument to _addOperatorFunction is not callable"
        self.params["functionName"] = function.__name__;
        self.params["functionModule"] = function.__module__;


    def _printOperator(self):
        print(self.getName()+":")
        print("inputs:" + str(len(self.inputPorts)))
        for port in self.inputPorts:
            print(port.getName())
        print("outputs:" + str(len(self.outputPorts)))
        for port in self.outputPorts:
            print(port.getName())

