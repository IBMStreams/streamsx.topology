# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import uuid
import json
import inspect
import pickle
import base64
from streamsx.topology.schema import CommonSchema

class SPLGraph(object):

    def __init__(self, name=None):
        if name is None:
            name = str(uuid.uuid1()).replace("-", "")
        self.name = name
        self.operators = []
        self.modules = set()

    def addOperator(self, kind, function=None, name=None):
        if name is None:
            name = self.name + "_OP"+str(len(self.operators))
        if(kind.startswith("$")):    
            op = Marker(len(self.operators), kind, name, {}, self)                           
        else:
            op = SPLInvocation(len(self.operators), kind, function, name, {}, self)
        self.operators.append(op)
        # TODO
        if not function is None:
            if not inspect.isbuiltin(function):
                self.modules.add(inspect.getmodule(function))
        return op
    
    def addPassThruOperator(self):
        name = self.name + "_OP"+str(len(self.operators))
        op = SPLInvocation(len(self.operators), "com.ibm.streamsx.topology.functional.python::PyFunctionPassThru", None, name, {}, self)
        self.operators.append(op)
        return op

    def generateSPLGraph(self):
        _graph = {}
        _graph["name"] = self.name
        _graph["namespace"] = self.name
        _graph["public"] = True
        _graph["config"] = {}
        _graph["config"]["includes"] = []
        _ops = []
        self.addModules(_graph["config"]["includes"])
        for op in self.operators:
            _ops.append(op.generateSPLOperator())

        _graph["operators"] = _ops
        return _graph

    def addModules(self, includes):
        for module in self.modules:
           mf = {}
           mf["source"] = module.__file__
           mf["target"] = "opt/python/modules"
           includes.append(mf)
           
    def getLastOperator(self):
        return self.operators[len(self.operators) -1]      
        
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

    def addOutputPort(self, oWidth=None, name=None, inputPort=None, schema= CommonSchema.Python):
        if name is None:
            name = self.name + "_OUT"+str(len(self.outputPorts))
        oport = OPort(name, self, len(self.outputPorts), schema, oWidth)
        self.outputPorts.append(oport)

        if not inputPort is None:
            oport.connect(inputPort)
        return oport

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
            name = self.name + "_IN"+ str(len(self.inputPorts))
        iport = IPort(name, self, len(self.inputPorts))
        self.inputPorts.append(iport)

        if not outputPort is None:
            iport.connect(outputPort)
        return iport


    def generateSPLOperator(self):
        _op = {}
        _op["name"] = self.name

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
        if (function == None):
            return None
        if not hasattr(function, "__call__"):
            raise "argument to _addOperatorFunction is not callable"
                 
        if inspect.isroutine(function):
            # callable is a function
            self.params["pyName"] = function.__name__
        else:
            # callable is a callable class instance
            self.params["pyName"] = function.__class__.__name__
            # pickle format is binary; base64 encode so it is json serializable 
            self.params["pyCallable"] = base64.b64encode(pickle.dumps(function)).decode("ascii")

        self.params["pyModule"] = function.__module__
                    

    def _printOperator(self):
        print(self.name+":")
        print("inputs:" + str(len(self.inputPorts)))
        for port in self.inputPorts:
            print(port.name())
        print("outputs:" + str(len(self.outputPorts)))
        for port in self.outputPorts:
            print(port.name)

class IPort(object):
    def __init__(self, name, operator, index):
        self.name = name
        self.operator = operator
        self.index = index
        self.outputPorts = []

    def connect(self, oport):
        if not oport in self.outputPorts:
            self.outputPorts.append(oport)

        if not self in oport.inputPorts:
            oport.connect(self)

    def getOperator(self):
        return self.operator

    def getSPLInputPort(self):
        _iport = {}
        _iport["name"] = self.name
        _iport["connections"] = [port.name for port in self.outputPorts]
        return _iport

class OPort(object):
    def __init__(self, name, operator, index, schema, width=None):
        self.name = name
        self.operator = operator
        self.schema = schema
        self.index = index
        self.width = width

        self.inputPorts = []

    def connect(self, iport):
        if not iport in self.inputPorts:
            self.inputPorts.append(iport)
        
        if not self in iport.outputPorts:
            iport.connect(self)

    def getOperator(self):
        return self.operator

    def getSPLOutputPort(self):
        _oport = {}
        _oport["type"] = self.schema.schema()
        _oport["name"] = self.name
        _oport["connections"] = [port.name for port in self.inputPorts]
        if not self.width is None:
            _oport["width"] = int(self.width)
        return _oport

class Marker(SPLInvocation):

    def __init__(self, index, kind, name, params, graph):
        self.index = index
        self.kind = kind
        self.name = name
        self.params = {}
        self.setParameters(params)
        self.graph = graph

        self.inputPorts = []
        self.outputPorts = []
                   

    def generateSPLOperator(self):
        _op = {}
        _op["name"] = self.name

        _op["kind"] = self.kind
        _op["partitioned"] = False

        _op["marker"] = True
        _op["model"] = "virtual"
        _op["language"] = "virtual"

        _outputs = []
        _inputs = []

        for output in self.outputPorts:
            _outputs.append(output.getSPLOutputPort())

        for input in self.inputPorts:
            _inputs.append(input.getSPLInputPort())
        _op["outputs"] = _outputs
        _op["inputs"] = _inputs
        _op["config"] = {}

        return _op

