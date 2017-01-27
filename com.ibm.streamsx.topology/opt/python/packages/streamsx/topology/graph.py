# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2016

import sys
import uuid
import json
import inspect
import pickle

try:
    import dill
    dill.settings['recurse'] = True
except ImportError:
    dill = pickle

import types
import base64
import streamsx.topology.dependency
import streamsx.topology.functions
import streamsx.topology.param
from streamsx.topology.schema import CommonSchema
from streamsx.topology.schema import _stream_schema

class SPLGraph(object):

    def __init__(self, topology, name=None):
        if name is None:
            name = str(uuid.uuid1()).replace("-", "")
        # Allows Topology or SPLGraph to be passed to submit
        self.graph = self
        self.name = name
        self.topology = topology
        self.operators = []
        self.resolver = streamsx.topology.dependency._DependencyResolver(self.topology)
        self._views = []

    def get_views(self):
        return self._views

    def add_views(self, view):
        self._views.append(view)

    def addOperator(self, kind, function=None, name=None, params=None):
        if(params is None):
            params = {}
        if name is None:
            if function is not None:
               if hasattr(function, '__name__'):
                   n = function.__name__
                   if n == '<lambda>':
                       # Avoid use of <> characters in name
                       # as they are converted to unicode
                       # escapes in SPL identifier
                       n = 'lambda'
                   name = n + "_"
               elif hasattr(function, '__class__'):
                   name = function.__class__.__name__ + "_"
            else:
               name = self.name + "_OP"
        name = name + str(len(self.operators))
        if(kind.startswith("$")):    
            op = Marker(len(self.operators), kind, name, {}, self)                           
        else:
            if function is not None:
                params['toolkitDir'] = streamsx.topology.param.toolkit_dir()
            op = _SPLInvocation(len(self.operators), kind, function, name, params, self)
        self.operators.append(op)
        if not function is None:
            dep_instance = function
            if isinstance(function, streamsx.topology.functions._IterableInstance):
                dep_instance = type(function._it)

            if not inspect.isbuiltin(dep_instance):
                self.resolver.add_dependencies(inspect.getmodule(dep_instance))
        return op
    
    def addPassThruOperator(self):
        name = self.name + "_OP"+str(len(self.operators))
        op = _SPLInvocation(len(self.operators), "spl.relational::Functor", None, name, {}, self)
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
        self.addPackages(_graph["config"]["includes"])
        for op in self.operators:
            _ops.append(op.generateSPLOperator())

        _graph["operators"] = _ops
        return _graph
   
    def addPackages(self, includes):
        for package_path in self.resolver.packages:
           mf = {}
           mf["source"] = package_path
           mf["target"] = "opt/python/packages"
           includes.append(mf)

    def addModules(self, includes):
        for module_path in self.resolver.modules:
           mf = {}
           mf["source"] = module_path
           mf["target"] = "opt/python/modules"
           includes.append(mf)

    def getLastOperator(self):
        return self.operators[len(self.operators) -1]      
        
    def printJSON(self):
      print(json.dumps(self.generateSPLGraph(), sort_keys=True, indent=4, separators=(',', ': ')))

class _SPLInvocation(object):

    def __init__(self, index, kind, function, name, params, graph, view_configs = None):
        self.index = index
        self.kind = kind
        self.function = function
        self.name = name
        self.params = {}
        self.setParameters(params)
        self._addOperatorFunction(self.function)
        self.graph = graph
        self.viewable = True

        if view_configs is None:
            self.view_configs = []
        else:
            self.view_configs = view_configs

        self.inputPorts = []
        self.outputPorts = []

    def addOutputPort(self, oWidth=None, name=None, inputPort=None, schema= CommonSchema.Python,partitioned=None):
        if name is None:
            name = self.name + "_OUT"+str(len(self.outputPorts))
        oport = OPort(name, self, len(self.outputPorts), schema, oWidth, partitioned)
        self.outputPorts.append(oport)
        if schema == CommonSchema.Python:
            self.viewable = False

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

    def getViewConfig(self):
        return self.view_configs

    def addViewConfig(self, view_configs):
        self.view_configs.append(view_configs)

    def addInputPort(self, name=None, outputPort=None):
        if name is None:
            name = self.name + "_IN"+ str(len(self.inputPorts))
        iPortSchema = CommonSchema.Python    
        if not outputPort is None :
            iPortSchema = outputPort.schema        
        iport = IPort(name, self, len(self.inputPorts),iPortSchema)
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
        _op["config"]["streamViewability"] = self.viewable
        _op["config"]["viewConfigs"] = self.view_configs
        _params = {}
        # Add parameters as their string representation
        # unless they value has a spl_json() function,
        # then use that
        _params = {}
        for name in self.params:
            param = self.params[name]
            try:
                _params[name] = param.spl_json()
            except:
                _value = {}
                _value["value"] = param
                _params[name] = _value
        _op["parameters"] = _params
        return _op

    def _addOperatorFunction(self, function):
        if (function == None):
            return None
        if not hasattr(function, "__call__"):
            raise "argument to _addOperatorFunction is not callable"

        # Wrap a lambda as a callable class instance
        if isinstance(function, types.LambdaType) and function.__name__ == "<lambda>" :
            function = streamsx.topology.functions._Callable(function)
        elif function.__module__ == '__main__':
            # Function/Class defined in main, create a callable wrapping its
            # dill'ed form
            function = streamsx.topology.functions._Callable(function)
         
        if inspect.isroutine(function):
            # callable is a function
            self.params["pyName"] = function.__name__
        else:
            # callable is a callable class instance
            self.params["pyName"] = function.__class__.__name__
            # dill format is binary; base64 encode so it is json serializable 
            self.params["pyCallable"] = base64.b64encode(dill.dumps(function)).decode("ascii")

        # note: functions in the __main__ module cannot be used as input to operations 
        # function.__module__ will be '__main__', so C++ operators cannot import the module
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
    def __init__(self, name, operator, index, schema):
        self.name = name
        self.operator = operator
        self.index = index
        self.schema = schema
        self.outputPorts = []

    def connect(self, oport):
        if not oport in self.outputPorts:
            self.outputPorts.append(oport)

        if not self in oport.inputPorts:
            oport.connect(self)

    def getSPLInputPort(self):
        _iport = {}
        _iport["name"] = self.name
        _iport["connections"] = [port.name for port in self.outputPorts]
        _iport["type"] = self.schema.schema()
        return _iport

class OPort(object):
    def __init__(self, name, operator, index, schema, width=None, partitioned=None):
        self.name = name
        self.operator = operator
        self.schema = _stream_schema(schema)
        self.index = index
        self.width = width
        self.partitioned =  partitioned

        self.inputPorts = []

    def connect(self, iport):
        if not iport in self.inputPorts:
            self.inputPorts.append(iport)
        
        if not self in iport.outputPorts:
            iport.connect(self)

    def getSPLOutputPort(self):
        _oport = {}
        _oport["type"] = self.schema.schema()
        _oport["name"] = self.name
        _oport["connections"] = [port.name for port in self.inputPorts]
        if not self.width is None:
            _oport["width"] = int(self.width)
        if not self.partitioned is None:
            _oport["partitioned"] = self.partitioned
        return _oport

class Marker(_SPLInvocation):

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

