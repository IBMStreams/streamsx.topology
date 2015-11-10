__author__ = 'wcmarsha'

from topology.api.OperatorInvocation import OperatorInvocation
from topology.utils.decorators import overrides
from topology.impl.ports.OPort import OPort
from topology.impl.ports.IPort import IPort
import binascii
import marshal
import inspect
import imp


class OpInvocation(OperatorInvocation):

    def __init__(self, index, name, method, params, graph = None):
        self.index = index
        self.name = name
        self.method = method
        self.params = {}
        self.setParameters(params)
        self._addOperatorFunction(self.method)
        self.graph = graph

        self.inputPorts = []
        self.outputPorts = []

    @overrides(OperatorInvocation)
    def getGraph(self):
        return self.graph

    @overrides(OperatorInvocation)
    def addOutputPort(self, name=None, inputPort=None):
        if name is None:
            name = self.getName() + "_OUT"+str(len(self.getOutputPorts()))
        oport = OPort(name, self, len(self.getOutputPorts()))
        self.outputPorts.append(oport)

        if not inputPort is None:
            oport.connect(inputPort)
        return oport

    @overrides(OperatorInvocation)
    def getParameters(self):
        return self.params

    @overrides(OperatorInvocation)
    def setParameters(self, params):
        for param in params:
            self.params[param] = params[param]


    @overrides(OperatorInvocation)
    def appendParameters(self, params):
        for param in params:
            if self.params.get(param) is None:
                self.params[param] = params[param]
            else:
                for innerParam in param:
                    self.params[param].append(innerParam)

    @overrides(OperatorInvocation)
    def addInputPort(self, name=None, outputPort=None):
        if name is None:
            name = self.getName() + "_IN"+ str(len(self.getInputPorts()))
        iport = IPort(name, self, len(self.getInputPorts()))
        self.inputPorts.append(iport)

        if not outputPort is None:
            iport.connect(outputPort)
        return iport


    @overrides(OperatorInvocation)
    def getInputPorts(self):
        return self.inputPorts


    @overrides(OperatorInvocation)
    def getOutputPorts(self):
        return self.outputPorts


    @overrides(OperatorInvocation)
    def getName(self):
        return self.name


    @overrides(OperatorInvocation)
    def generateSPLOperator(self):
        _op = {}
        _op["name"] = self.getName()

        kind = ""
        if len(self.getInputPorts()) > 0 and len(self.getOutputPorts()) > 0:
            """this is a transform function"""
            kind = "pythonFunctions::tupleFunction"
        elif len(self.getInputPorts()) > 0 and len(self.getOutputPorts()) == 0:
            """this is a sink function"""
            kind = "pythonFunctions::sinkFunction"
        elif len(self.getInputPorts()) == 0 and len(self.getOutputPorts()) > 0:
            """this is a source function"""
            kind = "pythonFunctions::sourceFunction"

        _op["kind"] = kind
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

    def _addOperatorFunction(self, callableObj):
        if not hasattr(callableObj, "__call__"):
            raise "argument to _addOperatorFunction is not callable"

        """Always serialize"""
        marshalled = marshal.dumps(callableObj.__code__)
        hexedFunc = binascii.hexlify(marshalled)
        stringedFunc = hexedFunc.decode()
        funcSource = inspect.getsourcefile(callableObj)
        if funcSource == "<stdin>" or funcSource == "<string>":
            """Assumed stateless"""
            paramDict = {"serializedFunc": [stringedFunc], "functionName": [callableObj.__name__], "functionLocation":
                         [funcSource]}
            self.setParameters(paramDict)
            return
        else:
            (name, suffix, mode, mtype)  = inspect.getmoduleinfo(funcSource)
            mod = imp.load_source(name, funcSource)
            funcInModuleMatchingName= [item for item in inspect.getmembers(mod, inspect.isfunction) if item[0] ==
                                       callableObj.__name__]
            if len(funcInModuleMatchingName) > 0:
                """Function is resolvable in module"""
                paramDict = {"serializedFunc": [""], "functionName": [callableObj.__name__], "functionLocation": [name]}
                self.setParameters(paramDict)
                return
            else:
                """Function was defined in another function in source file and is not resolvable"""
                paramDict = {"serializedFunc": [stringedFunc], "functionName": [callableObj.__name__], "functionLocation": [name]}
                self.setParameters(paramDict)


    def _printOperator(self):
        print(self.getName()+":")
        print("inputs:" + str(len(self.inputPorts)))
        for port in self.inputPorts:
            print(port.getName())
        print("outputs:" + str(len(self.outputPorts)))
        for port in self.outputPorts:
            print(port.getName())



