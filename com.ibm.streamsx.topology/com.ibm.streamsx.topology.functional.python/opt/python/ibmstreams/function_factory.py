__author__ = 'wcmarsha'

import types
import string
import random
import binascii
import marshal
import pickle
from ibmstreams.function_types import functionTypes

class functionFactory:
    """A factory that will return a new function object, depending on the function type passed
        in to the getFunction method."""

    @staticmethod
    def getFunction(type, serialized_func, function_name, function_location):
        if(type == functionTypes.SINK_FUNCTION):
            func_name = functionFactory._func_name_generator("sink_function_")

            sink = "def " + func_name + "(arg):\n"
            sink += "\tdepickled = pickle.loads(arg)\n"
            sink += "\t"+func_name + ".wrappedFunction(depickled)"

            exec(sink,globals())
            f = globals()[func_name]
            wrapped = functionFactory._getWrappedFunction(func_name, serialized_func,  function_name, function_location)
            f.wrappedFunction=wrapped
            return f

        if(type == functionTypes.TUPLE_FUNCTION):
            func_name = functionFactory._func_name_generator("transform_function_")

            trans = "def " + func_name + "(arg):\n" #return " + func_name + ".wrappedFunction(arg)"
            trans += "\tdepickled = pickle.loads(arg)\n"
            trans += "\tret = " + func_name + ".wrappedFunction(depickled)\n"
            trans += "\tif ret is None:\n"
            trans += "\t\treturn None\n"
            trans += "\tret = (ret)\n"
            trans += "\tdataBytes = pickle.dumps(ret)\n"
            trans += "\treturn dataBytes\n"

            exec(trans,globals())
            f = globals()[func_name]
            wrapped = functionFactory._getWrappedFunction(func_name, serialized_func, function_name, function_location)
            f.wrappedFunction=wrapped
            return f

        if(type == functionTypes.SOURCE_FUNCTION):
            func_name = functionFactory._func_name_generator("source_function_")

            source = "def " + func_name + "():\n"
            source += "\tret = " + func_name + ".wrappedFunction()\n"
            source += "\tif ret is None:\n"
            source += "\t\treturn None\n"
            source += "\tret = (ret)\n"
            source += "\tdataBytes = pickle.dumps(ret)\n"
            source += "\treturn dataBytes\n"

            exec(source,globals())
            f = globals()[func_name]
            wrapped = functionFactory._getWrappedFunction(func_name, serialized_func, function_name, function_location)
            f.wrappedFunction=wrapped
            return f

    @staticmethod
    def _deserializeFunction(function_name, serializedFunc):
        """Function is marshal'd, and hexlified"""
        # Bytes is a str
        binaryHex = serializedFunc.encode()
        binary = binascii.unhexlify(binaryHex)
        code = marshal.loads(binary)
        exec("globals()[\""+function_name + "\"] = types.FunctionType(code, globals(), \""+function_name+"\")", globals(), locals())
        return globals()[function_name]

    @staticmethod
    def _func_name_generator(prefix, size=6, chars=string.ascii_uppercase + string.digits):
        func_name = prefix + ''.join(random.choice(chars) for _ in range(size))
        while func_name in globals():
            func_name = prefix.join(random.choice(chars) for _ in range(size))
        return func_name

    @staticmethod
    def _getWrappedFunction(unique_prefix, serialized_func, function_name, function_location):
        if not serialized_func == "":
            return functionFactory._deserializeFunction(function_name, serialized_func)
        functionFactory._importFunctionFromModule(function_name, function_location)
        exec(unique_prefix + "wrapf = " + function_location+"."+function_name, globals())
        return globals()[unique_prefix + "wrapf"]

    @staticmethod
    def _importFunctionFromModule(function_name, function_location):
        """
        Puts the function's module in the globals namespace.
        :param function_name: Name of the function that will be used by the SPL wrapper function
        :param function_location: Name of the module in which the function resides
        :return: None
        """
        statement = "try:\n"
        statement += "\t" + function_location + " = __import__(\"" + function_location + "\", globals(), locals(), [\"" + function_name + "\"])\n"
        statement += "except:\n"
        statement += "\tprint(\"raised some exception\")\n"
        statement += "\timport sys\n"
        statement += "\tprint(sys.exc_info())\n"

        exec(statement, globals())