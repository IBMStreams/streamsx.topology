from topology.context import StreamsContextFactory
from topology.context.Types import Types
from topology.impl.OpGraph import OpGraph
from topology.Stream import Stream

import shutil
import inspect
import os

class Topology(object):
    """Topology that contains graph + operators"""
    def __init__(self, name, files=None):
        self.name=name
        self.graph = OpGraph(name)
        self.files = []
        self.num_counters = 0
        self.num_string_funcs = 0
        topology_package = os.path.dirname(os.path.abspath(__file__))
        self.addFiles([topology_package])
        if files is not None:
            self.addFiles(files)

    def getName(self):
        """
        Returns the name of the topology
        :return: The name of the topology.
        """
        return self.name

    def getGraph(self):
        """
        Returns the underlying OperatorGraph object
        :return: The OperatorGraphObject
        """
        return self.graph

    def counter(self):
        """
        Creates a stream that produces integer tuples that start from zero, incrementing by one each time the
        source is invoked.
        :return: A stream with the values of an integer counter as tuples.
        """
        f = "def counterFunc" + str(self.num_counters) + "():\n"
        f += "\tcounterFunc" + str(self.num_counters) + ".__dict__.setdefault(\"cnt\", -1)\n"
        f += "\tcounterFunc" + str(self.num_counters) + ".cnt += 1\n"
        f += "\treturn counterFunc" + str(self.num_counters) + ".cnt\n"
        exec(f, globals())
        func = globals()["counterFunc" + str(self.num_counters)]
        self.num_counters += 1
        return self.source(func)

    def strings(self, string_args):
        """
        Takes a list of strings and sends them as tuples on the stream.
        :param string_args: A list of strings
        :return: A stream with the values of the string array as tuples.
        """
        f = "def stringFunc" + str(self.num_string_funcs) + "():\n"
        f += "\tstringFunc" + str(self.num_string_funcs) + ".__dict__.setdefault(\"index\", -1)\n"
        f += "\tstringFunc" + str(self.num_string_funcs) + ".__dict__.setdefault(\"strings\", " + str(string_args) + ")\n"
        f += "\tstringFunc" + str(self.num_string_funcs) + ".index += 1\n"
        f += "\tif stringFunc" + str(self.num_string_funcs) + ".index >= len(stringFunc" + str(self.num_string_funcs) + ".strings):\n"
        f += "\t\treturn None\n"
        f += "\treturn stringFunc" + str(self.num_string_funcs) + ".strings[stringFunc" + str(self.num_string_funcs) + ".index]\n"
        exec(f, globals())
        func = globals()["stringFunc" + str(self.num_string_funcs)]
        self.num_string_funcs += 1
        return self.source(func)

    def source(self, func):
        """
        Takes a zero-argument function. Periodically polls the function, and passes the output as a tuple on the
        returned stream.
        :param func: A source function (zero arguments)
        :return: A stream whose tuples are the result of the output obtained by periodically invoking the provided
        function.
        """
        op = self.graph.addOperator(func)
        oport = op.addOutputPort()
        return Stream(self, oport)

    def addFiles(self, files):
        """
        Takes a list of files or directory. Copies them into the application bundle when submitting, adding any
        python modules/packages to the pythonPath.
        :param files: A list of files or directories.
        :return: None
        """
        for file_ in files:
            file_ = os.path.abspath(file_)
            if file_ in self.files:
                return
            self.files.append(file_)

    def _pythonPystreamssDir(self):
        _topology = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        _pystreams = os.path.dirname(_topology)
        return _pystreams
