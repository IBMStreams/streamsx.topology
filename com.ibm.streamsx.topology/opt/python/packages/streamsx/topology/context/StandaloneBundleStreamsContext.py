import os
from os.path import dirname

__author__ = 'wcmarsha'

import tempfile
from topology.context.ToolkitStreamsContext import ToolkitStreamsContext
from topology.context.ContextProperties import ContextProperties

class StandaloneBundleStreamsContext(ToolkitStreamsContext):
    def __init__(self):
        pass

    def submit(self, topology, config={}):
        if ContextProperties.TOOLKIT_DIR not in config:
            config[ContextProperties.TOOLKIT_DIR] = tempfile.mkdtemp("", "tk")

        super(StandaloneBundleStreamsContext, self).submit(topology, config)
        toolkit_root = config[ContextProperties.TOOLKIT_DIR]

        # Get python SPL toolkit to compile
        python_toolkit_root = dirname(dirname(dirname(dirname(dirname(os.path.abspath(__file__))))))
        python_toolkit_root = os.path.join(python_toolkit_root, "com.ibm.streamsx.topology.functional.python")

        # Go to the root of the generated toolkit
        prev_cwd = os.getcwd()
        os.chdir(toolkit_root)

        # Compile the SPL application
        sc = "sc -T -M " + topology.getName() + "::" + topology.getName() + " "
        sc += "-t " + python_toolkit_root
        os.system(sc)

        # Copy the bundle
        os.system("cp -f ./output/" + topology.getName()+"."+topology.getName()+".sab " + prev_cwd)
        os.chdir(prev_cwd)