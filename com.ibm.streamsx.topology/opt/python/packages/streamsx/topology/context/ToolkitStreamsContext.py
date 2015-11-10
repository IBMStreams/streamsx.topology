import json
import os
import tempfile
from topology.context.ContextProperties import ContextProperties

__author__ = 'wcmarsha'


class ToolkitStreamsContext(object): #Use new-style class
    def __init__(self):
        pass

    def submit(self, topology, config={}):
        # Set the toolkit root if it hasn't been.
        if ContextProperties.TOOLKIT_DIR not in config:
            config[ContextProperties.TOOLKIT_DIR] = tempfile.mkdtemp("", "tk")

        namespace = topology.getName()
        toolkit_root = config[ContextProperties.TOOLKIT_DIR]
        tk_opt_python_packages = os.path.join(toolkit_root, "opt", "python", "packages")
        json_filename = os.path.join(toolkit_root, namespace, namespace + ".json")
        spl_filename = os.path.join(toolkit_root, namespace, namespace + ".spl")

        self.make_directory_structure(toolkit_root, namespace)

        # Copy explicitly included files to the toolkit_root/opt/python/packages
        for file in topology.files:
            if not os.path.exists(file):
                raise "File " + file + " does not exist"
            else:
                os.system("cp -r " + file + " " + tk_opt_python_packages)
                if not os.path.exists(os.path.join(tk_opt_python_packages, os.path.basename(file))):
                    raise "Error copying file " + file
                print("Copied " + file + " to " + os.path.join(tk_opt_python_packages, os.path.basename(file)))

        # Generate the SPL
        # TODO: replace with code the generates JSON and submits to java generator
        spl_fd = open(json_filename, 'w')
        spl_fd.write(json.dumps(topology.getGraph().generateSPLGraph(), sort_keys=True,
            indent=4, separators=(',', ': ')))
        spl_fd.close()

        streams_install = os.environ.get('STREAMS_INSTALL')
        if streams_install is None:
            raise "Please set the STREAMS_INSTALL system variable"

        # Get path to com.ibm.streamsx.topology/lib
        topology_lib = os.path.abspath(__file__)
        for i in range(5):
            topology_lib = os.path.dirname(topology_lib)
        topology_lib = os.path.join(topology_lib, "lib")
        os.system("java -cp \"" + streams_install + "/lib/*:" + topology_lib +
                  "/*\" com.ibm.streamsx.topology.generator.spl.SPLFromFileName " +
                  json_filename + " " + spl_filename)


    def make_directory_structure(self, toolkit_root, namespace):
        tk_namespace = os.path.join(toolkit_root, namespace)
        tk_impl = os.path.join(toolkit_root, "impl")
        tk_impl_lib = os.path.join(toolkit_root, "impl", "lib")
        tk_etc = os.path.join(toolkit_root, "etc")
        tk_opt = os.path.join(toolkit_root, "opt")
        tk_opt_python = os.path.join(toolkit_root, "opt", "python")
        tk_opt_python_packages = os.path.join(toolkit_root, "opt", "python", "packages")

        self.safe_mkdir(tk_namespace)
        self.safe_mkdir(tk_impl)
        self.safe_mkdir(tk_impl_lib)
        self.safe_mkdir(tk_etc)
        self.safe_mkdir(tk_opt)
        self.safe_mkdir(tk_opt_python)
        self.safe_mkdir(tk_opt_python_packages)

    def safe_mkdir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
