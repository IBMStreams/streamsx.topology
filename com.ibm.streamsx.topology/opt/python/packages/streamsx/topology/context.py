from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
try:
    from future import standard_library
    standard_library.install_aliases()
except (ImportError, NameError):
    # nothing to do here
    pass
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import tempfile
import os
import os.path
import json
import subprocess
import threading
import sys
import logging
from platform import python_version

logger = logging.getLogger('streamsx.topology.py_submit')


def _delete_json(fn):
    if os.path.isfile(fn):
        os.remove(fn)


def _print_process_stdout(process):
    try:
        while True:
            line = process.stdout.readline();
            if len(line) == 0:
                process.stdout.close()
                break
            line = line.decode("utf-8").strip()
            print(line)
    except:
        process.stdout.close()
        logger.exception("Error reading from process stdout")
        raise


def _print_process_stderr(process, fn):
    try:
        while True:
            line = process.stderr.readline()
            if len(line) == 0:
                process.stderr.close()
                break
            line = line.decode("utf-8").strip()
            print(line)
            if "com.ibm.streamsx.topology.internal.streams.InvokeSc getToolkitPath" in line:
                _delete_json(fn)
    except:
        process.stderr.close()
        logger.exception("Error reading from process stderr")
        raise
#
# Submission of a python graph using the Java Application API
# The JAA is reused to have a single set of code that creates
# SPL, the toolkit, the bundle and submits it to the relevant
# environment
#
def submit(ctxtype, app_topology, config = None, username = None, password = None, rest_api_url = None, log_level=logging.INFO):
    """
    Submits a topology with the specified context type.
    
    Args:
        ctxtype (string): context type.  Values include:
        * DISTRIBUTED - the topology is submitted to a Streams instance.
          The bundle is submitted using `streamtool` which must be setup to submit without requiring authentication input.
        * STANDALONE - the topology is executed directly as an Streams standalone application.
          The standalone execution is spawned as a separate process
        * BUNDLE - execution of the topology produces an SPL application bundle
          (.sab file) that can be submitted to an IBM Streams instance as a distributed application.
        * JUPYTER - the topology is run in standalone mode, and context.submit returns a stdout streams of bytes which 
          can be read from to visualize the output of the application.
        app_topology: a Topology object or Topology.graph object
        
    Returns:
        An output stream of bytes if submitting with JUPYTER, otherwise returns None.
    """    
    logger.setLevel(log_level)
    context_submitter = ContextSubmitter(app_topology, config, username, password, rest_api_url)
    context_submitter.submit(ctxtype)

class ContextSubmitter:
    """
    ContextSubmitter:
        Responsible for performing the correct submission depending on a number of factors, including: the
        presence/absence of a streams install, the type of context, and whether the user seeks to retrieve data via rest.
    """

    def __init__(self, app_topology, config = None, username = None, password = None, rest_api_url = None):
        self.app_topology = app_topology.graph
        self.config = config
        self.username = username
        self.password = password
        self.rest_api_url = rest_api_url

        if self.config is None:
            self.config = {}

        # deserialize vcap config before passing it off to Java
        if 'topology.service.vcap' in config:
            config['topology.service.vcap'] = json.loads(config['topology.service.vcap'])

        # encode the relevant python version information into the config
        self._do_pyversion_initialization()


    def _do_pyversion_initialization(self):
        # path to python binary
        pythonbin = sys.executable
        pythonreal = os.path.realpath(pythonbin)
        pythondir = os.path.dirname(pythonbin)
        pythonrealfile = os.path.basename(pythonreal)
        pythonrealconfig = os.path.realpath(pythondir + "/" + pythonrealfile + "-config")
        pythonversion = python_version()

        # place the fullpaths to the python binary that is running and
        # the python-config that will used into the config
        self.config["pythonversion"] = {}
        self.config["pythonversion"]["version"] = pythonversion
        self.config["pythonversion"]["binaries"] = []
        bf = {}
        bf["python"] = pythonreal
        bf["pythonconfig"] = pythonrealconfig
        self.config["pythonversion"]["binaries"].append(bf)

    def submit(self, ctxtype):
        try:
            fn = self._createJSONFile(self._createFullJSON())
        except Exception:
            logger.exception("Error generating SPL and creating JSON file.")
            raise

        # Create connection to SWS
        if self.username is not None and self.password is not None:
            rc = None
            if self.rest_api_url is None:
                try:
                    process = subprocess.Popen(['streamtool', 'geturl', '--api'],
                                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    rest_api_url = process.stdout.readline().strip().decode('utf-8')
                except:
                    logger.exception("Error getting SWS rest api url ")
                    raise

            for view in self.app_topology.get_views():
                view.set_streams_context_config(
                    {'username': self.username, 'password': self.password, 'rest_api_url': rest_api_url})
        try:
            return self._submitUsingJava(ctxtype, fn)
        except:
            logger.exception("Error submitting with java")
            _delete_json(fn)
            raise

    def _createFullJSON(self):
        fj = {}
        fj["deploy"] = self.config
        fj["graph"] = self.app_topology.generateSPLGraph()
        return fj

    def _createJSONFile(self, fj):
        if sys.hexversion < 0x03000000:
            tf = tempfile.NamedTemporaryFile(mode="w+t", suffix=".json", prefix="splpytmp", delete=False)
        else:
            tf = tempfile.NamedTemporaryFile(mode="w+t", suffix=".json", encoding="UTF-8", prefix="splpytmp",
                                             delete=False)
        tf.write(json.dumps(fj, sort_keys=True, indent=2, separators=(',', ': ')))
        tf.close()
        return tf.name

    def _submitUsingJava(self, ctxtype, fn):
        ctxtype_was = ctxtype
        if ctxtype == "JUPYTER":
            ctxtype = "STANDALONE"

        tk_root = self._get_toolkit_root()

        cp = os.path.join(tk_root, "lib", "com.ibm.streamsx.topology.jar")

        streams_install = os.environ.get('STREAMS_INSTALL')
        if streams_install is None:
            java_home = os.environ.get('JAVA_HOME')
            if java_home is None:
                raise "Please set the JAVA_HOME system variable"

            jvm = os.path.join(java_home, "bin", "java")
            submit_class = "com.ibm.streamsx.topology.context.remote.RemoteContextSubmit"
        else:
            jvm = os.path.join(streams_install, "java", "jre", "bin", "java")
            submit_class = "com.ibm.streamsx.topology.context.StreamsContextSubmit"
            cp = cp + ':' + os.path.join(streams_install, "lib", "com.ibm.streams.operator.samples.jar")

        args = [jvm, '-classpath', cp, submit_class, ctxtype, fn]
        process = subprocess.Popen(args, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        try:
            stderr_thread = threading.Thread(target=_print_process_stderr, args=([process, fn]))
            stderr_thread.daemon = True
            stderr_thread.start()
            if not ctxtype_was == "JUPYTER":
                stdout_thread = threading.Thread(target=_print_process_stdout, args=([process]))
                stdout_thread.daemon = True
                stdout_thread.start()
                process.wait()
                return None
            else:
                return process.stdout
        except:
            logger.exception("Error starting java subprocess for submission")
            raise

    # There are two modes for execution.
    #
    # Pypi (Python focused)
    #  Pypi (pip install) package includes the SPL toolkit as
    #      streamsx/.toolkit/com.ibm.streamsx.topology
    #      However the streamsx Python packages have been moved out
    #      of the toolkit's (opt/python/package) compared
    #      to the original toolkit layout. They are moved to the
    #      top level of the pypi package.
    #
    # SPL Toolkit (SPL focused):
    #   Streamsx Python packages are executed from opt/python/packages
    #
    # This function determines the root of the SPL toolkit based
    # upon the existance of the '.toolkit' directory.
    #
    def _get_toolkit_root(self):
        # Directory of this file (streamsx/topology)
        dir = os.path.dirname(os.path.abspath(__file__))

        # This is streamsx
        dir = os.path.dirname(dir)

        # See if .toolkit exists, if so executing from
        # a pip install
        tk_root = os.path.join(dir, '.toolkit', 'com.ibm.streamsx.topology')
        if os.path.isdir(tk_root):
            return tk_root

        # Else dir is tk/opt/python/packages/streamsx

        dir = os.path.dirname(dir)
        dir = os.path.dirname(dir)
        dir = os.path.dirname(dir)
        tk_root = os.path.dirname(dir)
        return tk_root