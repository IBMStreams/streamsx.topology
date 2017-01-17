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

from streamsx.topology import logging_utils
import logging
import tempfile
import os
import os.path
import json
import subprocess
import threading
import sys
import enum

from platform import python_version

logging_utils.initialize_logging()
logger = logging.getLogger('streamsx.topology.py_submit')

#
# Submission of a python graph using the Java Application API
# The JAA is reused to have a single set of code_createJSONFile that creates
# SPL, the toolkit, the bundle and submits it to the relevant
# environment
#
def submit(ctxtype, graph, config=None, username=None, password=None, log_level=logging.INFO):
    """
    Submits a topology with the specified context type.
    
    Args:
        ctxtype (string): context type.  Values include:
        * DISTRIBUTED - the topology is submitted to a Streams instance.
          The bundle is submitted using `streamtool` which must be setup to submit without requiring authentication
          input. Additionally, a username and password may optionally be provided to enable retrieving data from remote
          views.
        * STANDALONE - the topology is executed directly as an Streams standalone application.
          The standalone execution is spawned as a separate process
        * BUNDLE - execution of the topology produces an SPL application bundle
          (.sab file) that can be submitted to an IBM Streams instance as a distributed application.
        * JUPYTER - the topology is run in standalone mode, and context.submit returns a stdout streams of bytes which 
          can be read from to visualize the output of the application.
        * BUILD_ARCHIVE - Creates a Bluemix-compatible build archive.
          execution of the topology produces a build archive, which can be submitted to a streaming
          analytics Bluemix remote build service.
        * ANALYTICS_SERVICE - If a local Streams install is present, the application is built locally and then submitted
          to a Bluemix streaming analytics service. If a local Streams install is not present, the application is 
          submitted to, built, and executed on a Bluemix streaming analytics service. If the ConfigParams.FORCE_REMOTE_BUILD
          flag is set to true, the application will be built on Bluemix even if a local Streams install is present.
        graph: a Topology object.
        config (dict): a configuration object containing job configurations and/or submission information. Keys include:
        * 'topology.service.vcap' - a json representation of a VCAP object.
        * 'topology.service.name' - the name of the streaming analytics service for submission.
        * 'topology.forceRemoteBuild' - A flag which will force the application to be compiled and submitted remotely, if possible.
        username (string): an optional SWS username. Needed for retrieving remote view data.
        password (string): an optional SWS password. Used in conjunction with the username, and needed for retrieving
        remote view data.
        log_level: The maximum logging level for log output.
        
    Returns:
        An output stream of bytes if submitting with JUPYTER, otherwise returns None.
    """    
    logger.setLevel(log_level)
    context_submitter = _SubmitContextFactory(graph, config, username, password).get_submit_context(ctxtype)
    try:
        return context_submitter.submit()
    except:
        logger.exception("Error while submitting application.")


class _BaseSubmitter:
    """
    A submitter which handles submit operations common across all submitter types..
    """
    def __init__(self, ctxtype, config, app_topology):
        self.ctxtype = ctxtype
        self.config = dict()
        if config is not None:
            # Make copy of config to avoid modifying
            # the callers config
            self.config.update(config)
        self.app_topology = app_topology

        # Convert the JobConfig into overlays
        self._create_job_config_overlays()

        # encode the relevant python version information into the config
        self._do_pyversion_initialization()

        # Create the json file containing the representation of the application
        try:
            self.fn = self._create_json_file(self._create_full_json())
        except Exception:
            logger.exception("Error generating SPL and creating JSON file.")
            raise

    def submit(self):
        tk_root = self._get_toolkit_root()

        cp = os.path.join(tk_root, "lib", "com.ibm.streamsx.topology.jar")

        streams_install = os.environ.get('STREAMS_INSTALL')
        # If there is no streams install, get java from JAVA_HOME and use the remote contexts.
        if streams_install is None:
            java_home = os.environ.get('JAVA_HOME')
            if java_home is None:
                raise ValueError("JAVA_HOME not found. Please set the JAVA_HOME system variable")

            jvm = os.path.join(java_home, "bin", "java")
            submit_class = "com.ibm.streamsx.topology.context.remote.RemoteContextSubmit"
        # Otherwise, use the Java version from the streams install
        else:
            jvm = os.path.join(streams_install, "java", "jre", "bin", "java")
            if ConfigParams.FORCE_REMOTE_BUILD in self.config and self.config[ConfigParams.FORCE_REMOTE_BUILD]:
                submit_class = "com.ibm.streamsx.topology.context.remote.RemoteContextSubmit"
            else:
                submit_class = "com.ibm.streamsx.topology.context.StreamsContextSubmit"
            cp = cp + ':' + os.path.join(streams_install, "lib", "com.ibm.streams.operator.samples.jar")

        args = [jvm, '-classpath', cp, submit_class, self.ctxtype, self.fn]
        logger.info("Generating SPL and submitting application.")
        process = subprocess.Popen(args, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        try:
            stderr_thread = threading.Thread(target=_print_process_stderr, args=([process, self.fn]))
            stderr_thread.daemon = True
            stderr_thread.start()

            stdout_thread = threading.Thread(target=_print_process_stdout, args=([process]))
            stdout_thread.daemon = True
            stdout_thread.start()
            process.wait()
            return None

        except:
            logger.exception("Error starting java subprocess for submission")
            raise

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
        bf = dict()
        bf["python"] = pythonreal
        bf["pythonconfig"] = pythonrealconfig
        self.config["pythonversion"]["binaries"].append(bf)

    def _create_job_config_overlays(self):
        if ConfigParams.JOB_CONFIG in self.config:
            jco = self.config[ConfigParams.JOB_CONFIG]
            del self.config[ConfigParams.JOB_CONFIG]
            jco._add_overlays(self.config)
 
    def _create_full_json(self):
        fj = dict()
        fj["deploy"] = self.config
        fj["graph"] = self.app_topology.generateSPLGraph()
        return fj

    def _create_json_file(self, fj):
        if sys.hexversion < 0x03000000:
            tf = tempfile.NamedTemporaryFile(mode="w+t", suffix=".json", prefix="splpytmp", delete=False)
        else:
            tf = tempfile.NamedTemporaryFile(mode="w+t", suffix=".json", encoding="UTF-8", prefix="splpytmp",
                                             delete=False)
        tf.write(json.dumps(fj, sort_keys=True, indent=2, separators=(',', ': ')))
        tf.close()
        return tf.name


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
    @staticmethod
    def _get_toolkit_root():
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



class _JupyterSubmitter(_BaseSubmitter):
    def submit(self):
        tk_root = self._get_toolkit_root()

        cp = os.path.join(tk_root, "lib", "com.ibm.streamsx.topology.jar")

        streams_install = os.environ.get('STREAMS_INSTALL')
        # If there is no streams install, get java from JAVA_HOME and use the remote contexts.
        if streams_install is None:
            java_home = os.environ.get('JAVA_HOME')
            if java_home is None:
                raise ValueError("JAVA_HOME not found. Please set the JAVA_HOME system variable")

            jvm = os.path.join(java_home, "bin", "java")
            submit_class = "com.ibm.streamsx.topology.context.remote.RemoteContextSubmit"
        # Otherwise, use the Java version from the streams install
        else:
            jvm = os.path.join(streams_install, "java", "jre", "bin", "java")
            submit_class = "com.ibm.streamsx.topology.context.StreamsContextSubmit"
            cp = cp + ':' + os.path.join(streams_install, "lib", "com.ibm.streams.operator.samples.jar")

        args = [jvm, '-classpath', cp, submit_class, ContextTypes.STANDALONE, self.fn]
        process = subprocess.Popen(args, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        try:
            stderr_thread = threading.Thread(target=_print_process_stderr, args=([process, self.fn]))
            stderr_thread.daemon = True
            stderr_thread.start()

            if process.stdout is None:
                raise ValueError("The returned stdout from the spawned process is None.")
            return process.stdout
        except:
            logger.exception("Error starting java subprocess for submission")
            raise


class _RemoteBuildSubmitter(_BaseSubmitter):
    """
    A submitter which retrieves the SWS REST API URL and then submits the application to be built and submitted
    on Bluemix within a Streaming Analytics service.
    """
    def __init__(self, ctxtype, config, app_topology):
        _BaseSubmitter.__init__(self, ctxtype, config, app_topology)

        # Get the username, password, and rest API URL
        if ConfigParams.VCAP_SERVICES not in config or ConfigParams.SERVICE_NAME not in config:
            raise ValueError("The ConfigParams.VCAP_SERVICES and ConfigParams.SERVICE_NAME parameters were not supplied.")
        services = config[ConfigParams.VCAP_SERVICES]['streaming-analytics']
        creds = None
        for service in services:
            if service['name'] == config[ConfigParams.SERVICE_NAME]:
                creds = service['credentials']
                break
        if creds is None:
            raise ValueError(config['topology.service.name'] + " service was not found in the supplied VCAP")
        username = creds['userid']
        password = creds['password']

        # Obtain REST only when needed. Otherwise, submitting "Hello World" without requests fails.
        try:
            import requests
        except (ImportError, NameError):
            logger.exception('Unable to import the optional "Requests" module. This is needed when performing'
                             ' a remote build or retrieving view data.')
            raise

        # Obtain the streams SWS REST URL
        resources_url = creds['rest_url'] + creds['resources_path']
        #print("Rest host is ", creds['rest_url'], " resources_url is ", resources_url)
        try:
            response = requests.get(resources_url, auth=(username, password)).json()
        except:
            logger.exception("Error while querying url: " + resources_url)
            raise

        rest_api_url = response['streams_rest_url'] + '/resources'

        # Give each view in the app the necessary information to connect to SWS.
        for view in app_topology.get_views():
            connection_info = {'username': username, 'password': password, 'rest_api_url': rest_api_url}
            view.set_streams_context_config(connection_info)


class _DistributedSubmitter(_BaseSubmitter):
    """
    A submitter which retrieves the SWS REST API URL and then submits the application to be built and submitted
    on Bluemix within a Streaming Analytics service.
    """
    def __init__(self, ctxtype, config, app_topology, username, password):
        _BaseSubmitter.__init__(self, ctxtype, config, app_topology)

        # If a username or password isn't supplied, don't attempt to retrieve view data, but throw an error if views
        # were created
        if (username is None or password is None) and len(app_topology.get_views()) > 0:
            raise ValueError("To access views data, both a username and a password must be supplied when submitting.")
        elif username is None or password is None:
            return
        try:
            process = subprocess.Popen(['streamtool', 'geturl', '--api'],
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            rest_api_url = process.stdout.readline().strip().decode('utf-8')
            logger.debug("The rest API URL obtained from streamtool is " + rest_api_url)
        except:
            logger.exception("Error getting SWS rest api url via streamtool")
            raise

        # Give each view in the app the necessary information to connect to SWS.
        for view in app_topology.get_views():
            view.set_streams_context_config(
                {'username': username, 'password': password, 'rest_api_url': rest_api_url})


class _SubmitContextFactory:
    """
    ContextSubmitter:
        Responsible for performing the correct submission depending on a number of factors, including: the
        presence/absence of a streams install, the type of context, and whether the user seeks to retrieve data via rest
    """
    def __init__(self, app_topology, config=None, username=None, password=None):
        self.app_topology = app_topology.graph
        self.config = config
        self.username = username
        self.password = password

        if self.config is None:
            self.config = {}

    def get_submit_context(self, ctxtype):

        # If there is no streams install present, currently only ANALYTICS_SERVICE, TOOLKIT, and BUILD_ARCHIVE
        # are supported.
        streams_install = os.environ.get('STREAMS_INSTALL')
        if streams_install is None:
            if not (ctxtype == ContextTypes.TOOLKIT or ctxtype == ContextTypes.BUILD_ARCHIVE
                    or ctxtype == ContextTypes.ANALYTICS_SERVICE):
                raise UnsupportedContextException(ctxtype + " must be submitted when a streams install is present.")

        if ctxtype == ContextTypes.JUPYTER:
            logger.debug("Selecting the JUPYTER context for submission")
            return _JupyterSubmitter(ctxtype, self.config, self.app_topology)
        elif ctxtype == ContextTypes.DISTRIBUTED:
            logger.debug("Selecting the DISTRIBUTED context for submission")
            return _DistributedSubmitter(ctxtype, self.config, self.app_topology, self.username, self.password)
        elif ctxtype == ContextTypes.ANALYTICS_SERVICE:
            logger.debug("Selecting the ANALYTICS_SERVICE context for submission")
            return _RemoteBuildSubmitter(ctxtype, self.config, self.app_topology)
        else:
            logger.debug("Using the BaseSubmitter, and passing the context type through to java.")
            return _BaseSubmitter(ctxtype, self.config, self.app_topology)


# Used to delete the JSON file after it is no longer needed.
def _delete_json(fn):
    if os.path.isfile(fn):
        os.remove(fn)


# Used by a thread which polls a subprocess's stdout and writes it to stdout
def _print_process_stdout(process):
    try:
        while True:
            line = process.stdout.readline()
            if len(line) == 0:
                process.stdout.close()
                break
            line = line.decode("utf-8").strip()
            print(line)
    except:
        process.stdout.close()
        logger.exception("Error reading from process stdout")
        raise


# Used by a thread which polls a subprocess's stderr and writes it to stderr, until the sc compilation
# has begun.
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


class UnsupportedContextException(Exception):
    """
    An exeption class for when something goes wrong with submitting using a particular context.
    """
    def __init__(self, msg):
        Exception.__init__(self, msg)

class ContextTypes:
    """
        Types of submission contexts:

        DISTRIBUTED - the topology is submitted to a Streams instance.
        The bundle is submitted using `streamtool` which must be setup to submit without requiring authentication
        input. Additionally, a username and password may optionally be provided to enable retrieving data from remote
        views.
        STANDALONE - the topology is executed directly as an Streams standalone application.
        The standalone execution is spawned as a separate process
        BUNDLE - execution of the topology produces an SPL application bundle
        (.sab file) that can be submitted to an IBM Streams instance as a distributed application.
        STANDALONE_BUNDLE - execution of the topology produces an SPL application bundle that, when executed,
        is spawned as a separate process.
        JUPYTER - the topology is run in standalone mode, and context.submit returns a stdout streams of bytes which
        can be read from to visualize the output of the application.
        BUILD_ARCHIVE - Creates a Bluemix-compatible build archive.
        execution of the topology produces a build archive, which can be submitted to a streaming
        analytics Bluemix remote build service.
        TOOLKIT - Execution of the topology produces a toolkit.
        ANALYTICS_SERVICE - If a local Streams install is present, the application is built locally and then submitted
        to a Bluemix streaming analytics service. If a local Streams install is not present, the application is 
        submitted to, built, and executed on a Bluemix streaming analytics service. If the ConfigParams.REMOTE_BUILD
        flag is set to true, the application will be built on Bluemix even if a local Streams install is present.
    """
    TOOLKIT = 'TOOLKIT'
    BUILD_ARCHIVE = 'BUILD_ARCHIVE'
    BUNDLE = 'BUNDLE'
    STANDALONE_BUNDLE = 'STANDALONE_BUNDLE'
    STANDALONE = 'STANDALONE'
    DISTRIBUTED = 'DISTRIBUTED'
    JUPYTER = 'JUPYTER'
    ANALYTICS_SERVICE = 'ANALYTICS_SERVICE'


class ConfigParams:
    """
    Configuration options which may be used as keys in the submit's config parameter.

    VCAP_SERVICES - a json object containing the VCAP information used to submit to Bluemix
    SERVICE_NAME - the name of the streaming analytics service to use from VCAP_SERVICES.
    """
    VCAP_SERVICES = 'topology.service.vcap'
    SERVICE_NAME = 'topology.service.name'
    FORCE_REMOTE_BUILD = 'topology.forceRemoteBuild'
    JOB_CONFIG = 'topology.jobConfigOverlays'

class JobConfig:
    """
    Job configuration
    """
    def __init__(self, job_name=None, job_group=None, data_directory=None):
        self.job_name = job_name
        self.job_group = job_group
        self.data_directory = data_directory

    def _add_overlays(self, config):
       """
       {"jobConfigOverlays":[{"jobConfig":{"jobName":"BluemixSubmitSample"},"deploymentConfig":{"fusionScheme":"legacy"}}]}
       """
       jco = {}
       config["jobConfigOverlays"] = [jco]
       jc = {}

       if self.job_name is not None:
            jc["jobName"] = self.job_name
       if self.job_group is not None:
           jc["jobGroup"] = self.job_group
       if self.data_directory is not None:
           jc["dataDirectory"] = self.data_directory

       if jc:
           jco["jobConfig"] = jc
