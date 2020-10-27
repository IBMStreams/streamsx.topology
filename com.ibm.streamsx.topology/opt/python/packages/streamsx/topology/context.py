# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2020
"""

Context for submission and build of topologies.

"""

__all__ = ['ContextTypes', 'ConfigParams', 'JobConfig', 'SubmissionResult', 'submit', 'build', 'run']

import logging
import os
import os.path
import shutil
import json
import platform
import subprocess
import threading
import sys
import codecs
import tempfile
import copy
import time
import warnings

import streamsx.rest
import streamsx.rest_primitives
import streamsx._streams._version
import urllib.parse as up

__version__ = streamsx._streams._version.__version__

logger = logging.getLogger(__name__)

#
# Submission of a python graph using the Java Application API
# The JAA is reused to have a single set of code_createJSONFile that creates
# SPL, the toolkit, the bundle and submits it to the relevant
# environment
#
def submit(ctxtype, graph, config=None, username=None, password=None):
    """
    Submits a `Topology` (application) using the specified context type.

    Used to submit an application for compilation into a Streams application and
    execution within an Streaming Analytics service or IBM Streams instance.

    `ctxtype` defines how the application will be submitted, see :py:class:`ContextTypes`.

    The parameters `username` and `password` are only required when submitting to an
    IBM Streams instance and it is required to access the Streams REST API from the
    code performing the submit. Accessing data from views created by
    :py:meth:`~streamsx.topology.topology.Stream.view` requires access to the Streams REST API.

    Args:
        ctxtype(str): Type of context the application will be submitted to. A value from :py:class:`ContextTypes`.
        graph(Topology): The application topology to be submitted.
        config(dict): Configuration for the submission, augmented with values such as a :py:class:`JobConfig` or keys from :py:class:`ConfigParams`.
        username(str): Deprecated: Username for the Streams REST api. Use environment variable ``STREAMS_USERNAME`` if using user-password authentication.
        password(str): Deprecated: Password for `username`. Use environment variable ``STREAMS_PASSWORD`` if using user-password authentication.

    Returns:
        SubmissionResult: Result of the submission. Content depends on :py:class:`ContextTypes`
        constant passed as `ctxtype`.
    """
    streamsx._streams._version._mismatch_check(__name__)
    graph = graph.graph

    if not graph.operators:
        raise ValueError("Topology {0} does not contain any streams.".format(graph.topology.name))

    if username or password:
        warnings.warn("Use environment variables STREAMS_USERNAME and STREAMS_PASSWORD", DeprecationWarning, stacklevel=2)

    context_submitter = _SubmitContextFactory(graph, config, username, password).get_submit_context(ctxtype)
    sr = SubmissionResult(context_submitter.submit())
    sr._submitter = context_submitter
    return sr

def build(topology, config=None, dest=None, verify=None):
    """
    Build a topology to produce a Streams application bundle.

    Builds a topology using :py:func:`submit` with context type :py:const:`~ContextTypes.BUNDLE`. The result is a sab file on the local file system along
    with a job config overlay file matching the application.

    The build uses a build service or a local install, see :py:const:`~ContextTypes.BUNDLE` for details.

    Args:
        topology(Topology): Application topology to be built.
        config(dict): Configuration for the build.
        dest(str): Destination directory for the sab and JCO files. Default is context specific.
        verify: SSL verification used by requests when using a build service. Defaults to enabling SSL verification.

    Returns:
        3-element tuple containing

        - **bundle_path** (*str*): path to the bundle (sab file) or ``None`` if not created.
        - **jco_path** (*str*): path to file containing the job config overlay for the application or ``None`` if not created.
        - **result** (*SubmissionResult*): value returned from ``submit``.

    .. seealso:: :py:const:`~ContextTypes.BUNDLE` for details on how to configure the build service to use.
    .. versionadded:: 1.14
    """
    if verify is not None:
        config = config.copy() if config else dict()
        config[ConfigParams.SSL_VERIFY] = verify

    sr = submit(ContextTypes.BUNDLE, topology, config=config)
    if 'bundlePath' in sr:
        if dest:
            bundle = sr['bundlePath']
            bundle_dest = os.path.join(dest, os.path.basename(bundle))
            if os.path.exists(bundle_dest): os.remove(bundle_dest)
            shutil.move(bundle, dest)
            sr['bundlePath'] = bundle_dest

            jco = sr['jobConfigPath']
            jco_dest = os.path.join(dest, os.path.basename(jco))
            if os.path.exists(jco_dest): os.remove(jco_dest)
            shutil.move(jco, dest)
            sr['jobConfigPath'] = jco_dest
        return sr['bundlePath'], sr['jobConfigPath'], sr

    return None, None, sr



class _BaseSubmitter(object):
    """
    A submitter which handles submit operations common across all submitter types..
    """
    def __init__(self, ctxtype, config, graph):
        self.ctxtype = ctxtype
        self.config = dict()
        if config is not None:
            # Make copy of config to avoid modifying
            # the callers config
            self.config.update(config)
            # When SERVICE_DEFINITION is a String, it is assumed that 
            # it is JSON SAS credentials, which must be converted to a JSON object
            service_def = self.config.get(ConfigParams.SERVICE_DEFINITION)
            if service_def:
                if isinstance(service_def, str):
                    self.config[ConfigParams.SERVICE_DEFINITION] = json.loads(service_def)
        self.config['contextType'] = str(self.ctxtype)
        if 'originator' not in self.config:
            self.config['originator'] = 'topology-' + __version__ + ':python-' + platform.python_version()
        self.graph = graph
        self.fn = None
        self.results_file = None
        self.keepArtifacts = False
        if 'topology.keepArtifacts' in self.config:
            self.keepArtifacts = self.config.get('topology.keepArtifacts')

    def _config(self):
        "Return the submit configuration"
        return self.config

    def submit(self):
        # Convert the JobConfig into overlays
        self._create_job_config_overlays()

        # encode the relevant python version information into the config
        self._add_python_info()

        # Create the json file containing the representation of the application
        try:
            self._create_json_file(self._create_full_json())
        except IOError:
            logger.error("Error writing json graph to file.")
            raise

        try:
            return self._submit_exec()
        finally:
            if not self.keepArtifacts:
                _delete_json(self)

    def _submit_exec(self):

        tk_root = self._get_toolkit_root()

        cp = os.path.join(tk_root, "lib", "com.ibm.streamsx.topology.jar")

        remote_context = False
        streams_install = os.environ.get('STREAMS_INSTALL')
        # If there is no streams install, get java from JAVA_HOME and use the remote contexts.
        if streams_install is None:
            java_home = os.environ.get('JAVA_HOME')
            if java_home is None:
                raise ValueError("JAVA_HOME not found. Please set the JAVA_HOME system variable")

            jvm = os.path.join(java_home, "bin", "java")
            remote_context = True
        # Otherwise, use the Java version from the streams install
        else:
            jvm = os.path.join(streams_install, "java", "jre", "bin", "java")
            if self.config.get(ConfigParams.FORCE_REMOTE_BUILD):
                remote_context = True
            cp = cp + ':' + os.path.join(streams_install, "lib", "com.ibm.streams.operator.samples.jar")

        progress_fn = lambda _ : None
        if remote_context:
            submit_class = "com.ibm.streamsx.topology.context.remote.RemoteContextSubmit"
            try:
                # Verify we are in a IPython env.
                get_ipython() # noqa : F821
                import ipywidgets as widgets
                logger.debug("ipywidgets available - creating IntProgress")
                progress_bar = widgets.IntProgress(
                    value=0,
                    min=0, max=10, step=1,
                    description='Initializing',
                    bar_style='info', orientation='horizontal',
                    style={'description_width':'initial'})
                logger.debug("ipywidgets available - created IntProgress")
                try:
                    display(progress_bar) # noqa : F821
                    def _show_progress(msg):
                        if msg is True:
                            progress_bar.value = progress_bar.max
                            progress_bar.bar_style = 'success'
                            return
                        if msg is False:
                            progress_bar.bar_style = 'danger'
                            return
                        msg = msg.split('-')
                        progress_bar.value += 1
                        progress_bar.description = msg[3]
                    progress_fn = _show_progress
                except:
                    logger.debug("ipywidgets IntProgress error: %s", sys.exc_info()[1])
                    pass
            except:
                logger.debug("ipywidgets not available: %s", sys.exc_info()[1])
                pass
        else:
            submit_class = "com.ibm.streamsx.topology.context.local.StreamsContextSubmit"

        jul_cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logging.properties')
        jul = '-Djava.util.logging.config.file=' + jul_cfg

        args = [jvm, '-classpath', cp, jul, submit_class, self.ctxtype, self.fn, str(logging.getLogger().getEffectiveLevel())]
        logger.info("Generating SPL and submitting application.")
        proc_env = self._get_java_env()
        process = subprocess.Popen(args, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, env=proc_env)

        stderr_thread = threading.Thread(target=_print_process_stderr, args=([process, self, progress_fn]))
        stderr_thread.daemon = True
        stderr_thread.start()

        stdout_thread = threading.Thread(target=_print_process_stdout, args=([process]))
        stdout_thread.daemon = True
        stdout_thread.start()
        process.wait()

        results_json = {}

        # Only try to read the results file if the submit was successful.
        if process.returncode == 0:
            with open(self.results_file) as _file:
                try:
                    results_json = json.loads(_file.read())
                    progress_fn(True)
                except IOError:
                    logger.error("Could not read file:" + str(_file.name))
                    progress_fn(False)
                    raise
                except json.JSONDecodeError:
                    logger.error("Could not parse results file:" + str(_file.name))
                    progress_fn(False)
                    raise
                except:
                    logger.error("Unknown error while processing results file.")
                    progress_fn(False)
                    raise
        else:
            progress_fn(False)

        results_json['return_code'] = process.returncode
        self._augment_submission_result(results_json)
        self.submission_results = results_json
        return results_json


    def _augment_submission_result(self, submission_result):
        """Allow a subclass to augment a submission result"""
        pass

    def _get_java_env(self):
        "Get the environment to be passed to the Java execution"
        return os.environ.copy()

    def _add_python_info(self):
        # Python information added to deployment
        pi = {}
        pi["prefix"] = sys.exec_prefix
        pi["version"] = sys.version
        pi['major'] = sys.version_info.major
        pi['minor'] = sys.version_info.minor
        self.config["python"] = pi

    def _create_job_config_overlays(self):
        if ConfigParams.JOB_CONFIG in self.config:
            jco = self.config[ConfigParams.JOB_CONFIG]
            del self.config[ConfigParams.JOB_CONFIG]
            jco._add_overlays(self.config)

    def _create_full_json(self):
        fj = dict()

        # Removing Streams Connection object because it is not JSON serializable, and not applicable for submission
        # Need to re-add it, since the StreamsConnection needs to be returned from the submit.
        sc = self.config.pop(ConfigParams.STREAMS_CONNECTION, None)
        fj["deploy"] = self.config.copy()
        fj["graph"] = self.graph.generateSPLGraph()

        _file = tempfile.NamedTemporaryFile(prefix="results", suffix=".json", mode="w+t", delete=False)
        _file.close()
        fj["submissionResultsFile"] = _file.name
        self.results_file = _file.name
        logger.debug("Results file created at " + _file.name)

        if sc is not None:
            self.config[ConfigParams.STREAMS_CONNECTION] = sc
        return fj

    def _create_json_file(self, fj):
        if sys.hexversion < 0x03000000:
            tf = tempfile.NamedTemporaryFile(mode="w+t", suffix=".json", prefix="splpytmp", delete=False)
        else:
            tf = tempfile.NamedTemporaryFile(mode="w+t", suffix=".json", encoding="UTF-8", prefix="splpytmp",
                                         delete=False)
        tf.write(json.dumps(fj, sort_keys=True, indent=2, separators=(',', ': ')))
        tf.close()

        self.fn = tf.name

    def _setup_views(self):
        # Link each view back to this context.
        for view in self.graph._views:
            view.stop_data_fetch()
            view._submit_context = self

    def streams_connection(self):
        raise NotImplementedError("Views require submission to DISTRIBUTED or ANALYTICS_SERVICE context")

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

class _StreamingAnalyticsSubmitter(_BaseSubmitter):
    """
    A submitter supports the ANALYTICS_SERVICE (Streaming Analytics service) context.
    """

    # Maintains the last time by service in ms since epoch the last
    # time a thread saw the service running. Allows avoidance of
    # status checks when we are somewhat confident the service
    # is running, eg. during test runs or repeated submissions.
    _SERVICE_ACTIVE = threading.local()

    def __init__(self, ctxtype, config, graph):
        super(_StreamingAnalyticsSubmitter, self).__init__(ctxtype, config, graph)
        self._streams_connection = self._config().get(ConfigParams.STREAMS_CONNECTION)
        if ConfigParams.SERVICE_DEFINITION in self._config():
            # Convert the service definition to a VCAP services definition.
            # Which is then passed through to Java as a VCAP_SERVICES env var
            # Service name matching the generated VCAP is passed through config.
            service_def = self._config().get(ConfigParams.SERVICE_DEFINITION)
            self._vcap_services = _vcap_from_service_definition(service_def)
            self._config()[ConfigParams.SERVICE_NAME] = _name_from_service_definition(service_def)
        else:
            self._vcap_services = self._config().get(ConfigParams.VCAP_SERVICES)
        self._service_name = self._config().get(ConfigParams.SERVICE_NAME)

        if self._streams_connection is not None:
            if not isinstance(self._streams_connection, streamsx.rest.StreamingAnalyticsConnection):
                raise ValueError("config must contain a StreamingAnalyticsConnection object when submitting to "
                                 "{} context".format(ctxtype))

            # Use credentials stored within StreamingAnalyticsConnection
            self._service_name = self._streams_connection.service_name
            self._vcap_services = {'streaming-analytics': [
                {'name': self._service_name, 'credentials': self._streams_connection.credentials}
            ]}
            self._config()[ConfigParams.SERVICE_NAME] = self._service_name

            # TODO: Compare credentials between the config and StreamsConnection, verify they are the same

        # Clear the VCAP_SERVICES key in config, since env var will contain the content
        self._config().pop(ConfigParams.VCAP_SERVICES, None)
        self._config().pop(ConfigParams.SERVICE_DEFINITION, None)

        self._setup_views()
        self._job = None

    def _create_full_json(self):
        fj = super(_StreamingAnalyticsSubmitter, self)._create_full_json()
        if hasattr(_StreamingAnalyticsSubmitter._SERVICE_ACTIVE, 'running'):
            rts = _StreamingAnalyticsSubmitter._SERVICE_ACTIVE.running
            if self._service_name in rts:
                sn = self._service_name if self._service_name else os.environ['STREAMING_ANALYTICS_SERVICE_NAME']
                fj['deploy']['serviceRunningTime'] = rts[sn]
        return fj

    def _job_access(self):
        if self._job:
            return self._job
        if self._streams_connection is None:
            self._streams_connection = streamsx.rest.StreamingAnalyticsConnection(self._vcap_services, self._service_name)
        self._job = self._streams_connection.get_instances()[0].get_job(
                id=self.submission_results['jobId'])
        return self._job

    def _augment_submission_result(self, submission_result):
        vcap = streamsx.rest._get_vcap_services(self._vcap_services)
        credentials = streamsx.rest._get_credentials(vcap, self._service_name)
        
        if streamsx.rest_primitives._IAMConstants.V2_REST_URL in credentials:
            instance_id = credentials[streamsx.rest_primitives._IAMConstants.V2_REST_URL].split('streaming_analytics/', 1)[1]
        else:
            instance_id = credentials['jobs_path'].split('/service_instances/', 1)[1].split('/', 1)[0]
        submission_result['instanceId'] = instance_id
        if 'jobId' in submission_result:
            if not hasattr(_StreamingAnalyticsSubmitter._SERVICE_ACTIVE, 'running'):
                _StreamingAnalyticsSubmitter._SERVICE_ACTIVE.running = dict()
            sn = self._service_name if self._service_name else os.environ['STREAMING_ANALYTICS_SERVICE_NAME']
            _StreamingAnalyticsSubmitter._SERVICE_ACTIVE.running[sn] = int(time.time() * 1000.0)

    def _get_java_env(self):
        "Pass the VCAP through the environment to the java submission"
        env = super(_StreamingAnalyticsSubmitter, self)._get_java_env()
        vcap = streamsx.rest._get_vcap_services(self._vcap_services)
        env['VCAP_SERVICES'] = json.dumps(vcap)
        return env

class _BundleSubmitter(_BaseSubmitter):
    """
    A submitter which supports the BUNDLE context
    including remote build.
    """
    def __init__(self, ctxtype, config, graph):
        _BaseSubmitter.__init__(self, ctxtype, config, graph)
        self._remote = config.get(ConfigParams.FORCE_REMOTE_BUILD)

        if not self._remote and 'STREAMS_INSTALL' in os.environ:
            return

        self._streams_connection = config.get(ConfigParams.STREAMS_CONNECTION)


        if self._streams_connection is not None:
            pass
        else:
            # Look for a service definition
            svc_info = streamsx.rest_primitives.Instance._find_service_def(config)
            if not svc_info:
                # Look for endpoint set by env vars.
                inst = streamsx.rest_primitives.Instance.of_endpoint(verify=config.get(ConfigParams.SSL_VERIFY))
                if inst is not None:
                    self._streams_connection = inst.rest_client._sc

        if isinstance(self._streams_connection, streamsx.rest.StreamsConnection):
            if isinstance(self._streams_connection.session.auth, streamsx.rest_primitives._ICPDExternalAuthHandler):
                svc_info = self._streams_connection.session.auth._cfg
                self._config()[ConfigParams.SERVICE_DEFINITION] = svc_info
                if  self._streams_connection.session.verify == False:
                    self._config()[ConfigParams.SSL_VERIFY] = False
        else:
            svc_info =  streamsx.rest_primitives.Instance._find_service_def(config)
            if svc_info:
                self._config()[ConfigParams.SERVICE_DEFINITION] = svc_info
                streamsx.rest_primitives.Instance._clear_service_info(self._config())

    def _get_java_env(self):
        "Set env vars from connection if set"
        env = super(_BundleSubmitter, self)._get_java_env()
        env.pop('STREAMS_DOMAIN_ID', None)
        env.pop('STREAMS_INSTANCE_ID', None)
        if self._remote:
            env.pop('STREAMS_INSTALL', None)
        return env
        
class _EdgeSubmitter(_BaseSubmitter):
    """
    A submitter which supports the EDGE context (force remote build).
    """
    def __init__(self, ctxtype, config, graph):
        _BaseSubmitter.__init__(self, ctxtype, config, graph)
        config[ConfigParams.FORCE_REMOTE_BUILD] = True # EDGE is always remote build
        self._remote = config.get(ConfigParams.FORCE_REMOTE_BUILD)
        self._streams_connection = config.get(ConfigParams.STREAMS_CONNECTION)

        if self._streams_connection is not None:
            pass
        else:
            # Look for a service definition
            svc_info = streamsx.rest_primitives.Instance._find_service_def(config)
            if not svc_info:
                # Look for endpoint set by env vars.
                inst = streamsx.rest_primitives.Instance.of_endpoint(verify=config.get(ConfigParams.SSL_VERIFY))
                if inst is not None:
                    self._streams_connection = inst.rest_client._sc

        if isinstance(self._streams_connection, streamsx.rest.StreamsConnection):
            if isinstance(self._streams_connection.session.auth, streamsx.rest_primitives._ICPDExternalAuthHandler):
                svc_info = self._streams_connection.session.auth._cfg
                self._config()[ConfigParams.SERVICE_DEFINITION] = svc_info
                if  self._streams_connection.session.verify == False:
                    self._config()[ConfigParams.SSL_VERIFY] = False
        else:
            svc_info =  streamsx.rest_primitives.Instance._find_service_def(config)
            if svc_info:
                self._config()[ConfigParams.SERVICE_DEFINITION] = svc_info
                streamsx.rest_primitives.Instance._clear_service_info(self._config())

        # check that serviceBuildPoolsEndpoint is set
        try:
            serviceBuildPoolsEndpoint = self._config()[ConfigParams.SERVICE_DEFINITION]['connection_info']['serviceBuildPoolsEndpoint']
        except KeyError: 
                raise RuntimeError('Build service is not configured for EDGE submission')

    def _get_java_env(self):
        "Set env vars from connection if set"
        env = super(_EdgeSubmitter, self)._get_java_env()
        env.pop('STREAMS_DOMAIN_ID', None)
        env.pop('STREAMS_INSTANCE_ID', None)
        if self._remote:
            env.pop('STREAMS_INSTALL', None)
        return env

def _get_distributed_submitter(config, graph, username, password):
    # CP4D integrated environment and within project
    svc_info = streamsx.rest_primitives.Instance._find_service_def(config)
    if svc_info:
        return _DistributedSubmitterCP4DIntegratedProject(config, graph, svc_info)

    # CP4D integrated environment external to project
    if  'CP4D_URL' in os.environ and \
        'STREAMS_INSTANCE_ID' in os.environ and \
        'STREAMS_PASSWORD' in os.environ:
        return _DistributedSubmitterCP4DIntegrated(config, graph)

    # CP4D standalone environment
    if  'STREAMS_REST_URL' in os.environ and \
        'STREAMS_PASSWORD' in os.environ:
        return _DistributedSubmitterCP4DStandalone(config, graph)

    # Streams 4.2/4.3 by connection
    if  'STREAMS_INSTALL' in os.environ and \
        'STREAMS_INSTANCE_ID' in os.environ and \
        ConfigParams.STREAMS_CONNECTION in config and \
        isinstance(config[ConfigParams.STREAMS_CONNECTION], streamsx.rest.StreamsConnection):
        return _DistributedSubmitter4Conn(config, graph, username, password)

    # Streams 4.2/4.3 by environment
    if  'STREAMS_INSTALL' in os.environ and \
        'STREAMS_DOMAIN_ID' in os.environ and \
        'STREAMS_INSTANCE_ID' in os.environ:
        return _DistributedSubmitter4(config, graph, username, password)

    raise RuntimeError('Insufficient configuration for DISTRIBUTED submission')

class _DistributedSubmitter(_BaseSubmitter):
    """
    A submitter which supports the DISTRIBUTED context.
    Sub-classed for specific configurations
    """
    def __init__(self, config, graph, username, password):
        super(_DistributedSubmitter, self).__init__(ContextTypes.DISTRIBUTED, config, graph)

        self._streams_connection = None
        self.username = username
        self.password = password
        self._job = None

        # Give each view in the app the necessary information to connect to SWS.
        self._setup_views()

    def _job_access(self):
        if self._job:
            return self._job

        instance = self._get_instance()
        self._job = instance.get_job(id=self.submission_results['jobId'])
        return self._job


class _DistributedSubmitterCP4DIntegratedProject(_DistributedSubmitter):
    """
    A submitter which supports the CPD integrated configuration
    within a project.
    """
    def __init__(self, config, graph, svc_info):
        super(_DistributedSubmitterCP4DIntegratedProject, self).__init__(config, graph, None, None)
        # use the config here rather than svc_info as the config contains SSL_VERIFY
        streams_instance = streamsx.rest_primitives.Instance.of_service(config)
        if hasattr(streams_instance, 'productVersion'):
            svc_info['productVersion'] = streams_instance.productVersion
        # when we use the REST-API of the CP4D from inside the CP4D (Notebook in a project)
        # we go over this URL: https://internal-nginx-svc:12443
        svc_info['cluster_ip'] = 'internal-nginx-svc'
        svc_info['cluster_port'] = 12443

        # user-provided cp4d URL to override the hard-coded from above
        if ConfigParams.CP4D_URL in config:
            userUrl = config[ConfigParams.CP4D_URL]
            if userUrl:
                es = up.urlparse(userUrl)
                if ':' in es.netloc:
                    cluster_ip = es.netloc.split(':')[0]
                    cluster_port = es.netloc.split(':')[1]
                else:
                    cluster_ip = es.netloc
                    cluster_port = 443
                svc_info['cluster_ip_orig'] = svc_info['cluster_ip']
                svc_info['cluster_port_orig'] = svc_info['cluster_port']
                svc_info['cluster_ip'] = cluster_ip
                svc_info['cluster_port'] = cluster_port

        self._config()[ConfigParams.SERVICE_DEFINITION] = svc_info
        self._config()[ConfigParams.FORCE_REMOTE_BUILD] = True
        streamsx.rest_primitives.Instance._clear_service_info(self._config())

    def _get_instance(self):
        return streamsx.rest_primitives.Instance.of_service(self._config())

    def _get_java_env(self):
        env = super(_DistributedSubmitterCP4DIntegratedProject, self)._get_java_env()
        env.pop('CP4D_URL', None)
        env.pop('STREAMS_DOMAIN_ID', None)
        env.pop('STREAMS_INSTANCE_ID', None)
        env.pop('STREAMS_INSTALL', None)
        return env


class _DistributedSubmitterCP4DIntegrated(_DistributedSubmitter):
    """
    A submitter which supports the CPD integrated configuration
    outside a project.
    """
    def __init__(self, config, graph):
        super(_DistributedSubmitterCP4DIntegrated, self).__init__(config, graph, None, None)
        # Look for endpoint set by env vars.
        self._inst = streamsx.rest_primitives.Instance.of_endpoint(verify=config.get(ConfigParams.SSL_VERIFY))
        if self._inst is None:
            raise ValueError("Incorrect configuration for Cloud Pak for Data integrated configuration")
        self._streams_connection = self._inst.rest_client._sc
        svc_info = self._streams_connection.session.auth._cfg
        if hasattr(self._inst, 'productVersion'):
            svc_info['productVersion'] = self._inst.productVersion
        self._config()[ConfigParams.SERVICE_DEFINITION] = svc_info
        self._config()[ConfigParams.FORCE_REMOTE_BUILD] = True

    def _get_instance(self):
        return self._inst

    def _get_java_env(self):
        env = super(_DistributedSubmitterCP4DIntegrated, self)._get_java_env()
        env.pop('CP4D_URL', None)
        env.pop('STREAMS_DOMAIN_ID', None)
        env.pop('STREAMS_INSTANCE_ID', None)
        env.pop('STREAMS_INSTALL', None)
        return env

class _DistributedSubmitterCP4DStandalone(_DistributedSubmitter):
    """
    A submitter which supports the CPD standalone configuration.
    """
    def __init__(self, config, graph):
        super(_DistributedSubmitterCP4DStandalone, self).__init__(config, graph, None, None)
        # Look for endpoint set by env vars.
        self._inst = streamsx.rest_primitives.Instance.of_endpoint(verify=config.get(ConfigParams.SSL_VERIFY))
        if self._inst is None:
            raise ValueError("Incorrect configuration for Cloud Pak for Data standalone configuration")
        self._streams_connection = self._inst.rest_client._sc
        self._config()[ConfigParams.FORCE_REMOTE_BUILD] = True

    def _get_instance(self):
        return self._inst

    def _get_java_env(self):
        env = super(_DistributedSubmitterCP4DStandalone, self)._get_java_env()
        env.pop('CP4D_URL', None)
        env.pop('STREAMS_DOMAIN_ID', None)
        env.pop('STREAMS_INSTANCE_ID', None)
        env.pop('STREAMS_INSTALL', None)
        return env

class _DistributedSubmitter4(_DistributedSubmitter):
    """
    A submitter which supports the DISTRIBUTED context
    for IBM Streams 4.2/4.3.
    """
    def __init__(self, config, graph, username, password):
        super(_DistributedSubmitter4, self).__init__(config, graph, username, password)

    def _get_instance(self):
        if not self._streams_connection:
            self._streams_connection = streamsx.rest.StreamsConnection(self.username, self.password)
            if ConfigParams.SSL_VERIFY in self._config():
                self._streams_connection.session.verify = self._config()[ConfigParams.SSL_VERIFY]
        return self._streams_connection.get_instance(os.environ['STREAMS_INSTANCE_ID'])
       

class _DistributedSubmitter4Conn(_DistributedSubmitter4):
    """
    A submitter which supports the DISTRIBUTED context
    for IBM Streams 4.2/4.3 using a connection.
    """
    def __init__(self, config, graph, username, password):
        super(_DistributedSubmitter4Conn, self).__init__(config, graph, username, password)
        self._streams_connection = config.get(ConfigParams.STREAMS_CONNECTION)
        self.username = self._streams_connection.session.auth[0]
        self.password = self._streams_connection.session.auth[1]
        if (username is not None and username != self.username) or (password is not None and password != self.password):
            raise RuntimeError('Credentials supplied in the arguments differ than '
                               'those specified in the StreamsConnection object')

    def _get_instance(self):
        iid = os.environ.get('STREAMS_INSTANCE_ID')
        return self._streams_connection.get_instance(id=iid)

    def _get_java_env(self):
        env = super(_DistributedSubmitter4Conn, self)._get_java_env()
        # Need to sure the environment matches the connection.
        sc = self._streams_connection
        env['STREAMS_DOMAIN_ID'] = sc.get_domains()[0].id
        return env


class _SubmitContextFactory(object):
    """
    ContextSubmitter:
        Responsible for performing the correct submission depending on a number of factors, including: the
        presence/absence of a streams install, the type of context, and whether the user seeks to retrieve data via rest
    """
    def __init__(self, graph, config=None, username=None, password=None):
        self.graph = graph
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
            if ctxtype == ContextTypes.STANDALONE:
                raise ValueError(ctxtype + " must be submitted when an IBM Streams install is present.")

        if ctxtype == ContextTypes.DISTRIBUTED:
            logger.debug("Selecting the DISTRIBUTED context for submission")
            return _get_distributed_submitter(self.config, self.graph, self.username, self.password)
        elif ctxtype == ContextTypes.STREAMING_ANALYTICS_SERVICE:
            logger.debug("Selecting the STREAMING_ANALYTICS_SERVICE context for submission")
            ctxtype = ContextTypes.STREAMING_ANALYTICS_SERVICE
            return _StreamingAnalyticsSubmitter(ctxtype, self.config, self.graph)
        elif ctxtype == 'BUNDLE':
            logger.debug("Selecting the BUNDLE context for submission")
            if 'CP4D_URL' in os.environ:
                return _BundleSubmitter(ctxtype, self.config, self.graph)
            if 'VCAP_SERVICES' in os.environ or \
                    ConfigParams.VCAP_SERVICES in self.config or \
                    ConfigParams.SERVICE_DEFINITION in self.config:
                sbs = _SasBundleSubmitter(self.config, self.graph)
                if sbs._remote:
                    return sbs
            return _BundleSubmitter(ctxtype, self.config, self.graph)
        elif ctxtype == 'EDGE':
            logger.debug("Selecting the EDGE context for submission")
            return _EdgeSubmitter(ctxtype, self.config, self.graph)
        elif ctxtype == 'EDGE_BUNDLE':
            logger.debug("Selecting the EDGE_BUNDLE context for submission")
            return _EdgeSubmitter(ctxtype, self.config, self.graph)
        else:
            logger.debug("Using the BaseSubmitter, and passing the context type through to java.")
            return _BaseSubmitter(ctxtype, self.config, self.graph)


# Used to delete the JSON file after it is no longer needed.
def _delete_json(submitter):
    for fn in [submitter.fn, submitter.results_file]:
        if fn and os.path.isfile(fn):
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
        logger.error("Error reading from Java subprocess stdout stream.")
        raise
    finally:
        process.stdout.close()

_JAVA_LOG_LVL = {
    # java.util.logging
    'SEVERE': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO':logging.INFO, 'CONFIG':logging.INFO,
    'FINE:':logging.DEBUG, 'FINER':logging.DEBUG, 'FINEST':logging.DEBUG,
    'FATAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'DEBUG:':logging.DEBUG, 'TRACE':logging.DEBUG
    }

# Used by a thread which polls a subprocess's stderr and writes it to
# a logger or stderr
def _print_process_stderr(process, submitter, progress_fn):
    try:
        while True:
            line = process.stderr.readline()
            if len(line) == 0:
                process.stderr.close()
                break
            line = line.decode("utf-8").strip()
            em = line.rstrip().split(': ', 1)
            if len(em) == 2 and em[0] in _JAVA_LOG_LVL:
                if 'INFO' == em[0] and em[1].startswith('!!-streamsx-'):
                    progress_fn(em[1])
                    continue
                logger.log(_JAVA_LOG_LVL[em[0]], em[1])
                continue
            print(line, file=sys.stderr)
    except:
        logger.error("Error reading from Java subprocess stderr stream.")
        raise
    finally:
        process.stderr.close()

class ContextTypes(object):
    """
        Submission context types.

        A :py:class:`~streamsx.topology.topology.Topology` is submitted using :py:func:`submit` and a context type.
        Submision of a `Topology` generally builds the application into a Streams application
        bundle (sab) file and then submits it for execution in the required context.

        The Streams application bundle contains all the artifacts required by an application such
        that it can be executed remotely (e.g. on a Streaming Analytics service), including
        distributing the execution of the application across multiple resources (hosts).

        The context type defines which context is used for submission.

        The main context types result in a running application and are:

            * :py:const:`STREAMING_ANALYTICS_SERVICE` - Application is submitted to a Streaming Analytics service running on IBM Cloud.
            * :py:const:`DISTRIBUTED` - Application is submitted to an IBM Streams instance.
            * :py:const:`STANDALONE` - Application is executed as a local process, IBM Streams `standalone` application. Typically this is used during development or testing.

        The :py:const:`BUNDLE` context type compiles the application (`Topology`) to produce a
        Streams application bundle (sab file). The bundle is not executed but may subsequently be submitted
        to a Streaming Analytics service or an IBM Streams instance. A bundle may be submitted multiple
        times to services or instances, each resulting in a unique job (running application).
    """
    STREAMING_ANALYTICS_SERVICE = 'STREAMING_ANALYTICS_SERVICE'
    """Submission to Streaming Analytics service running on IBM Cloud.

    The `Topology` is compiled and the resultant Streams application bundle
    (sab file) is submitted for execution on the Streaming Analytics service.

    When **STREAMS_INSTALL** is not set or the :py:func:`submit` `config` parameter has
    :py:const:`~ConfigParams.FORCE_REMOTE_BUILD` set to `True` the compilation of the application
    occurs remotely by the service. This allows creation and submission of Streams applications
    without a local install of IBM Streams.

    When **STREAMS_INSTALL** is set and the :py:func:`submit` `config` parameter has
    :py:const:`~ConfigParams.FORCE_REMOTE_BUILD` set to `False` or not set then the creation of the
    Streams application bundle occurs locally and the bundle is submitted for execution on the service.

    Environment variables:
        These environment variables define how the application is built and submitted.

        * **STREAMS_INSTALL** - (optional) Location of a IBM Streams installation (4.0.1 or later). The install must be running on RedHat/CentOS 7 and `x86_64` architecture.

    """
    DISTRIBUTED = 'DISTRIBUTED'
    """Submission to an IBM Streams instance.

    .. rubric:: IBM Cloud Pak for Data integated configuration

    *Projects (within cluster)*

    The `Topology` is compiled using the Streams build service and submitted
    to an Streams service instance running in the same Cloud Pak for
    Data cluster as the Jupyter notebook or script declaring the application.

    The instance is specified in the configuration passed into :py:func:`submit`. The code that selects a service instance by name is::

        from icpd_core import icpd_util
        cfg = icpd_util.get_service_instance_details(name='instanceName', instance_type="streams")

        topo = Topology()
        ...
        submit(ContextTypes.DISTRIBUTED, topo, cfg)

    The resultant `cfg` dict may be augmented with other values such as
    a :py:class:`JobConfig` or keys from :py:class:`ConfigParams`.

    *External to cluster or project*

    The `Topology` is compiled using the Streams build service and submitted
    to a Streams service instance running in Cloud Pak for Data.

    Environment variables:
        These environment variables define how the application is built and submitted.

        * **CP4D_URL** - Cloud Pak for Data deployment URL, e.g. `https://cp4d_server:31843`
        * **STREAMS_INSTANCE_ID** - Streams service instance name.
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
        * **STREAMS_PASSWORD** - Password for authentication.

    .. rubric:: IBM Cloud Pak for Data standalone configuration

    The `Topology` is compiled using the Streams build service and submitted
    to a Streams service instance using REST apis.

    Environment variables:
        These environment variables define how the application is built and submitted.

        * **STREAMS_BUILD_URL** - Streams build service URL, e.g. when the service is exposed as node port: `https://<NODE-IP>:<NODE-PORT>`
        * **STREAMS_REST_URL** - Streams SWS service (REST API) URL, e.g. when the service is exposed as node port: `https://<NODE-IP>:<NODE-PORT>`
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
        * **STREAMS_PASSWORD** - Password for authentication.

    .. rubric:: IBM Streams on-premise 4.2 & 4.3

    The `Topology` is compiled locally and the resultant Streams application bundle
    (sab file) is submitted to an IBM Streams instance.

    Environment variables:
        These environment variables define how the application is built and submitted.

        * **STREAMS_INSTALL** - Location of a IBM Streams installation (4.2 or 4.3).
        * **STREAMS_DOMAIN_ID** - Domain identifier for the Streams instance.
        * **STREAMS_INSTANCE_ID** - Instance identifier.
        * **STREAMS_ZKCONNECT** - (optional) ZooKeeper connection string for domain (when not using an embedded ZooKeeper)
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.

    .. warning::
        ``streamtool`` is used to submit the job with on-premise 4.2 & 4.3 Streams and requires that ``streamtool`` does not prompt for authentication.  This is achieved by using ``streamtool genkey``.

        .. seealso::
            `Generating authentication keys for IBM Streams <https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.1/com.ibm.streams.cfg.doc/doc/ibminfospherestreams-user-security-authentication-rsa.html>`_

    """

    STANDALONE = 'STANDALONE'
    """Build and execute locally.

    Compiles and executes the `Topology` locally in IBM Streams standalone mode as a separate sub-process.
    Typically used for devlopment and testing.

    The call to :py:func:`submit` return when (if) the application completes. An application
    completes when it has finite source streams and all tuples from those streams have been
    processed by the complete topology. If the source streams are infinite (e.g. reading tweets)
    then the standalone application will not complete.

    Environment variables:
        This environment variables define how the application is built.

        * **STREAMS_INSTALL** - Location of a IBM Streams installation (4.0.1 or later).

    """

    BUNDLE = 'BUNDLE'
    """Create a Streams application bundle.

    The `Topology` is compiled to produce Streams application bundle (sab file).

    The resultant application can be submitted to:
        * Streaming Analytics service using the Streams console or the Streaming Analytics REST api.
        * IBM Streams instance using the Streams console, JMX api or command line ``streamtool submitjob``.
        * Executed standalone for development or testing.

    The bundle must be built on the same operating system version and architecture as the intended running
    environment. For Streaming Analytics service this is currently RedHat/CentOS 7 and `x86_64` architecture.
    
    .. rubric:: IBM Cloud Pak for Data integated configuration

    *Projects (within cluster)*

    The `Topology` is compiled using the Streams build service for 
    a Streams service instance running in the same Cloud Pak for
    Data cluster as the Jupyter notebook or script declaring the application.

    The instance is specified in the configuration passed into :py:func:`submit`. The code that selects a service instance by name is::

        from icpd_core import icpd_util
        cfg = icpd_util.get_service_instance_details(name='instanceName', instance_type="streams")

        topo = Topology()
        ...
        submit(ContextTypes.BUNDLE, topo, cfg)

    The resultant `cfg` dict may be augmented with other values such as
    keys from :py:class:`ConfigParams`.

    *External to cluster or project*

    The `Topology` is compiled using the Streams build service for a Streams service instance running in Cloud Pak for Data.

    Environment variables:
        These environment variables define how the application is built and submitted.

        * **CP4D_URL** - Cloud Pak for Data deployment URL, e.g. `https://cp4d_server:31843`
        * **STREAMS_INSTANCE_ID** - Streams service instance name.
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
        * **STREAMS_PASSWORD** - Password for authentication.

    .. rubric:: IBM Cloud Pak for Data standalone configuration

    The `Topology` is compiled using the Streams build service.

    Environment variables:
        These environment variables define how the application is built.

        * **STREAMS_BUILD_URL** - Streams build service URL, e.g. when the service is exposed as node port: `https://<NODE-IP>:<NODE-PORT>`
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
        * **STREAMS_PASSWORD** - Password for authentication.

    .. rubric:: IBM Streams on-premise 4.2 & 4.3
    
    The `Topology` is compiled using a local IBM Streams installation.

    Environment variables:
        These environment variables define how the application is built.

        * **STREAMS_INSTALL** - Location of a local IBM Streams installation.

    """
    TOOLKIT = 'TOOLKIT'
    """Creates an SPL toolkit.

    `Topology` applications are implemented as an SPL application before compilation into an Streams application
    bundle. This context type produces the intermediate SPL toolkit that is input to the SPL compiler for
    bundle creation.

    .. note::

        `TOOLKIT` is typically only used when diagnosing issues with bundle generation.
    """

    BUILD_ARCHIVE = 'BUILD_ARCHIVE'
    """Creates a build archive.

    This context type produces the intermediate code archive used for bundle creation.

    .. note::

        `BUILD_ARCHIVE` is typically only used when diagnosing issues with bundle generation.
    """

    EDGE = 'EDGE'
    """Submission to build service running on IBM Cloud Pak for Data to create an image for Edge.

    The `Topology` is compiled and the resultant Streams application bundle
    (sab file) is added to an image for Edge.

    .. rubric:: IBM Cloud Pak for Data integated configuration

    *Projects (within cluster)*

    The `Topology` is compiled using the Streams build service for 
    a Streams service instance running in the same Cloud Pak for
    Data cluster as the Jupyter notebook or script declaring the application.

    The instance is specified in the configuration passed into :py:func:`submit`. The code that selects a service instance by name is::

        from streamsx.topology.context import submit, ContextTypes
        from icpd_core import icpd_util
        cfg = icpd_util.get_service_instance_details(name='instanceName', instance_type="streams")

        topo = Topology()
        ...
        submit(ContextTypes.EDGE, topo, cfg)

    The resultant `cfg` dict may be augmented with other values such as
    keys from :py:class:`ConfigParams` or :py:class:`JobConfig`.
    For example, apply `imageName` and `imageTag`::

        from streamsx.topology.context import submit, ContextTypes, JobConfig
        from icpd_core import icpd_util
        cfg = icpd_util.get_service_instance_details(name='instanceName', instance_type="streams")

        topo = Topology()
        ...
        jc = JobConfig()
        jc.raw_overlay = {'edgeConfig': {'imageName':'py-sample-app', 'imageTag':'v1.0'}}
        jc.add(cfg)

        submit(ContextTypes.EDGE, topo, cfg)

    *External to cluster or project*

    The `Topology` is compiled using the Streams build service for a Streams service instance running in Cloud Pak for Data.

    Environment variables:
        These environment variables define how the application is built and submitted.

        * **CP4D_URL** - Cloud Pak for Data deployment URL, e.g. `https://cp4d_server:31843`
        * **STREAMS_INSTANCE_ID** - Streams service instance name.
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
        * **STREAMS_PASSWORD** - Password for authentication.

    Example code to query the base images::

        from streamsx.build import BuildService

        bs = BuildService.of_endpoint(verify=False)
        baseImages = bs.get_base_images()
        print('# images = ' + str(len(baseImages)))
        for i in baseImages:
            print(i.id)
            print(i.registry)

    Example code to select a base image for the image build::

        from streamsx.topology.context import submit, ContextTypes, JobConfig
        topo = Topology()
        ...
        jc = JobConfig()
        jc.raw_overlay = {'edgeConfig': {'imageName':'py-sample-app', 'imageTag':'v1.0', 'baseImage':'streams-base-edge-python-el7:5.3.0.0'}}
        jc.add(cfg)

        submit(ContextTypes.EDGE, topo, cfg)

    .. rubric:: EDGE configuration
    
    The dict *edgeConfig* supports the following fields that are used for the image creation:

        * **imageName** - [str] name of the image
        * **imageTag** - [str] name of the image tag
        * **baseImage** - [str] identify the name of the base image
        * **pipPackages** - [list] identify one or more Python install packages that are to be included in the image.
        * **rpms** - [list] identify one or more linux RPMs that are to be included in the image
        * **locales** - [list] identify one or more locales that are to be included in the image. The first item in the list is the "default" locale. The locales are identified in the java format  <language>_<county>_<variant>. Example: "en_US"

    Example with adding pip packages and rpms::

        jc.raw_overlay = {'edgeConfig': {'imageName': image_name, 'imageTag': image_tag, 'pipPackages':['pandas','numpy'], 'rpms':['atlas-devel']}} 

    """

    EDGE_BUNDLE = 'EDGE_BUNDLE'
    """Creates a Streams application bundle.

    The `Topology` is compiled on build service running on IBM Cloud Pak for Data and the resultant Streams application bundle
    (sab file) is downloaded.

    .. note::

        `EDGE_BUNDLE` is typically only used when diagnosing issues with applications for EDGE.

    """

class ConfigParams(object):
    """
    Configuration options which may be used as keys in :py:func:`submit` `config` parameter.
    """
    VCAP_SERVICES = 'topology.service.vcap'
    """Streaming Analytics service definitions including credentials in **VCAP_SERVICES** format.

    Provides the connection credentials when connecting to a Streaming Analytics service
    using context type :py:const:`~ContextTypes.STREAMING_ANALYTICS_SERVICE`.
    The ``streaming-analytics`` service to use within the service definitions is identified
    by name using :py:const:`SERVICE_NAME`.

    The key overrides the environment variable **VCAP_SERVICES**.

    The value can be:
        * Path to a local file containing a JSON representation of the VCAP services information.
        * Dictionary containing the VCAP services information.

    .. seealso:: :ref:`sas-vcap`
    """
    SERVICE_NAME = 'topology.service.name'
    """Streaming Analytics service name.

    Selects the specific Streaming Analytics service from VCAP service definitions
    defined by the the environment variable **VCAP_SERVICES** or the key :py:const:`VCAP_SERVICES` in the `submit` config.

    .. seealso:: :ref:`sas-service-name`
    """
    SPACE_NAME = 'topology.spaceName'
    """
    Key for a deployment space on a Cloud Pak for Data, when submitted to :py:const:`DISTRIBUTED`
    
    .. versionadded:: 1.17
    """
    CP4D_URL = 'topology.cp4d_url'
    """
    Key for specifying the URL of the Cloud Pak for Data, when submitted to :py:const:`DISTRIBUTED` from within a CP4D project
    
    .. versionadded:: 1.17
    """
    FORCE_REMOTE_BUILD = 'topology.forceRemoteBuild'
    """Force a remote build of the application.

    When submitting to :py:const:`STREAMING_ANALYTICS_SERVICE` a local build of the Streams application bundle
    will occur if the environment variable **STREAMS_INSTALL** is set. Setting this flag to `True` ignores the
    local Streams install and forces the build to occur remotely using the service.

    """
    JOB_CONFIG = 'topology.jobConfigOverlays'
    """
    Key for a :py:class:`JobConfig` object representing a job configuration for a submission.
    """
    STREAMS_CONNECTION = 'topology.streamsConnection'
    """
    Key for a :py:class:`StreamsConnection` object for connecting to a running IBM Streams instance. Only supported for Streams 4.2, 4.3. Requires environment
    variable ``STREAMS_INSTANCE_ID`` to be set.
    """
    SSL_VERIFY = 'topology.SSLVerify'
    """
    Key for the SSL verification value passed to `requests` as its ``verify``
    option for distributed contexts. By default set to `True`.

    .. note:: Only ``True`` or ``False`` is supported. Behaviour is undefined
        when passing a path to a CA_BUNDLE file or directory with
        certificates of trusted CAs.

    .. versionadded:: 1.11
    """
    SERVICE_DEFINITION = 'topology.service.definition'
    """Streaming Analytics service definition.
    Identifies the Streaming Analytics service to use. The definition can be one of

        * The `service credentials` copied from the `Service credentials` page of the service console (not the Streams console).
          Credentials are provided in JSON format. They contain such as the API key and secret, as well as connection information for the service.
        * A JSON object (`dict`) created from the `service credentials`, for example with `json.loads(service_credentials)`
        * A JSON object (`dict`) of the form: ``{ "type": "streaming-analytics", "name": "service name", "credentials": ... }``
          with the `service credentials` as the value of the ``credentials`` key. The value of the ``credentials`` key can
          be a JSON object (`dict`) or a `str` copied from the `Service credentials` page of the service console.

    This key takes precedence over :py:const:`VCAP_SERVICES` and :py:const:`SERVICE_NAME`.

    .. seealso:: :ref:`sas-service-def`
    """

    SC_OPTIONS = 'topology.sc.options'
    """
    Options to be passed to IBM Streams sc command.

    A topology is compiled into a Streams application
    bundle (`sab`) using the SPL compiler ``sc``.

    Additional options to be passed to ``sc``
    may be set using this key. The value can be a
    single string option (e.g. ``--c++std=c++11`` to select C++ 11 compilation)
    or a list of strings for multiple options.

    Setting ``sc`` options may be required when invoking SPL operators
    directly or testing SPL applications.

    .. warning::
        Options that modify the requested submission context (e.g. setting
        a different main composite) or deprecated options should not be specified.
    .. versionadded:: 1.12.10
    """

    _SPLMM_OPTIONS = 'topology.internal.splmm_options'
    """
    TBD
    """


class JobConfig(object):
    """
    Job configuration.

    `JobConfig` allows configuration of job that will result from
    submission of a :py:class:`Topology` (application).

    A `JobConfig` is set in the `config` dictionary passed to :py:func:`~streamsx.topology.context.submit`
    using the key :py:const:`~ConfigParams.JOB_CONFIG`. :py:meth:`~JobConfig.add` exists as a convenience
    method to add it to a submission configuration.

    A `JobConfig` can also be used when submitting a Streams application
    bundle through the Streaming Analytics REST API method :py:meth:`~streamsx.rest_primitives.StreamingAnalyticsService.submit_job`.

    Args:
        job_name(str): The name that is assigned to the job. A job name must be unique within a Streasm instance
            When set to `None` a system generated name is used.
        job_group(str): The job group to use to control permissions for the submitted job.
        preload(bool): Specifies whether to preload the job onto all resources in the instance, even if the job is
            not currently needed on each. Preloading the job can improve PE restart performance if the PEs are
            relocated to a new resource.
        data_directory(str): Specifies the location of the optional data directory. The data directory is a path
            within the cluster that is running the Streams instance.
        tracing: Specify the application trace level. See :py:attr:`tracing`
        space_name(str): Specifies the name of a deployment space on a CloudPak for Data system, which the job is associated with

    Example::

        # Submit a job with the name NewsIngester
        cfg = {}
        job_config = JobConfig(job_name='NewsIngester')
        job_config.add(cfg)
        context.submit('STREAMING_ANALYTICS_SERVICE', topo, cfg)

    .. seealso:: `Job configuration overlays reference <https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.1/com.ibm.streams.ref.doc/doc/submitjobparameters.html>`_
    """
    def __init__(self, job_name=None, job_group=None, preload=False, data_directory=None, tracing=None, space_name=None):
        self.job_name = job_name
        self.job_group = job_group
        self.preload = preload
        self.data_directory = data_directory
        self.tracing = tracing
        self._space_name = space_name
        self._pe_count = None
        self._raw_overlay = None
        self._submission_parameters = dict()
        self._comment = None

    @staticmethod
    def from_overlays(overlays):
        """Create a `JobConfig` instance from a full job configuration
        overlays object.

        All logical items, such as ``comment`` and ``job_name``, are
        extracted from `overlays`. The remaining information in the
        single job config overlay in ``overlays`` is set as ``raw_overlay``.

        Args:
            overlays(dict): Full job configuration overlays object.

        Returns:
            JobConfig: Instance representing logical view of `overlays`.

        .. versionadded:: 1.9
        """
        jc = JobConfig()
        jc.comment = overlays.get('comment')
        if 'jobConfigOverlays' in overlays:
             if len(overlays['jobConfigOverlays']) >= 1:
                 jco = copy.deepcopy(overlays['jobConfigOverlays'][0])

                 # Now extract the logical information
                 if 'jobConfig' in jco:
                     _jc = jco['jobConfig']
                     jc.job_name = _jc.pop('jobName', None)
                     jc.job_group = _jc.pop('jobGroup', None)
                     jc.preload = _jc.pop('preloadApplicationBundles', False)
                     jc.data_directory = _jc.pop('dataDirectory', None)
                     jc.tracing = _jc.pop('tracing', None)

                     for sp in _jc.pop('submissionParameters', []):
                         jc.submission_parameters[sp['name']] = sp['value']

                     if not _jc:
                         del jco['jobConfig']
                 if 'deploymentConfig' in jco:
                     _dc = jco['deploymentConfig']
                     if 'manual' == _dc.get('fusionScheme'):
                         if 'fusionTargetPeCount' in _dc:
                             jc.target_pe_count = _dc.pop('fusionTargetPeCount')
                         if len(_dc) == 1:
                             del jco['deploymentConfig']

                 if jco:
                     jc.raw_overlay = jco
        return jc

    @property
    def space_name(self):
        """
        The deployment space of a Cloud Pak for Data that the job will be associated with.
        """
        return self._space_name
    
    @space_name.setter
    def space_name(self, space_name):
        self._space_name = space_name
        
    @property
    def tracing(self):
        """
        Runtime application trace level.

        The runtime application trace level can be a string with value ``error``, ``warn``, ``info``,
        ``debug`` or ``trace``.

        In addition a level from Python ``logging`` module can be used in with ``CRITICAL`` and ``ERROR`` mapping
        to ``error``, ``WARNING`` to ``warn``, ``INFO`` to ``info`` and ``DEBUG`` to ``debug``.

        Setting tracing to `None` or ``logging.NOTSET`` will result in the job submission using the Streams instance
        application trace level.

        The value of ``tracing`` is the level as a string (``error``, ``warn``, ``info``, ``debug`` or ``trace``)
        or None.

        """
        return self._tracing

    @tracing.setter
    def tracing(self, level):
        if level is None:
            pass
        elif level in {'error', 'warn', 'info', 'debug', 'trace'}:
            pass
        elif level == logging.CRITICAL or level == logging.ERROR:
            level = 'error'
        elif level == logging.WARNING:
            level = 'warn'
        elif level == logging.INFO:
            level = 'info'
        elif level == logging.DEBUG:
            level = 'debug'
        elif level == logging.NOTSET:
            level = None
        else:
            raise ValueError("Tracing value {0} not supported.".format(level))

        self._tracing = level

    @property
    def target_pe_count(self):
        """Target processing element count.

         When submitted against a Streams instance `target_pe_count` provides
         a hint to the scheduler as to how to partition the topology
         across processing elements (processes) for the job execution. When a job
         contains multiple processing elements (PEs) then the Streams scheduler can
         distributed the PEs across the resources (hosts) running in the instance.

         When set to ``None`` (the default) no hint is supplied to the scheduler.
         The number of PEs in the submitted job will be determined by the scheduler.

         The value is only a target and may be ignored when the topology contains
         :py:meth:`~Stream.isolate` calls.

         .. note::
             Only supported in Streaming Analytics service and IBM Streams 4.2 or later.
        """
        if self._pe_count is None:
            return None
        return int(self._pe_count)

    @target_pe_count.setter
    def target_pe_count(self, count):
        if count is not None:
            count = int(count)
            if count < 1:
                raise ValueError("target_pe_count must be greater than 0.")
        self._pe_count = count

    @property
    def raw_overlay(self):
        """Raw Job Config Overlay.

        A submitted job is configured using Job Config Overlay which
        is represented as a JSON. `JobConfig` exposes Job Config Overlay
        logically with properties such as ``job_name`` and ``tracing``.
        This property (as a ``dict``) allows merging of the
        configuration defined by this object and raw representation
        of a Job Config Overlay. This can be used when a capability
        of Job Config Overlay is not exposed logically through this class.

        For example, the threading model can be set by::

            jc = streamsx.topology.context.JobConfig()
            jc.raw_overlay = {'deploymentConfig': {'threadingModel': 'manual'}}

        Any logical items set by this object **overwrite** any set with
        ``raw_overlay``. For example this sets the job name to
        to value set in the constructor (`DBIngest`) not the value
        in ``raw_overlay`` (`Ingest`)::

            jc = streamsx.topology.context.JobConfig(job_name='DBIngest')
            jc.raw_overlay = {'jobConfig': {'jobName': 'Ingest'}}

        .. note:: Contents of ``raw_overlay`` is a ``dict`` that is
             must match a single Job Config Overlay and be serializable
             as JSON to the correct format.

        .. seealso:: `Job Config Overlay reference <https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.1/com.ibm.streams.ref.doc/doc/submitjobparameters.html>`_

        .. versionadded:: 1.9
        """
        return self._raw_overlay

    @raw_overlay.setter
    def raw_overlay(self, raw):
        self._raw_overlay = raw

    @property
    def submission_parameters(self):
        """Job submission parameters.

        Submission parameters values for the job. A `dict` object
        that maps submission parameter names to values.

        .. versionadded:: 1.9
        """
        return self._submission_parameters

    @property
    def comment(self):
        """
        Comment for job configuration.

        The comment does not change the functionality of the job configuration.

        Returns:
            str: Comment text, `None` if it has not been set.

        .. versionadded:: 1.9
        """
        return self._comment

    @comment.setter
    def comment(self, value):
        if value:
            self._comment = str(value)
        else:
            self._comment = None

    def add(self, config):
        """
        Add this `JobConfig` into a submission configuration object.

        Args:
            config(dict): Submission configuration.

        Returns:
            dict: config.
        """
        config[ConfigParams.JOB_CONFIG] = self
        if self.space_name:
            config[ConfigParams.SPACE_NAME] = self.space_name
        return config

    def as_overlays(self):
        """Return this job configuration as a complete job configuration overlays object.

        Converts this job configuration into the full format supported by IBM Streams.
        The returned `dict` contains:

            * ``jobConfigOverlays`` key with an array containing a single job configuration overlay.
            * an optional ``comment`` key containing the comment ``str``.

        For example with this ``JobConfig``::

            jc = JobConfig(job_name='TestIngester')
            jc.comment = 'Test configuration'
            jc.target_pe_count = 2

        the returned `dict` would be::

            {"comment": "Test configuration",
                "jobConfigOverlays":
                    [{"jobConfig": {"jobName": "TestIngester"},
                    "deploymentConfig": {"fusionTargetPeCount": 2, "fusionScheme": "manual"}}]}

        The returned overlays object can be saved as JSON in a file
        using ``json.dump``. A file can be used with job submission
        mechanisms that support a job config overlays file, such as
        ``streamtool submitjob`` or the IBM Streams console.

        Example of saving a ``JobConfig`` instance as a file::
        
            jc = JobConfig(job_name='TestIngester')
            with open('jobconfig.json', 'w') as f:
                json.dump(jc.as_overlays(), f)


        Returns:
            dict: Complete job configuration overlays object built from this object.

        .. versionadded:: 1.9
        """
        return self._add_overlays({})

    def _add_overlays(self, config):
        """
        Add this as a jobConfigOverlays JSON to config.
        """
        if self._comment:
            config['comment'] = self._comment

        jco = {}
        config["jobConfigOverlays"] = [jco]

        if self._raw_overlay:
            jco.update(self._raw_overlay)

        jc = jco.get('jobConfig', {})

        if self.job_name is not None:
            jc["jobName"] = self.job_name
        if self.job_group is not None:
            jc["jobGroup"] = self.job_group
        if self.data_directory is not None:
            jc["dataDirectory"] = self.data_directory
        if self.preload:
            jc['preloadApplicationBundles'] = True
        if self.tracing is not None:
            jc['tracing'] = self.tracing

        if self.submission_parameters:
             sp = jc.get('submissionParameters', [])
             for name in self.submission_parameters:
                  sp.append({'name': str(name), 'value': self.submission_parameters[name]})
             jc['submissionParameters'] = sp

        if jc:
            jco["jobConfig"] = jc

        if self.target_pe_count is not None and self.target_pe_count >= 1:
            deployment = jco.get('deploymentConfig', {})
            deployment.update({'fusionScheme' : 'manual', 'fusionTargetPeCount' : self.target_pe_count})
            jco["deploymentConfig"] = deployment
        return config


class SubmissionResult(object):
    """Passed back to the user after a call to submit.
    Allows the user to use dot notation to access dictionary elements.

    Example accessing result files when using :py:const:`~ContextTypes.BUNDLE`::

        submission_result = submit(ContextTypes.BUNDLE, topology, config)
        print(submission_result.bundlePath)
        ...
        os.remove(submission_result.bundlePath)
        os.remove(submission_result.jobConfigPath)

    Result contains the generated toolkit location when using :py:const:`~ContextTypes.TOOLKIT`::

        submission_result = submit(ContextTypes.TOOLKIT, topology, config)
        print(submission_result.toolkitRoot)

    Result when using :py:const:`~ContextTypes.DISTRIBUTED` depends if the `Topology` is compiled locally and the resultant Streams application bundle
    (sab file) is submitted to an IBM Streams instance or if the `Topology` is compiled on build-service and submitted to an instance in Cloud Pak for Data::

        submission_result = submit(ContextTypes.DISTRIBUTED, topology, config)
        print(submission_result)

    Result contains the generated `image`, `imageDigest`, `submitMetrics` (building the bundle), `submitImageMetrics` (building the image) when using :py:const:`~ContextTypes.EDGE`::

        submission_result = submit(ContextTypes.EDGE, topology, config)
        print(submission_result.image)
        print(submission_result.imageDigest)

"""
    def __init__(self, results):
        self.results = results
        self._submitter = None

    @property
    def job(self):
        """REST binding for the job associated with the submitted build.

        Returns:
            Job: REST binding for running job or ``None`` if connection information was not available or no job was submitted.
        """
        if self._submitter and hasattr(self._submitter, '_job_access'):
            return self._submitter._job_access()
        return None

    def cancel_job_button(self, description=None):
        """Display a button that will cancel the submitted job.

        Used in a Jupyter IPython notebook to provide an interactive
        mechanism to cancel a job submitted from the notebook.

        Once clicked the button is disabled unless the cancel fails.

        A job may be cancelled directly using::

            submission_result = submit(ctx_type, topology, config)
            submission_result.job.cancel()

        Args:

            description(str): Text used as the button description, defaults to value based upon the job name.

        .. warning::
            Behavior when called outside a notebook is undefined.

        .. versionadded:: 1.12
        """
        if not hasattr(self, 'jobId'):
            return
  
        try:
            # Verify we are in a IPython env.
            get_ipython() # noqa : F821
            import ipywidgets as widgets
            if not description:
                description = 'Cancel job: '
                description += self.name if hasattr(self, 'name') else self.job.name
            button = widgets.Button(description=description,
                button_style='danger',
                layout=widgets.Layout(width='40%'))
            out = widgets.Output()
            vb = widgets.VBox([button, out])
            @out.capture(clear_output=True)
            def _cancel_job_click(b):
                b.disabled=True
                print('Cancelling job: id=' + str(self.job.id) + ' ...\n', flush=True)
                try:
                    rc = self.job.cancel()
                    out.clear_output()
                    if rc:
                        print('Cancelled job: id=' + str(self.job.id) + ' : ' + self.job.name + '\n', flush=True)
                    else:
                        print('Job already cancelled: id=' + str(self.job.id) + ' : ' + self.job.name + '\n', flush=True)
                except:
                    b.disabled=False
                    out.clear_output()
                    raise
 
            button.on_click(_cancel_job_click)
            display(vb) # noqa : F821
        except:
            pass

    def __getattr__(self, key):
        if key in self.__getattribute__("results"):
            return self.results[key]
        return self.__getattribute__(key)

    def __setattr__(self, key, value):
        if "results" in self.__dict__:
            results = self.results
            results[key] = value
        else:
            super(SubmissionResult, self).__setattr__(key, value)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    def __delitem__(self, key):
        if key in self.__getattribute__("results"):
            del self.results[key]
            return
        self.__delattr__(key)

    def __contains__(self, item):
        return item in self.results

    def __repr__(self):
        r = copy.copy(self.results)
        if 'streamsConnection' in r:
            del r['streamsConnection']
        return r.__repr__()


def _vcap_from_service_definition(service_def):
    """Turn a service definition into a vcap services
    containing a single service.
    """
    if 'credentials' in service_def:
        credentials = service_def['credentials']
    else:
        credentials = service_def

    service = {}
    service['credentials'] = credentials if isinstance(credentials, dict) else json.loads(credentials)
    service['name'] = _name_from_service_definition(service_def)
    vcap = {'streaming-analytics': [service]}
    return vcap

def _name_from_service_definition(service_def):
    return service_def['name'] if 'credentials' in service_def else 'service'

class _SasBundleSubmitter(_BaseSubmitter):
    """
    A submitter which supports the BUNDLE context
    for Streaming Analytics service.
    """
    def __init__(self, config, graph):
        _BaseSubmitter.__init__(self, 'SAS_BUNDLE', config, graph)
        self._remote = config.get(ConfigParams.FORCE_REMOTE_BUILD) or \
            not 'STREAMS_INSTALL' in os.environ
            
    def _get_java_env(self):
        "Set env vars from connection if set"
        env = super(_SasBundleSubmitter, self)._get_java_env()
        env.pop('STREAMS_DOMAIN_ID', None)
        env.pop('STREAMS_INSTANCE_ID', None)
        env.pop('STREAMS_INSTALL', None)
        return env

def run(topology, config=None, job_name=None, verify=None, ctxtype=ContextTypes.DISTRIBUTED):
    """
    Run a topology in a distributed Streams instance.

    Runs a topology using :py:func:`submit` with context type :py:const:`~ContextTypes.DISTRIBUTED` (by default). The result is running Streams job.

    Args:
        topology(Topology): Application topology to be run.
        config(dict): Configuration for the build.
        job_name(str): Optional job name. If set will override any job name in `config`.
        verify: SSL verification used by requests when using a build service. Defaults to enabling SSL verification.
        ctxtype(str): Context type for submission.

    Returns:
        2-element tuple containing

        - **job** (*Job*): REST binding object for the running job or ``None`` if no job was submitted.
        - **result** (*SubmissionResult*): value returned from ``submit``.

    .. seealso:: :py:const:`~ContextTypes.DISTRIBUTED` for details on how to configure the Streams instance to use.
    .. versionadded:: 1.14
    """
    config = config.copy() if config else dict()
    if job_name:
        if ConfigParams.JOB_CONFIG in config:
            # Ensure the original is not changed
            jc = JobConfig.from_overlays(config[ConfigParams.JOB_CONFIG].as_overlays())
            jc.job_name = job_name
            jc.add(config)
        else:
            JobConfig(job_name=job_name).add(config)

    if verify is not None:
        config[ConfigParams.SSL_VERIFY] = verify

    sr = submit(ctxtype, topology, config=config)
    return sr.job, sr

