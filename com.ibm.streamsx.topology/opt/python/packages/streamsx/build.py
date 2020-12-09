# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019


"""
REST API bindings for IBM® Streams Cloud Pak for Data build service.

**********************
Streams Build REST API
**********************

The REST Build API provides programmatic support for creating, submitting and managing Streams builds. 
You can use the REST Build API from any application that can establish an HTTPS connection to the server 
that is running the build service.  The current support includes methods for managing toolkits in the build service 
and for retrieving base images for Edge image builds.

Cloud Pak for Data
==================

:py:meth:`~streamsx.build.BuildService.of_endpoint` or :py:meth:`~streamsx.build.BuildService.of_service` 
is the entry point to using the Streams Build REST API bindings, returning a :py:class:`~streamsx.build.BuildService`.


.. seealso:: :ref:`sas-main`
"""

__all__ = ['BuildService']

import os
import time
import json
import logging
import re
import requests
import socket
import tempfile
import warnings
from pprint import pformat
from urllib import parse
from zipfile import ZipFile

import streamsx.topology.context

import streamsx._streams._version
__version__ = streamsx._streams._version.__version__

from streamsx import st
import streamsx.rest
import streamsx.rest_primitives

logger = logging.getLogger('streamsx.build')


class BuildService(streamsx.rest._AbstractStreamsConnection):
    """IBM Streams build service.

    An instance of a `BuildService` is created using :py:meth:`of_endpoint` or :py:meth:`of_service`.

    .. versionadded:: 1.13
    """
    def __init__(self, username=None, password=None, resource_url=None, buildpools_url=None, auth=None):
        """specify username, password, and resource_url"""
        streamsx._streams._version._mismatch_check(__name__)
        if auth:
            pass
        elif username and password:
            pass
        else:
            raise ValueError("Must supply either an authentication token or a username, password"
                             " to the BuildService constructor.")

        if not resource_url and 'STREAMS_REST_URL' in os.environ:
            resource_url = os.environ['STREAMS_REST_URL']
        
        self._build_url = resource_url
        self._buildpools_url = buildpools_url
        if auth:
            self.rest_client = streamsx.rest_primitives._StreamsRestClient(auth)
        else:
            self.rest_client = streamsx.rest_primitives._StreamsRestClient._of_basic(username, password)
        self.rest_client._sc = self
        self.session = self.rest_client.session
        self._resource_names = None


    @property
    def resource_url(self):
        """str: Endpoint URL for IBM Streams REST build API.

        """
        if self._build_url:
            return re.sub('/builds$','/resources', self._build_url)
        return None

    def get_resources(self):
        """Retrieves a list of all known Streams high-level Build REST resources.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.RestResource`: List of all Streams high-level Build REST resources.

        """
        return super().get_resources()

    def get_toolkits(self, name=None):
        """Retrieves a list of all installed Streams Toolkits.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.Toolkit`: List of all Toolkit resources.

        Args:
           name(str): Return toolkits matching name as a regular expression.

        """
        if not self._resource_names:
            self._resource_names = [res.name for res in self.get_resources()]
        if 'toolkits' in self._resource_names:
            return self._get_elements('toolkits', streamsx.rest_primitives.Toolkit, name=name)

        # from CPD 3.5 on, the toolkits are not a high-level resource of the build service.
        # They are under the application build pool.
        # streamsx.rest_primitives.Toolkit._toolkits_url(self) will return the right URL
        try:
            toolkits = []
            for tk_json_rep in self.rest_client.make_request(streamsx.rest_primitives.Toolkit._toolkits_url(self))['toolkits']:
                if not streamsx.rest_primitives._matching_resource(tk_json_rep, name):
                    continue
                toolkits.append(streamsx.rest_primitives.Toolkit(tk_json_rep, self.rest_client))
            return toolkits
        except:
            return []

    def get_toolkit(self, id):
        """Retrieves available toolkit matching a specific toolkit ID.

        Args:
            id (str): Toolkit identifier to retrieve.  This is the name and 
                      version of a toolkit.  For sample, `com.ibm.streamsx.rabbitmq-1.1.3`

        Returns:
            :py:class:`~.rest_primitives.Toolkit`: Toolkit matching `id`.

        Raises:
            ValueError: No matching toolkit exists.

        """
        if not self._resource_names:
            self._resource_names = [res.name for res in self.get_resources()]
        if 'toolkits' in self._resource_names:
            return self._get_element_by_id('toolkits', streamsx.rest_primitives.Toolkit, id)
        
        # from CPD 3.5 on, the toolkits are not a high-level resource of the build service.
        # They are under the application build pool.
        # streamsx.rest_primitives.Toolkit._toolkits_url(self) will return the right URL
        try:
            toolkits = []
            for tk_json_rep in self.rest_client.make_request(streamsx.rest_primitives.Toolkit._toolkits_url(self))['toolkits']:
                if not streamsx.rest_primitives._exact_resource(tk_json_rep, id):
                    continue
                toolkits.append(streamsx.rest_primitives.Toolkit(tk_json_rep, self.rest_client))
            if not toolkits:
                raise ValueError("No resource matching: {0}".format(id))
            if len(toolkits) == 1:
                return toolkits[0]
            raise ValueError("Multiple resources matching: {0}".format(id))
        except:
            raise ValueError("No resources matching: {0}".format(id))

    def upload_toolkit(self, path):
        """
        Upload a toolkit from a directory in the local filesystem to 
        the Streams build service.

        Multiple versions of a toolkit may be uploaded as long as each has
        a unique version.  If a toolkit is uploaded with a name and version
        matching an existing toolkit, it will not replace the existing
        toolkit, and ``None`` will be returned.
       
        Args:
            path(str): The path to the toolkit directory in the local filesystem.
        Returns:
            Toolkit: The created Toolkit, or ``None`` if it was not uploaded.

        """
        # Handle path does not exist, is not readable, is not a directory
        if not os.path.isdir(path):
            raise ValueError('"' + path + '" is not a path or is not readable')

        # Create a named temporary file
        with tempfile.NamedTemporaryFile(suffix='.zip') as tmpfile:
            filename = tmpfile.name
        
            basedir = os.path.abspath(os.path.join(path, os.pardir))

            with ZipFile(filename, 'w') as zipfile:
                for root, dirs, files in os.walk(path):
                    # Write the directory entry
                    relpath = os.path.relpath(root, basedir)
                    zipfile.write(root, relpath)
                    for file in files:
                        zipfile.write (os.path.join(root, file), os.path.join(relpath, file))
                zipfile.close()
            
                with open(filename, 'rb') as toolkit_fp:
                    res = self.rest_client.session.post(streamsx.rest_primitives.Toolkit._toolkits_url(self),
                        headers = {'Accept' : 'application/json',
                                   'Content-Type' : 'application/zip'},
                        data=toolkit_fp,
                        verify=self.rest_client.session.verify)
                    streamsx.rest_primitives._handle_http_errors(res)
                    new_toolkits = list(streamsx.rest_primitives.Toolkit(t, self.rest_client) for t in res.json()['toolkits'])

                    # It may be possible to upload multiple toolkits in one 
                    # post, but we are only uploading a single toolkit, so the
                    # list of new toolkits is expected to contain only one 
                    # element, and we return it.  It is also possible that no 
                    # new toolkit was returned.

                    if len(new_toolkits) >= 1:
                        return new_toolkits[0]    
                    return None

    def _get_buildPools(self, name=None):
        """Returns the build pools via the new resource type 'buildPools' from Edge enabled build services.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives._BuildPool`: A list of _BuildPool instances or ``None`` if build pools are not available.
        """
        buildpools = self._get_elements('buildPools', streamsx.rest_primitives._BuildPool, name=name)
        if buildpools is None:
            # workaround as long as 'buildPools' resource is not available
            buildpools_url = self._buildpools_url
            if not buildpools_url:
                # We do NOT get the deprecated BuildPool REST resource here as
                # it will not be a build pool of 'image' type
                return None
            buildpools_json = self.rest_client.make_request(buildpools_url)['buildPools']
            buildpools = [streamsx.rest_primitives._BuildPool(json_rep, self.rest_client) for json_rep in buildpools_json]

        return buildpools
    
    def get_base_images(self):
        """Retrieves a list of all installed base images for Edge applications.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.BaseImage`: List of all base images, ``None`` if there are no base images

        .. versionadded:: 1.15
        """
        buildpools = self._get_buildPools()
        if not buildpools:
            # None or empty list
            return None
        images = []
        for pool in buildpools:
            if hasattr(pool, 'baseimages'):
                images.extend([streamsx.rest_primitives.BaseImage(json_rep, self.rest_client) for json_rep in self.rest_client.make_request(pool.baseimages)['images']])
        if images:
            return images
        return None

    @staticmethod
    def _is_service_def(config):
        return 'connection_info' in config and config.get('type', None) == 'streams' and 'service_token' in config

    @staticmethod
    def _find_service_def(config):
        """
        Returns a service definition dict with connection_info, type, service_name, service_id, service_namespace,
        refreshed service_token, service_token_expire, and user_token
        """
        if BuildService._is_service_def(config):
            service = config
        else:
            service = config.get(streamsx.topology.context.ConfigParams.SERVICE_DEFINITION)

        if service and BuildService._is_service_def(service):
            svc_info = {}
            svc_info['connection_info'] = service['connection_info']
            svc_info['type'] = service['type']
            svcRestEndpoint = service['connection_info']['serviceRestEndpoint']
            if svcRestEndpoint.endswith('/'):
                svc_name = svcRestEndpoint.split('/')[-2]
            else:
                svc_name = svcRestEndpoint.split('/')[-1]
            svc_info['service_name'] = svc_name
            if 'user_token' in service:
                svc_info['user_token'] = service['user_token']
                svc_info['service_token'] = service['user_token']
            else:
                # try to get token from environmnet variable USER_ACCESS_TOKEN
                import os
                if 'USER_ACCESS_TOKEN' in os.environ:
                    tok = os.environ['USER_ACCESS_TOKEN']
                    svc_info['user_token'] = tok
                    svc_info['service_token'] = tok
                else:
                    raise ValueError('cannot find the user token in environment nor in service details')
            # the user token expires in ~ 11000 years; now + 10 years are sufficient. 
            svc_info['service_token_expire'] = int((time.time() + 10 * 365 * 86400)*1000)
            try:
                # add service_id and service_namespace
                from icpd_core import icpd_util
                con = icpd_util.get_connection(name=svc_name, conn_class='svc')
                if con:
                    svc_info['service_id'] = con['service_instance_id']
                    svc_info['service_namespace'] = con['service_instance_namespace']
            except:
                pass
            
            return svc_info
        return None

    @staticmethod
    def of_service(config):
        """Connect to a Cloud Pak for Data IBM Streams build service instance.

        The instance is specified in `config`. The configuration may be code injected from the list of services
        in a Jupyter notebook running in ICPD or manually created. The code that selects a service instance by name is::

            from icpd_core import ipcd_util
            cfg = icpd_util.get_service_details(name='instanceName', instance_type='streams')

            buildService = BuildService.of_service(cfg)
            
        SSL host verification is disabled by setting :py:const:`~streamsx.topology.context.ConfigParams.SSL_VERIFY`
        to ``False`` within `config` before calling this method::

            from icpd_core import ipcd_util
            cfg = icpd_util.get_service_details(name='instanceName', instance_type='streams')

            cfg[ConfigParams.SSL_VERIFY] = False
            buildService = BuildService.of_service(cfg)

        Args:
            config(dict): Configuration of IBM Streams service instance.

        Returns:
            :py:class:`BuildService`: Connection to Streams build service.

        .. note:: Only supported when running within the ICPD cluster,
            for example in a Jupyter notebook within an ICPD project.

        .. versionadded:: 1.15
        """
        service = BuildService._find_service_def(config)
        if not service:
            raise ValueError()
        # service_name is the instance name
        service_name = service['service_name']
        auth = streamsx.rest_primitives._ICPDAuthHandler(service_name, service['service_token'])
        resource_url = BuildService._root_from_endpoint(service['connection_info'].get('serviceBuildEndpoint'))
        buildpools_url = service['connection_info'].get('serviceBuildPoolsEndpoint', None)
        sc = BuildService(resource_url=resource_url, buildpools_url=buildpools_url, auth=auth)
        if streamsx.topology.context.ConfigParams.SSL_VERIFY in config:
            sc.rest_client.session.verify = config[streamsx.topology.context.ConfigParams.SSL_VERIFY]
        return sc

    @staticmethod
    def of_endpoint(endpoint=None, service_name=None, username=None, password=None, verify=None):
        """
        Connect to a Cloud Pak for Data IBM Streams build service instance.

        Two configurations are supported.

        .. rubric:: Integrated configuration

        The build service is bound to a Streams instance and is defined
        using the Cloud Pak for Data deployment endpoint (URL) and
        the Streams service name.

        The endpoint is passed in as `endpoint` defaulting the the
        environment variable ``CP4D_URL``.
        An example is `https://cp4d_server:31843`.

        The Streams service name is passed in as `service_name` defaulting
        to the environment variable ``STREAMS_INSTANCE_ID``.

        .. rubric:: Standalone configuration

        A build service is independent of a Streams instance and is defined
        using the build service endpoint.

        The endpoint is passed in as `endpoint` defaulting the the
        environment variable ``STREAMS_BUILD_URL``.
        An example is `https://build_service:34679`.

        No service name is specified thus `service_name` should be passed
        as ``None`` or not set.

        Args:
            endpoint(str): Endpoint defining the build service.
            service_name(str): Streams instance name for a integrated configuration.  This value is ignored for a standalone configuration.
            username(str): User name to authenticate as. Defaults to the environment variable ``STREAMS_USERNAME`` or the operating system identifier if not set.
            password(str): Password for authentication. Defaults to the environment variable ``STREAMS_PASSWORD`` or the operating system identifier if not set.
            verify: SSL verification. Set to ``False`` to disable SSL verification. Defaults to SSL verification being enabled.
       
        Returns:
            :py:class:`BuildService`: Connection to Streams build service or ``None`` if insufficient configuration was provided.
        """
        possible_integ = True
        if not endpoint:
            endpoint = os.environ.get('CP4D_URL')
            if not endpoint:
                endpoint = os.environ.get('STREAMS_BUILD_URL')
                if not endpoint:
                    return None
                possible_integ = False
                if service_name:
                    warnings.warn("Service name ignored for standalone configuration", UserWarning, stacklevel=2)
                    service_name = None

        if possible_integ and not service_name:
            service_name = os.environ.get('STREAMS_INSTANCE_ID')
        if not password:
            password = os.environ.get('STREAMS_PASSWORD')
        if not password:
            return None
        username = streamsx.rest_primitives._get_username(username)

        if service_name:
            # this is an integrated config
            auth=streamsx.rest_primitives._ICPDExternalAuthHandler(endpoint, username, password, verify, service_name)
            build_url = BuildService._root_from_endpoint(auth._cfg['connection_info'].get('serviceBuildEndpoint'))
            buildpools_ep = auth._cfg['connection_info'].get('serviceBuildPoolsEndpoint', None)
            if buildpools_ep:
                buildpools_url = buildpools_ep
            else:
                buildpools_url = None
            service_name=auth._cfg['service_name']
            sc = BuildService(resource_url=build_url, buildpools_url=buildpools_url, auth=auth)
            if verify is not None:
                sc.rest_client.session.verify = verify

        else:
            # This is a stand-alone config
            parsed = parse.urlparse(endpoint)
            build_url = parse.urlunparse((parsed.scheme, parsed.netloc, "/streams/rest/resources", None, None, None))
            # TODO: standalone config of CPD 3.5
            
            # Create a connection using basic authentication.  This will be
            # used to attempt to discover the security service.  If there
            # is no security service, this connection will continue to be
            # used; otherwise, it will be replaced by one using the 
            # security service.
            sc = BuildService(resource_url=build_url, username=username, password=password)
            if verify is not None:
                sc.rest_client.session.verify = verify
            for resource in sc.get_resources():
                if resource.name == 'accessTokens':
                    atinfo = parse.urlparse(resource.resource)
                    try:
                        # If we are external to the cluster then
                        # We can't resolve the security manager
                        # so revert to basic auth
                        socket.gethostbyname(atinfo.hostname)
                        auth=streamsx.rest_primitives._JWTAuthHandler(resource.resource, username, password, verify)
                        sc = BuildService(resource_url=build_url, buildpools_url=None, auth=auth)
                        if verify is not None:
                            sc.rest_client.session.verify = verify
                        break
                    except:
                        pass
            else:
                # No security service could be found.  We can try to proceed 
                # with basic authentication.
                pass
 
        return sc

    @staticmethod
    def _root_from_endpoint(endpoint):
        """
        Returns the build resources URL from the build endpoint
        """
        import urllib.parse as up
        esu = up.urlsplit(endpoint)
        if esu.path.startswith('/streams-build/instances/'):
            # external URL; CPD 2.5 ... < 3.5
            return endpoint.replace('/streams-build/instances', '/streams-build-resource/instances', 1)
        elif esu.path.startswith('/streams_build_service/v1/'):
            # external URL, CPD >= 3.5
            return up.urlunsplit((esu.scheme, esu.netloc, esu.path.replace('/builds', '/roots'), None, None))
        elif esu.path.startswith('/streams/rest/builds'):
            # internal URL; CPD 2.5 ... < 3.5
            return up.urlunsplit((esu.scheme, esu.netloc, esu.path.replace('/builds', '/resources'), None, None))
        elif esu.path.startswith('/streams/v1/builds'):
            # internal URL, CPD >= 3.5
            return up.urlunsplit((esu.scheme, esu.netloc, esu.path.replace('/builds', '/roots'), None, None))
        
        raise ValueError("Can't convert build endpoint " + endpoint + " into resource URL")

    def __str__(self):
        return pformat(self.__dict__)

