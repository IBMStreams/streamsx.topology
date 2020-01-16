# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019


"""
REST API bindings for IBM® Streams Cloud Pak for Data build service.

**********************
Streams Build REST API
**********************

The REST Build API provides programmatic support for creating, submitting and managing Streams builds. You can use the REST Build API from any application that can establish an HTTPS connection to the server that is running the build service.  The current support includes only methods for managing toolkits in the build service.

Cloud Pak for Data
==================

:py:meth:`~streamsx.rest_primitives.Instance.of_endpoint` is the entry point to using the Streams Build REST API bindings,
returning an :py:class:`~streamsx.build.BuildService`.


.. seealso:: :ref:`sas-main`
"""

__all__ = ['BuildService']

import os
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
from .rest import _AbstractStreamsConnection
from .rest_primitives import (Domain, Instance, Installation, RestResource, Toolkit, _StreamsRestClient, StreamingAnalyticsService, _streams_delegator,
    _exact_resource, _IAMStreamsRestClient, _IAMConstants, _get_username,
    _ICPDExternalAuthHandler, _handle_http_errors, _JWTAuthHandler)

logger = logging.getLogger('streamsx.build')


class BuildService(_AbstractStreamsConnection):
    """IBM Streams build service.

    A instance of a `BuildService` is created using :py:meth:`of_endpoint`.

    .. versionadded:: 1.13
    """
    def __init__(self, username=None, password=None, resource_url=None, auth=None):
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
        if auth:
            self.rest_client = _StreamsRestClient(auth)
        else:
            self.rest_client = _StreamsRestClient._of_basic(username, password)
        self.rest_client._sc = self
        self.session = self.rest_client.session

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
        return self._get_elements('toolkits', Toolkit, name=name)
     
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
        return self._get_element_by_id('toolkits', Toolkit, id)

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
                    res = self.rest_client.session.post(Toolkit._toolkits_url(self),
                        headers = {'Accept' : 'application/json',
                                   'Content-Type' : 'application/zip'},
                        data=toolkit_fp,
                        verify=self.rest_client.session.verify)
                    _handle_http_errors(res)
                    new_toolkits = list(Toolkit(t, self.rest_client) for t in res.json()['toolkits'])

                    # It may be possible to upload multiple toolkits in one 
                    # post, but we are only uploading a single toolkit, so the
                    # list of new toolkits is expected to contain only one 
                    # element, and we return it.  It is also possible that no 
                    # new toolkit was returned.

                    if len(new_toolkits) >= 1:
                        return new_toolkits[0]    
                    return None

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
            BuildService: Connection to Streams build service or ``None`` of insufficient configuration was provided.

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
        username = _get_username(username)

        if service_name:
            # this is an integrated config
            auth=_ICPDExternalAuthHandler(endpoint, username, password, verify, service_name)
            build_url = BuildService._root_from_endpoint(auth._cfg['connection_info'].get('serviceBuildEndpoint'))
            service_name=auth._cfg['service_name']
            sc = BuildService(resource_url=build_url, auth=auth)
            if verify is not None:
                sc.rest_client.session.verify = verify

        else:
            # This is a stand-alone config
            parsed = parse.urlparse(endpoint)
            build_url = parse.urlunparse((parsed.scheme, parsed.netloc, "/streams/rest/resources", None, None, None))

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
                        auth=_JWTAuthHandler(resource.resource, username, password, verify)
                        sc = BuildService(resource_url=build_url, auth=auth)
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
        import urllib.parse as up
        esu = up.urlsplit(endpoint)
        # CPD 2.5
        if esu.path.startswith('/streams-build/instances/'):
            return endpoint.replace('/streams-build/instances', '/streams-build-resource/instances', 1)

        if not esu.path.startswith('/streams/rest/builds'):
            return None

        es = endpoint.split('/')
        root_url = endpoint.split('/streams/rest/builds')[0]
        resource_url = root_url + '/streams/rest/resources'
        return resource_url

    def __str__(self):
        return pformat(self.__dict__)

