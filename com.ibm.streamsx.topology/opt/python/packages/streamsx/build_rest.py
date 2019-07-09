# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017

# TODO revise all documentation.

"""
REST API bindings for IBM® Streams & Streaming Analytics service.

****************
Streams REST API
****************

The Streams REST API provides programmatic access to configuration and status information for IBM Streams objects such as domains, instances, and jobs. 

IBM Cloud Pak for Data
======================

Within ICPD
-----------

:py:meth:`~streamsx.rest_primitives.Instance.of_service` is the entry point to using the Streams REST API bindings,
returning an :py:class:`~streamsx.rest_primitives.Instance`.
The configuration required to connect is obtained from ``ipcd_util.get_service_details`` passing in
the IBM Streams service instance name.

The call to ``ipcd_util.get_service_details`` can be code injected into a Jupyter notebook within
an ICPD project by selecting the service instance.

IBM Streams On-premises
=======================

:py:class:`StreamsConnection` is the entry point to using the Streams REST API bindings.
Through its functions and the returned objects status information
can be obtained for items such as :py:class:`instances <.rest_primitives.Instance>` and :py:class:`jobs <.rest_primitives.Job>`.

****************************
Streaming Analytics REST API
****************************

You can use the Streaming Analytics REST API to manage your service instance and the IBM Streams jobs that are running on the instance. The Streaming Analytics REST API is accessible from IBM Cloud applications that are bound to your service instance or from an application outside of IBM Cloud that is configured with the service instance VCAP information.

:py:class:`StreamingAnalyticsConnection` is the entry point to using the
Streaming Analytics REST API. The function :py:func:`~StreamingAnalyticsConnection.get_streaming_analytics` returns a :py:class:`~.rest_primitives.StreamingAnalyticsService` instance which is the wrapper around the Streaming Analytics REST API. This API allows functions such as :py:meth:`start <streamsx.rest_primitives.StreamingAnalyticsService.start_instance>` and :py:meth:`stop <streamsx.rest_primitives.StreamingAnalyticsService.stop_instance>` the service instance.

In addition `StreamingAnalyticsConnection` extends from :py:class:`StreamsConnection` and thus provides access to the Streams REST API for the service instance.

.. seealso::
    `IBM Streams REST API overview <https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.restapi.doc/doc/restapis.html>`_
        Reference documentation for the Streams REST API.
    `Streaming Analytics REST API <https://console.ng.bluemix.net/apidocs/220-streaming-analytics?&language=node#introduction>`_
        Reference documentation for the Streaming Analytics service REST API.

.. seealso:: :ref:`sas-main`
"""
from future.builtins import *

import os
import json
import logging
import re
import requests
from pprint import pformat
import streamsx.topology.context

import streamsx._streams._version
__version__ = streamsx._streams._version.__version__

from streamsx import st
from .rest_primitives import (Domain, Instance, Installation, RestResource, Toolkit, _StreamsRestClient, StreamingAnalyticsService, _streams_delegator,
    _exact_resource, _IAMStreamsRestClient, _IAMConstants, _get_username,
    _ICPDExternalAuthHandler)

logger = logging.getLogger('streamsx.rest')


# TODO pull up an abstract connection containing common features of this
# and StreamsConnection
class StreamsBuildConnection:
    """Creates a connection to a running distributed IBM Streams instance and exposes methods to retrieve the state of
    that instance.

    Streams maintains information regarding the state of its resources. For example, these resources could include the
    currently running Jobs, Views, PEs, Operators, and Domains. The :py:class:`StreamsConnection` provides methods to
    retrieve that information.

    Args:
        username (str): Username of an authorized Streams user. If ``None``, the username is taken from the ``STREAMS_USERNAME`` environment variable. If the ``STREAMS_USERNAME`` environment variable is not set, the default `streamsadmin` is used.

        password(str): Password for `username` If ``None``, the password is taken from the ``STREAMS_PASSWORD`` environment variable. If the ``STREAMS_PASSWORD`` environment variable is not set, the default `passw0rd` is used to match the Streams Quick Start edition setup.

        resource_url(str): Root URL for IBM Streams REST API. If ``None``, the URL is taken from the ``STREAMS_REST_URL`` environment variable. If the ``REST_URL`` environment variable is not set, then ``streamtool geturl --api`` is used to obtain the URL.

    Example:
        >>> resource_url = "https://streamsqse.localdomain:8443/streams/rest/resources"
        >>> sc = StreamsConnection("streamsadmin", "passw0rd", resource_url)
        >>> sc.session.verify=False  # manually disable SSL verification, if needed
        >>> instances = sc.get_instances()
        >>> jobs_count = 0
        >>> for instance in instances:
        >>>     jobs_count += len(instance.get_jobs())
        >>> print("There are {} jobs across all instances.".format(jobs_count))
        There are 10 jobs across all instances.

    Attributes:
        session (:py:class:`requests.Session`): Requests session object for making REST calls.
    """
    def __init__(self, username=None, password=None, resource_url=None, auth=None):
        """specify username, password, and resource_url"""
        streamsx._streams._version._mismatch_check(__name__)
        if auth:
            pass
        elif username and password:
            # resource URL can be obtained via streamtool geturl or REST call
            pass
        else:
            raise ValueError("Must supply either a IBM Cloud VCAP Services or a username, password"
                             " to the StreamsConnection constructor.")

        if not resource_url and 'STREAMS_BUILD_URL' in os.environ:
            resource_url = os.environ['STREAMS_BUILD_URL']
        
        self._build_url = resource_url
        if auth:
            self.rest_client = _StreamsRestClient(auth)
        else:
            self.rest_client = _StreamsRestClient._of_basic(username, password)
        self.rest_client._sc = self
        self.session = self.rest_client.session
        self._analytics_service = False # TODO What?

    @property
    def build_resource_url(self):
        """str: Endpoint URL for IBM Streams REST build API.  This will be
        None if the build endpoint is not defined for the remote service.

        .. versionadded:: 1.13
        """
        if self._build_url:            
            return re.sub('/builds$','/resources', self._build_url)
        return None


    def _get_elements(self, resource_name, eclass, id=None):
        for resource in self.get_resources():
            if resource.name == resource_name:
                elements = []
                for json_element in resource.get_resource()[resource_name]:
                    if not _exact_resource(json_element, id):
                        continue
                    elements.append(eclass(json_element, self.rest_client))
                return elements

    def _get_element_by_id(self, resource_name, eclass, id):
        """Get a single element matching an id"""
        elements = self._get_elements(resource_name, eclass, id=id)
        if not elements:
            raise ValueError("No resource matching: {0}".format(id))
        if len(elements) == 1:
            return elements[0]
        raise ValueError("Multiple resources matching: {0}".format(id))

    def get_resources(self):
        """Retrieves a list of all known Streams high-level Build REST resources.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.RestResource`: List of all Streams high-level Build REST resources.

        .. versionadded:: 1.13
        """
        json_resources = self.rest_client.make_request(self.build_resource_url)['resources']
        return [RestResource(resource, self.rest_client) for resource in json_resources]

    def get_toolkits(self):
        """Retrieves a list of all installed Streams Toolkits.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.Toolkit`: List of all Toolkit resources.

        .. versionadded:: 1.13
        """
        return self._get_elements('toolkits', Toolkit)
     
    def get_toolkit(self, id):
        """Retrieves available toolit matching a specific toolkit ID.

        Args:
            id (str): Toolkit identifier to retrieve.  This is the name and 
                      version of a toolkit.  For sample, `com.ibm.streamsx.rabbitmq-1.1.3`

        Returns:
            :py:class:`~.rest_primitives.Toolkit`: Toolkit matching `id`.

        Raises:
            ValueError: No matching toolkit exists.

        .. versionadded:: 1.13
        """
        return self._get_element_by_id('toolkits', Toolkit, id)

    def __str__(self):
        return pformat(self.__dict__)

    @staticmethod
    def of_endpoint(endpoint=None, service_name=None, username=None, password=None, verify=None):
        """
        Connect to a Cloud Pak for Data IBM Streams instance from
        outside the cluster.

        Args:
            endpoint(str): Deployment URL for Cloud Pak for Data, e.g. `https://icp4d_server:31843`. Defaults to the environment variable ``ICP4D_DEPLOYMENT_URL``.
            service_name(str): Streams instance name. Defaults to the environment variable ``STREAMS_INSTANCE_ID``.
            username(str): User name to authenticate as. Defaults to the environment variable ``STREAMS_USERNAME`` or the operating system identifier if not set.
            password(str): Password for authentication. Defaults to the environment variable ``STREAMS_PASSWORD`` or the operating system identifier if not set.
            verify: SSL verification. Set to ``False`` to disable SSL verification. Defaults to SSL verification being enabled.

        Returns:
            Instance: Connection to Streams instance or ``None`` of insufficient configuration was provided.

        .. versionadded:: 1.13
        """
        if not endpoint:
            endpoint = os.environ.get('ICP4D_DEPLOYMENT_URL')
            if endpoint:
                if not service_name:
                    service_name = os.environ.get('STREAMS_INSTANCE_ID')
                if not service_name:
                    return None
            else:
                endpoint = os.environ.get('STREAMS_BUILD_URL')
                if not endpoint:
                    return None
        if not endpoint:
            return None
        if not password:
            password = os.environ.get('STREAMS_PASSWORD')
        if not password:
            return None
        username = _get_username(username)

        auth=_ICPDExternalAuthHandler(endpoint, username, password, verify, service_name)

        build_url, _ = StreamsBuildConnection._root_from_endpoint(auth._cfg['connection_info'].get('serviceBuildEndpoint'))

        sc = StreamsBuildConnection(resource_url=build_url, auth=auth)
        if verify is not None:
            sc.rest_client.session.verify = verify
 
        return sc

    @staticmethod
    def _root_from_endpoint(endpoint):
        import urllib.parse as up
        esu = up.urlsplit(endpoint)
        if not esu.path.startswith('/streams/rest/builds'):
            return None, None

        es = endpoint.split('/')
        name = es[len(es)-1]
        root_url = endpoint.split('/streams/rest/builds')[0]
        resource_url = root_url + '/streams/rest/resources'
        return resource_url, name

# TODO is this needed?  If so, a better name is needed.
# removed StreamingAnalyticsConnection
