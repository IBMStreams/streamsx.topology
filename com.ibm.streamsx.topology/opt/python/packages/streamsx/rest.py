# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019

"""
REST API bindings for IBM® Streams & Streaming Analytics service.

****************
Streams REST API
****************

The Streams REST API provides programmatic access to configuration and status information for IBM Streams objects such as domains, instances, and jobs. 

IBM Cloud Pak for Data (Streams 5)
==================================

Integrated configuration within project
---------------------------------------

:py:meth:`~streamsx.rest_primitives.Instance.of_service` is the entry point to using the Streams REST API bindings,
returning an :py:class:`~streamsx.rest_primitives.Instance`.
The configuration required to connect is obtained from ``ipcd_util.get_service_details`` passing in the IBM Streams service instance name.

Integrated & standalone configurations
--------------------------------------

:py:meth:`~streamsx.rest_primitives.Instance.of_endpoint` is the entry point
to using the Streams REST API bindings, returning an :py:class:`~streamsx.rest_primitives.Instance`.

IBM Streams On-premises (4.2, 4.3)
==================================

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

__all__ = ['StreamsConnection', 'StreamingAnalyticsConnection']

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
import streamsx.rest_primitives

logger = logging.getLogger('streamsx.rest')

class _AbstractStreamsConnection(object):
    # This is an abstract property, but there seems to be no way to enforce
    # that in python 2.7.
    @property
    def resource_url(self):
        pass

    def get_resources(self):
        """Retrieves a list of all known Streams high-level REST resources.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.RestResource`: List of all Streams high-level REST resources.
        """
        json_resources = self.rest_client.make_request(self.resource_url)['resources']
        return [streamsx.rest_primitives.RestResource(resource, self.rest_client) for resource in json_resources]

    def _get_elements(self, resource_name, eclass, id=None, name=None):
        for resource in self.get_resources():
            if resource.name == resource_name:
                elements = []
                for json_element in resource.get_resource()[resource_name]:
                    if not streamsx.rest_primitives._exact_resource(json_element, id):
                        continue
                    if not streamsx.rest_primitives._matching_resource(json_element, name):
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

class StreamsConnection(_AbstractStreamsConnection):
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
        elif st._has_local_install:
            # Assume quickstart
            username = os.getenv("STREAMS_USERNAME", "streamsadmin")
            password = os.getenv("STREAMS_PASSWORD", "passw0rd")
        else:
            raise ValueError("Must supply either a IBM Cloud VCAP Services or a username, password"
                             " to the StreamsConnection constructor.")

        if not resource_url and 'STREAMS_REST_URL' in os.environ:
            resource_url = os.environ['STREAMS_REST_URL']
        
        self._resource_url = resource_url
        if auth:
            self.rest_client = streamsx.rest_primitives._StreamsRestClient(auth)
        else:
            self.rest_client = streamsx.rest_primitives._StreamsRestClient._of_basic(username, password)
        self.rest_client._sc = self
        self.session = self.rest_client.session
        self._analytics_service = False
        self._delegator_impl = None
        self._domains = None

    @property
    def _delegator(self):
        if self._delegator_impl is None:
            self._delegator_impl = streamsx.rest_primitives._streams_delegator(self)
        return self._delegator_impl

    @property
    def resource_url(self):
        """str: Root URL for IBM Streams REST API"""
        self._resource_url = self._resource_url or st.get_rest_api()
        return self._resource_url

    def get_domains(self):
        """Retrieves available domains.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.Domain`: List of available domains
        """
        # Domains are fixed and actually only one per REST api.
        if self._domains is None:
            self._domains = self._get_elements('domains', streamsx.rest_primitives.Domain)
        return self._domains

    def get_domain(self, id):
        """Retrieves available domain matching a specific domain ID

        Args:
            id (str): domain ID

        Returns:
            :py:class:`~.rest_primitives.Domain`: Domain matching `id`

        Raises:
            ValueError: No matching domain exists.
        """
        return self._get_element_by_id('domains', streamsx.rest_primitives.Domain, id)
  
    def get_instances(self):
        """Retrieves available instances.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.Instance`: List of available instances
        """
        return self._get_elements('instances', streamsx.rest_primitives.Instance)

    def get_instance(self, id):
        """Retrieves available instance matching a specific instance ID.

        Args:
            id (str): Instance identifier to retrieve.

        Returns:
            :py:class:`~.rest_primitives.Instance`: Instance matching `id`.

        Raises:
            ValueError: No matching instance exists or multiple matching instances exist.
        """
        return self._get_element_by_id('instances', streamsx.rest_primitives.Instance, id)

    def get_installations(self):
        """Retrieves a list of all known Streams installations.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.Installation`: List of all Installation resources.
        """
        return self._get_elements('installations', streamsx.rest_primitives.Installation)

    def get_resources(self):
        """Retrieves a list of all known Streams high-level REST resources.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.RestResource`: List of all Streams high-level REST resources.
        """
        return super().get_resources()

    def __str__(self):
        return pformat(self.__dict__)

# For Streams 5 onwards we get a rest URL that points
# directly to the Instance. We then create an Instance
# directly and use a simplified Instance specifc StreamsConnection.
# 
class _InstanceSc(StreamsConnection):
    @staticmethod
    def get_instance(url, auth, verify):
        import urllib.parse as up

        es = up.urlsplit(url)
        if es.path.startswith('/streams-rest'):
            # CPD < 3.5
            resource_url = up.urlunsplit((es.scheme, es.netloc, es.path.replace('/streams-rest/', '/streams-resource/', 1), None, None))
        elif es.path.startswith('/streams_instance/v1'):
            # CPD >= 3.5
            roots = 'roots' if es.path.endswith('/') else '/roots'
            resource_url = up.urlunsplit((es.scheme, es.netloc, es.path + roots, None, None))

        rest_client = streamsx.rest_primitives._StreamsRestClient(auth)
        if verify is not None:
            rest_client.session.verify = verify
        # as a side-effect in the constructor, the rest_client get the _InstanceSc object as the member '_sc',
        # the _InstanceSc object itself is not needed here, but the constructor invocation must not be removed.
        _InstanceSc(resource_url, rest_client)
        return streamsx.rest_primitives.Instance(rest_client.make_request(url), rest_client)

    def __init__(self, resource_url, rest_client):
        self._resource_url = resource_url
        self.rest_client = rest_client
        self.rest_client._sc = self
        self._delegator_impl = streamsx.rest_primitives._StreamsRestDelegator(rest_client)
        self.session = self.rest_client.session
        self._domains = None

class StreamingAnalyticsConnection(StreamsConnection):
    """Creates a connection to a running Streaming Analytics service and exposes methods
    to retrieve the state of the service and its instance.

    Args:
        vcap_services (str, optional): VCAP services (JSON string or a filename whose content contains a JSON string).
            If not specified, it uses the value of **VCAP_SERVICES** environment variable.
        service_name (str, optional): Name of the Streaming Analytics service.
            If not specified, it uses the value of **STREAMING_ANALYTICS_SERVICE_NAME** environment variable.

    Example:
        >>> # Assume environment variable VCAP_SERVICES has correct information
        >>> sc = StreamingAnalyticsConnection(service_name='Streaming-Analytics')
        >>> print(sc.get_streaming_analytics().get_instance_status())
        {'plan': 'Standard', 'state': 'STARTED', 'enabled': True, 'status': 'running'}

    .. seealso: :ref:`sas-access`
    """
    def __init__(self, vcap_services=None, service_name=None):
        streamsx._streams._version._mismatch_check(__name__)
        self.service_name = service_name or os.environ.get('STREAMING_ANALYTICS_SERVICE_NAME')
        self.credentials = _get_credentials(_get_vcap_services(vcap_services), self.service_name)
        self._resource_url = None

        self._iam = False
        if streamsx.rest_primitives._IAMConstants.V2_REST_URL in self.credentials and not ('userid' in self.credentials and 'password' in self.credentials):
            self._iam = True

        if self._iam:
            self.rest_client = streamsx.rest_primitives._IAMStreamsRestClient._create(self.credentials)
        else:
            self.rest_client = streamsx.rest_primitives._StreamsRestClient._of_basic(self.credentials['userid'], self.credentials['password'])
        self.rest_client._sc = self
        self.session = self.rest_client.session
        self._analytics_service = True
        self._sas = streamsx.rest_primitives.StreamingAnalyticsService(self.rest_client, self.credentials)
        self._delegator_impl = self._sas._delegator
        self._domains = None

    @staticmethod
    def of_definition(service_def):
       """Create a connection to a Streaming Analytics service.

       The single service is defined by `service_def` which can be one of

               * The `service credentials` copied from the `Service credentials` page of the service console (not the Streams console). Credentials are provided in JSON format. They contain such as the API key and secret, as well as connection information for the service. 
               * A JSON object (`dict`) of the form: ``{ "type": "streaming-analytics", "name": "service name", "credentials": {...} }`` with the `service credentials` as the value of the ``credentials`` key.

       Args:
           service_def(dict): Definition of the service to connect to.

       Returns:
           StreamingAnalyticsConnection: Connection to defined service.
       """
       vcap_services = streamsx.topology.context._vcap_from_service_definition(service_def)
       service_name = streamsx.topology.context._name_from_service_definition(service_def)
       return StreamingAnalyticsConnection(vcap_services, service_name)
        
    @property
    def resource_url(self):
        """str: Root URL for IBM Streams REST API"""
        if self._iam:
            self._resource_url = self._resource_url or _get_iam_rest_api_url_from_creds(self.rest_client, self.credentials)
        else:
            self._resource_url = self._resource_url or _get_rest_api_url_from_creds(self.session, self.credentials)
        return self._resource_url

    def get_streaming_analytics(self):
        """Returns a :py:class:`~.rest_primitives.StreamingAnalyticsService` to allow further interaction with
        the Streaming Analytics service.

        Returns:
            :py:class:`~.rest_primitives.StreamingAnalyticsService`:
                Object for interacting with the Streaming Analytics service.
        """
        return self._sas


def _get_vcap_services(vcap_services=None):
    """Retrieves the VCAP Services information from the `ConfigParams.VCAP_SERVICES` field in the config object. If
    `vcap_services` is not specified, it takes the information from VCAP_SERVICES environment variable.

    Args:
        vcap_services (str): Try to parse as a JSON string, otherwise, try open it as a file.
        vcap_services (dict): Return the dict as is.

    Returns:
        dict: A dict representation of the VCAP Services information.

    Raises:
        ValueError:
            * if `vcap_services` nor VCAP_SERVICES environment variable are specified.
            * cannot parse `vcap_services` as a JSON string nor as a filename.
    """
    vcap_services = vcap_services or os.environ.get('VCAP_SERVICES')
    if not vcap_services:
        raise ValueError(
            "VCAP_SERVICES information must be supplied as a parameter or as environment variable 'VCAP_SERVICES'")
    # If it was passed to config as a dict, simply return it
    if isinstance(vcap_services, dict):
        return vcap_services
    try:
        # Otherwise, if it's a string, try to load it as json
        vcap_services = json.loads(vcap_services)
    except json.JSONDecodeError:
        # If that doesn't work, attempt to open it as a file path to the json config.
        try:
            with open(vcap_services) as vcap_json_data:
                vcap_services = json.load(vcap_json_data)
        except:
            raise ValueError("VCAP_SERVICES information is not JSON or a file containing JSON:", vcap_services)
    return vcap_services


def _get_credentials(vcap_services, service_name=None):
    """Retrieves the credentials of the VCAP Service of the specified `service_name`.  If
    `service_name` is not specified, it takes the information from STREAMING_ANALYTICS_SERVICE_NAME environment
    variable.

    Args:
        vcap_services (dict): A dict representation of the VCAP Services information.
        service_name (str): One of the service name stored in `vcap_services`

    Returns:
        dict: A dict representation of the credentials.

    Raises:
        ValueError:  Cannot find `service_name` in `vcap_services`
    """
    service_name = service_name or os.environ.get('STREAMING_ANALYTICS_SERVICE_NAME', None)
    # Get the service corresponding to the SERVICE_NAME
    services = vcap_services['streaming-analytics']
    creds = None
    for service in services:
        if service['name'] == service_name:
            creds = service['credentials']
            break

    # If no corresponding service is found, error
    if creds is None:
        raise ValueError("Streaming Analytics service " + str(service_name) + " was not found in VCAP_SERVICES")
    return creds


def _get_rest_api_url_from_creds(session, credentials):
    """Retrieves the Streams REST API URL from the provided credentials.
    Args:
        session (:py:class:`requests.Session`): A Requests session object for making REST calls
        credentials (dict): A dict representation of the credentials.
    Returns:
        str: The remote Streams REST API URL.
    """
    resources_url = credentials['rest_url'] + credentials['resources_path']
    try:
        response_raw = session.get(resources_url, auth=(credentials['userid'], credentials['password']), timeout=streamsx.rest_primitives._TIMEOUTS['GET'])
        response = response_raw.json()
    except:
        logger.error("Error while retrieving rest REST url from: " + resources_url)
        raise

    response_raw.raise_for_status()

    rest_api_url = response['streams_rest_url'] + '/resources'
    return rest_api_url

def _get_iam_rest_api_url_from_creds(rest_client, credentials):
    """Retrieves the Streams REST API URL from the provided credentials using iam authentication.
    Args:
        rest_client (:py:class:`rest_primitives._IAMStreamsRestClient`): A client  for making REST calls using IAM authentication
        credentials (dict): A dict representation of the credentials.
    Returns:
        str: The remote Streams REST API URL.
    """
    res = rest_client.make_request(credentials[streamsx.rest_primitives._IAMConstants.V2_REST_URL])
    base = res['streams_self']
    end = base.find('/instances')
    return base[:end] + '/resources'
    
