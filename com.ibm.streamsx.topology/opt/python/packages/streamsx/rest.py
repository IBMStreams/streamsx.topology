# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
import requests
import os
import json
import logging
import streamsx.st as st

from .rest_primitives import Domain, Instance, Installation, Resource, _StreamsRestClient, StreamingAnalyticsService,  _exact_resource
from .rest_errors import ViewNotFoundError
from pprint import pformat

logger = logging.getLogger('streamsx.rest')


class StreamsConnection:
    """Creates a connection to a running Streams instance and exposes methods to retrieve the state of that instance.

    Streams maintains information regarding the state of its resources. For example, these resources could include the
    currently running Jobs, Views, PEs, Operators, and Domains. The StreamsConnection provides methods to retrieve that
    information.

    ``StreamsConnection`` connects to  distributed IBM Streams instance.

    Args:
        username: Username of an authorized Streams user.
        password: Password for user
        resource_url: Root URL for IBM Streams REST API.

    Example:
        >>> # If the resource url does not have a valid SSL certificate, you can disable the SSL verification
        >>> _resource_url = "https://streamsqse.localdomain:8443/streams/rest/resources"
        >>> sc = StreamsConnection(username="streamsadmin", password="passw0rd", resource_url=_resource_url)
        >>> sc.session.verify=False
        >>> instances = sc.get_instances()
        >>> jobs_count = 0
        >>> for instance in instances:
        ...    jobs_count += len(instance.get_jobs())
        >>> print("There are " + jobs_count + " jobs across all instances.")

    """
    def __init__(self, username=None, password=None, resource_url=None):
        # manually specify username, password, and resource_url
        if username and password:
            # resource URL can be obtained via streamtool geturl or REST call
            pass
        elif st._has_local_install:
            # Assume quickstart
            username = 'streamsadmin'
            password = 'passw0rd'
        else:
            raise ValueError("Must supply either a Bluemix VCAP Services or a username, password"
                             " to the StreamsConnection constructor.")

        self._resource_url = resource_url
        self.rest_client = _StreamsRestClient(username, password)
        self.rest_client._sc = self
        self.session = self.rest_client.session
        self._analytics_service = False

    @property
    def resource_url(self):
        if self._resource_url is None:
            self._resource_url = st.get_rest_api()
        return self._resource_url

    def _get_elements(self, resource_name, eclass, id=None):
        elements = []
        for resource in self.get_resources():
            if resource.name == resource_name:
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

    def get_domains(self):
        """Retrieve available domains.


        Returns:
            list of :py:class:`Domain` instances.

        """
        return self._get_elements('domains')

    def get_domain(self, id):
        """Retrieve available domains.

                Args:
                    id: Domain identifier to retrieve.

                Returns:
                    :py:class:`Domain`: Domain matching ``id``.

                Raises:
                    No matching domain exists or multiple matching domains exist.
        """
        return self._get_element_by_id('domains', Domain, id)

    def get_instances(self):
        """Retrieve available instances.

        """
        return self._get_elements('instances', Instance, id=id)

    def get_instance(self, id):
        """Retrieve available domains.

            Args:
                id: Instance identifier to retrieve.

            Returns:
                :py:class:`Instance`: Instance matching ``id``.

            Raises:
                No matching instance exists or multiple matching instances exist.
        """
        return self._get_element_by_id('instances', Instance, id)

    def get_installations(self):
        """Retrieves a list of all known Streams installations.

        :return: Returns a list of all Installation resources.
        :type return: list.
        """
        return self._get_elements('installations', Installation)

    def get_resources(self):
        resources = []
        json_resources = self.rest_client.make_request(self.resource_url)['resources']
        for json_resource in json_resources:
            resources.append(Resource(json_resource, self.rest_client))
        return resources

    def __str__(self):
        return pformat(self.__dict__)

class StreamingAnalyticsConnection(StreamsConnection):
    """Creates a connection to a running Streaming Analytics service and exposes methods
    to retrieve the state of the service and its instance.

    Args:
        vcap_services: Vcap services.
        service_name: Name of the Streaming Analytics service.
    """
    def __init__(self, vcap_services=None, service_name=None):
        vcap = _get_vcap_services(vcap_services)
        self.credentials = _get_credentials(vcap, service_name)
        super(StreamingAnalyticsConnection, self).__init__(self.credentials['userid'], self.credentials['password'])
        self._analytics_service = True

    @property
    def resource_url(self):
        if self._resource_url is None:
            self._resource_url = _get_rest_api_url_from_creds(self.session, self.credentials)
        return self._resource_url

    def get_streaming_analytics(self):
        """
        Get a :py:class:`StreamingAnalyticsService` to allow interaction with
        the Streaming Analytics service this object is connected to.

        Returns:
            StreamingAnalyticsService: Object to interact with service.
        """
        return StreamingAnalyticsService(self.rest_client, self.credentials)


def _get_vcap_services(vcap_services = None):
    """Retrieves the VCAP Services information from the `ConfigParams.VCAP_SERVICES` field in the config object. If
    the field is a string, it attempts to parse it as a dict. If the field is a file, it reads the file and attempts
    to parse the contents as a dict.

    :param config: Connection information for Bluemix.
    :type config: dict.
    :return: A dict representation of the VCAP Services information.
    :type return: dict.
    """
    if vcap_services is None:
        vcap_services = os.environ.get('VCAP_SERVICES')
        if vcap_services is None:
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
    """Retrieves the credentials of the VCAP Service specified by the `ConfigParams.SERVICE_NAME` field in `config`.

    :param config: Connection information for Bluemix.
    :type config: dict.
    :param vcap_services: A dict representation of the VCAP Services information.
    :type vcap_services: dict.
    :return: A dict representation of the credentials.
    :type return: dict.
    """

    if service_name is None:
        service_name = os.environ.get('STREAMING_ANALYTICS_SERVICE_NAME', None)
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
        session (Session): A Requests session object for making REST calls
        credentials (dict): A dict representation of the credentials.
    Returns:
        The remote Streams REST API URL.
    """
    resources_url = credentials['rest_url'] + credentials['resources_path']
    try:
        response_raw = session.get(resources_url, auth=(credentials['userid'], credentials['password']))
        response = response_raw.json()
    except:
        logger.error("Error while retrieving rest REST url from: " + resources_url)
        raise


    response_raw.raise_for_status()

    rest_api_url = response['streams_rest_url'] + '/resources'
    return rest_api_url

