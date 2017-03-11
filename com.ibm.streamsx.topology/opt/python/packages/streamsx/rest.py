# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
import requests
import os
import json
import logging
import streamsx.st as st

from .rest_primitives import Domain, Instance, Installation, Resource, StreamsRestClient, StreamingAnalyticsService,  _exact_resource
from .rest_errors import ViewNotFoundError
from pprint import pformat
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = logging.getLogger('streamsx.rest')


class StreamsConnection:
    """Creates a connection to a running Streams instance and exposes methods to retrieve the state of that instance.

    Streams maintains information regarding the state of its resources. For example, these resources could include the
    currently running Jobs, Views, PEs, Operators, and Domains. The StreamsConnection provides methods to retrieve that
    information.

    Example:
        >>> _resource_url = "https://streamsqse.localdomain:8443/streams/rest/resources"
        >>> sc = StreamsConnection(username="streamsadmin", password="passw0rd", resource_url=_resource_url)
        >>> instances = sc.get_instances()
        >>> jobs_count = 0
        >>> for instance in instances:
        ...    jobs_count += len(instance.get_jobs())
        >>> print("There are " + jobs_count + " jobs across all instances.")

    """
    def __init__(self, username=None, password=None, resource_url=None, config=None, instance_id=None):
        """
        :param username: The username of an authorized Streams user.
        :type username: str.
        :param password: The password associated with the username.
        :type password: str.
        :param resource_url: The resource endpoint of the instance. Can be found with `st geturl --api`.
        :type resource_url: str.
        :param config: Connection information for Bluemix. Should not be used in conjunction with username, password,
        and resource_url.
        :type config: dict.
        """
        # manually specify username, password, and resource_url
        if username and password and resource_url:
            self._setup_distributed(instance_id, username, password, resource_url)

        # Connect to Bluemix service using VCAP
        elif config:
            vcap_services = _get_vcap_services(vcap_services=config.get(ConfigParams.VCAP_SERVICES))
            self.credentials = _get_credentials(config[ConfigParams.SERVICE_NAME], vcap_services)
            self._analytics_service = True

            # Obtain the streams SWS REST URL
            rest_api_url = _get_rest_api_url_from_creds(self.credentials)

            # Create rest connection to remote Bluemix SWS
            self.rest_client = StreamsRestClient(self.credentials['userid'], self.credentials['password'], rest_api_url)
            self.resource_url = rest_api_url
            # Get the instance id from one of the URL paths
            self.instance_id = self.credentials['jobs_path'].split('/service_instances/',1)[1].split('/',1)[0]

        elif username and password and st._has_local_install:
            self._setup_distributed(instance_id, username, password, st.get_rest_api())

        elif st._has_local_install:
            # Assume quickstart
            self._setup_distributed(instance_id, 'streamsadmin', 'passw0rd', st.get_rest_api())

        else:
            logger.error("Invalid arguments for StreamsContext.__init__: must supply either a BlueMix VCAP Services or "
                         "a username, password, and resource url.")
            raise ValueError("Must supply either a BlueMix VCAP Services or a username, password, and resource url"
                             " to the StreamsContext constructor.")
        self.rest_client._sc = self

    def _setup_distributed(self, instance_id, username, password, resource_url):
        self.resource_url = resource_url
        self.rest_client = StreamsRestClient(username, password, self.resource_url)
        self._analytics_service = False
        if instance_id is None:
            instance_id = os.environ['STREAMS_INSTANCE_ID']
        self.instance_id = instance_id

    def _get_elements(self, resource_name, eclass, id=None):
        elements = []
        for resource in self.get_resources():
            if resource.name == resource_name:
                for json_element in resource.get_resource()[resource_name]:
                    if not _exact_resource(json_element, id):
                        continue
                    elements.append(eclass(json_element, self.rest_client))
        return elements

    def get_streaming_analytics(self):
        """
        Get a ref:StreamingAnalyticsService to allow interaction with
        the Streaming Analytics service this object is connected to.

        This connection must be configured for a Streaming Analytics service.
        Returns:
            StreamingAnalyticsService: Object to interact with service.
        """
        assert self._analytics_service

        return StreamingAnalyticsService(self.rest_client, self.credentials)

    def get_domains(self, id=None):
        """Retrieves a list of all Domain resources across all known streams installations.

        :return: Returns a list of all Domain resources.
        :type return: list.
        """
        return self._get_elements('domains', Domain, id=id)

    def get_instances(self, id=None):
        """Retrieves a list of all Instance resources across all known streams installations.

        :return: Returns a list of all Instance resources.
        :type return: list.
        """
        return self._get_elements('instances', Instance, id=id)

    def get_installations(self):
        """Retrieves a list of all known streams Installations.

        :return: Returns a list of all Installation resources.
        :type return: list.
        """
        return self._get_elements('installations', Installation)

    def get_views(self):
        """Gets a list of all View resources across all known streams installations.

        :return: Returns a list of all View resources.
        :type return: list.
        """
        views = []
        for domain in self.get_domains():
            for instance in domain.get_instances():
                for view in instance.get_views():
                    views.append(view)
        return views

    def get_view(self, name):
        """Gets a view with the specified `name`. If there are multiple views with the same name, it will return
        the first one encountered.

        :param name: The name of the View resource.
        :return: The view resource with the specified `name`.
        """
        for domain in self.get_domains():
            for instance in domain.get_instances():
                for view in instance.get_views():
                    if view.name == name:
                        return view
        raise ViewNotFoundError("Could not locate view: " + name)

    def get_resources(self):
        resources = []
        json_resources = self.rest_client.make_request(self.resource_url)['resources']
        for json_resource in json_resources:
            resources.append(Resource(json_resource, self.rest_client))
        return resources

    def __str__(self):
        return pformat(self.__dict__)


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
        try:
            vcap_services = os.environ['VCAP_SERVICES']
        except KeyError:
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


def _get_credentials(service_name, vcap_services):
    """Retrieves the credentials of the VCAP Service specified by the `ConfigParams.SERVICE_NAME` field in `config`.

    :param config: Connection information for Bluemix.
    :type config: dict.
    :param vcap_services: A dict representation of the VCAP Services information.
    :type vcap_services: dict.
    :return: A dict representation of the credentials.
    :type return: dict.
    """
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


def _get_rest_api_url_from_creds(credentials):
    """Retrieves the Streams REST API URL from the provided credentials.

    :param credentials: A dict representation of the credentials.
    :type credentials: dict.
    :return: The remote Streams REST API URL.
    :type return: str.
    """
    resources_url = credentials['rest_url'] + credentials['resources_path']
    try:
        response = requests.get(resources_url, auth=(credentials['userid'], credentials['password'])).json()
    except:
        logger.exception("Error while retrieving SWS REST url from: " + resources_url)
        raise

    rest_api_url = response['streams_rest_url'] + '/resources'
    return rest_api_url


class ConfigParams(object):
    """
    Configuration options which may be used as keys in the config parameter of the StreamsContext constructor.

    VCAP_SERVICES - a json object containing the VCAP information used to submit to Bluemix
    SERVICE_NAME - the name of the streaming analytics service to use from VCAP_SERVICES.
    """
    VCAP_SERVICES = 'topology.service.vcap'
    SERVICE_NAME = 'topology.service.name'

