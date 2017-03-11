# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
import logging
import requests
import queue
import threading
import time
import json
import re

from pprint import pprint, pformat
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = logging.getLogger('streamsx.rest')

def _exact_resource(json_rep, id=None):
    if id is not None:
        if not 'id' in json_rep:
            return False
        return id == json_rep['id']
    return True
    
def _matching_resource(json_rep, name=None):
    if name is not None:
        if not 'name' in json_rep:
            return False
        return re.match(name, json_rep['name'])
    return True

  


class _ResourceElement(object):
    """A class whose fields are populated by the JSON returned from a REST call.
    """
    def __init__(self, json_rep, rest_client):
        """
        :param json_rep: The JSON response from a REST call.
        :param rest_client: The client used to make the REST call.
        """
        self.rest_client=rest_client
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)

    def _get_elements(self, url, key, eclass, id=None, name=None):
        """Generically get elements from an object.

        Args:
            url: url of children.
            key: key in the returned json.
            eclass: element class to create instances of.

        Returns: List of eclass instances
        """
        elements = []
        json_elements = self.rest_client.make_request(url)[key]
        for json_element in json_elements:
            if not _exact_resource(json_element, id):
                continue
            if not _matching_resource(json_element, name):
                continue
            elements.append(eclass(json_element, self.rest_client))
        return elements

class StreamsRestClient(object):
    """Handles the session connection with the Streams REST API.

    :param username: The username of an authorized Streams user.
    :type username: str.
    :param password: The password associated with the username.
    :type password: str.
    :param resource_url: The resource endpoint of the instance. Can be found with `st geturl --api` for local Streams
    installs.
    :type resource_url: str.
    """
    def __init__(self, username, password, resource_url):
        self.resource_url = resource_url
        # Create session to reuse TCP connection
        # https authentication
        self._username = username
        self._password = password

        session = requests.Session()
        session.auth = (username, password)
        session.verify = False

        self.session = session

    def make_request(self, url):
        logger.debug('Beginning a REST request to: ' + url)
        return self.session.get(url).json()

    def __str__(self):
        return pformat(self.__dict__)


class ViewThread(threading.Thread):
    """A thread which, when invoked, begins fetching data from the supplied view and populates the `View.items` queue.
    """
    def __init__(self, view, tuple_getter):
        super(ViewThread, self).__init__()
        self.view = view
        self.tuple_getter = tuple_getter
        self.stop = threading.Event()
        self.items = queue.Queue()

        self._last_collection_time = -1
        self._last_collection_time_count = 0

    def __call__(self):
        while not self._stopped():
            time.sleep(1)
            _items = self._get_deduplicated_view_items()
            if _items is not None:
                for itm in _items:
                    self.items.put(itm)

    def _get_deduplicated_view_items(self):
        # Retrieve the view object
        data_name = self.view.attributes[0]['name']
        items = self.view.get_view_items()
        data = []

        # The number of already seen tuples to ignore on the last millisecond time boundary
        ignore_last_collection_time_count = self._last_collection_time_count

        for item in items:
            # Ignore tuples from milliseconds we've already seen
            if item.collectionTime < self._last_collection_time:
                continue
            elif item.collectionTime == self._last_collection_time:
                # Ignore tuples within the millisecond which we've already seen.
                if ignore_last_collection_time_count > 0:
                    ignore_last_collection_time_count -= 1
                    continue

                # If we haven't seen it, continue
                data.append(self.tuple_getter(item))
            else:
                data.append(self.tuple_getter(item))

        if len(items) > 0:
            # Record the current millisecond time boundary.
            _last_collection_time = items[-1].collectionTime
            _last_collection_time_count = 0
            backwards_counter = len(items) - 1
            while backwards_counter >= 0 and items[backwards_counter].collectionTime == _last_collection_time:
                _last_collection_time_count += 1
                backwards_counter -= 1

            self._last_collection_time = _last_collection_time
            self._last_collection_time_count = _last_collection_time_count

        return data

    def _stopped(self):
        return self.stop.isSet()

def _get_json_tuple(item):
    """
    Get a tuple from a view with a schema
    tuple<rstring jsonString>
    """
    return json.loads(item.data['jsonString']))

class View(_ResourceElement):
    """The view element resource provides access to information about a view that is associated with an active job, and
    exposes methods to retrieve data from the View's Stream.
    """
    def __init__(self, json_view, rest_client):
        super(View, self).__init__(json_view, rest_client)
        self.view_thread = ViewThread(self, _get_json_tuple)

    def get_domain(self):
        return Domain(self.rest_client.make_request(self.domain), self.rest_client)

    def get_instance(self):
        return Instance(self.rest_client.make_request(self.instance), self.rest_client)

    def get_job(self):
        return Job(self.rest_client.make_request(self.job), self.rest_client)

    def stop_data_fetch(self):
        self.view_thread.stop.set()

    def start_data_fetch(self):
        self.view_thread.stop.clear()
        t = threading.Thread(target=self.view_thread)
        t.start()
        return self.view_thread.items

    def get_view_items(self):
        view_items = []
        for json_view_items in self.rest_client.make_request(self.viewItems)['viewItems']:
            view_items.append(ViewItem(json_view_items, self.rest_client))
        logger.debug("Retrieved " + str(len(view_items)) + " items from view " + self.name)
        return view_items

class ViewItem(_ResourceElement):
    """
    Represents the data of a tuple, it's type, and the time when it was collected from the stream.
    """
    pass

class Host(_ResourceElement):
    """The host element resource provides access to information about a host that is allocated to a domain as a
    resource for running Streams services and applications.
    """
    pass


class Job(_ResourceElement):
    """The job element resource provides access to information about a submitted job within a specified instance.
    """
    def get_views(self):
        return self._get_elements(self.views, 'views', View)

    def get_domain(self):
        return Domain(self.rest_client.make_request(self.domain), self.rest_client)

    def get_instance(self):
        return Instance(self.rest_client.make_request(self.instance), self.rest_client)

    def get_hosts(self):
        return self._get_elements(self.hosts, 'hosts', Host)

    def get_operator_connections(self):
        return self._get_elements(self.operatorConnections, 'operatorConnections', OperatorConnection)

    def get_operators(self):
        return self._get_elements(self.operators, 'operators', Operator)

    def get_pes(self):
        return self._get_elements(self.pes, 'pes', PE)

    def get_pe_connections(self):
        return self._get_elements(self.peConnections, 'peConnections', PEConnection)

    def get_resource_allocations(self):
        return self._get_elements(self.resourceAllocations, 'resourceAllocations', ResourceAllocation)

    def cancel(self, force=False):
        """Cancel this job.

        Args:
            force(bool): Forcefully cancel this job.

        Returns:
            True if the job was cancelled, otherwise False if an error occurred.

        """
        if not self.rest_client._sc._analytics_service:
            import streamsx.st as st
            if st._has_local_install:
                if not st._cancel_job(self.id, force):
                    if force:
                        return False
                    return st._cancel_job(self.id, force=True)
                return True
        else:
            self.rest_client._sc.get_streaming_analytics().cancel_job(self.id)
            return True
        raise NotImplementedError('Job.cancel()')

class Operator(_ResourceElement):
    """The operator element resource provides access to information about a specific operator in a job.
    """
    def get_metrics(self, name=None):
        """
        Get metrics for an operator.

        Args:
            name(str): Only return metrics matching ``name`` as a regular
                expression using ``re.match(name, metric_name``.
                If name is not supplied then all metrics for this operator are returned.

        Returns:
             list(Metric): List of matching metrics.
        """
        return self._get_elements(self.metrics, 'metrics', Metric, name=name)

class OperatorConnection(_ResourceElement):
    """The operator connection element resource provides access to information about a connection between two operator
    ports.
    """
    pass

class Metric(_ResourceElement):
    """
    Metric resource provides access to information about a Streams metric.
    """
    pass

class PE(_ResourceElement):
    """The processing element (PE) resource provides access to information about a PE.
    """
    pass


class PEConnection(_ResourceElement):
    """The processing element (PE) connection resource provides access to information about a connection between two
    processing element (PE) ports.
    """
    pass


class ResourceAllocation(_ResourceElement):
    pass


class ActiveService(_ResourceElement):
    pass


class Installation(_ResourceElement):
    pass


class ImportedStream(_ResourceElement):
    pass


class ExportedStream(_ResourceElement):
    pass


class Instance(_ResourceElement):
    """The instance element resource provides access to information about a Streams instance."""
    def get_operators(self):
        return self._get_elements(self.operators, 'operators', Operator)

    def get_operator_connections(self):
        return self._get_elements(self.operatorConnections, 'operatorConnections', OperatorConnection)

    def get_pes(self):
        return self._get_elements(self.pes, 'pes', PE)

    def get_pe_connections(self):
        return self._get_elements(self.peConnections, 'peConnections', PEConnection)

    def get_views(self):
        return self._get_elements(self.views, 'views', View)

    def get_hosts(self):
        return self._get_elements(self.hosts, 'hosts', Host)

    def get_domain(self):
        return Domain(self.rest_client.make_request(self.domain), self.rest_client)

    def get_jobs(self, id=None, name=None):
        if id is not None:
            id = str(id)
        return self._get_elements(self.jobs, 'jobs', Job, id, name)

    def get_imported_streams(self):
        return self._get_elements(self.importedStreams, 'importedStreams', ImportedStream)

    def get_exported_streams(self):
        return self._get_elements(self.exportedStreams, 'exportedStreams', ExportedStream)

    def get_active_services(self):
        return self._get_elements(self.activeServices, 'activeServices', ActiveService)

    def get_resource_allocations(self):
        return self._get_elements(self.resourceAllocations, 'resourceAllocations', ResourceAllocation)

class ResourceTag(object):
    def __init__(self, json_resource_tag):
        self.definition_format_properties = json_resource_tag['definitionFormatProperties']
        self.description = json_resource_tag['description']
        self.name = json_resource_tag['name']
        self.properties_definition = json_resource_tag['propertiesDefinition']
        self.reserved = json_resource_tag['reserved']

    def __str__(self):
        return pformat(self.__dict__)


class ActiveVersion(object):
    def __init__(self, json_active_version):
        self.architecture = json_active_version['architecture']
        self.build_version = json_active_version['buildVersion']
        self.edition_name = json_active_version['editionName']
        self.full_product_version = json_active_version['fullProductVersion']
        self.minimum_os_base_version = json_active_version['minimumOSBaseVersion']
        self.minimum_os_patch_version = json_active_version['minimumOSPatchVersion']
        self.minimum_os_version = json_active_version['minimumOSVersion']
        self.product_name = json_active_version['productName']
        self.product_version = json_active_version['productVersion']

    def __str__(self):
        return pformat(self.__dict__)


class Domain(_ResourceElement):
    """The domain element resource provides access to information about an InfoSphere Streams domain."""
    def get_instances(self):
        return self._get_elements(self.instances, 'instances', Instance)

    def get_hosts(self):
        return self._get_elements(self.hosts, 'hosts', Host)

    def get_active_services(self):
        return self._get_elements(self.activeServices, 'activeServices', ActiveService)

    def get_resource_allocations(self):
        return self._get_elements(self.resourceAllocations, 'resourceAllocations', ResourceAllocation)

    def get_resources(self):
        return self._get_elements(self.resource_url, 'resources', Resource)


class Resource(_ResourceElement):
    def get_resource(self):
        return self.rest_client.make_request(self.resource)


def get_view_obj(_view, rc):
    for domain in rc.get_domains():
        for instance in domain.get_instances():
            for view in instance.get_views():
                if view.name == _view.name:
                    return view
    return None


class StreamingAnalyticsService(object):
    """Streaming Analytics service running on IBM Bluemix cloud platform.

    """
    def __init__(self, rest_client, credentials):
        self.rest_client = rest_client
        self._credentials = credentials

    def _get_url(self, req_name):
        return self._credentials['rest_url'] + self._credentials[req_name]

    def cancel_job(self, job_id=None, job_name=None):
        """Cancel a running job.

        Args:
            job_id(str): Identifier of job to be canceled.
            job_name(str): Name of job to be canceled.

        Returns:

        """
        payload = {}
        if job_name is not None:
            payload['job_name'] = job_name
        if job_id is not None:
            payload['job_id'] = job_id

        jobs_url = self._get_url('jobs_path')
        return self.rest_client.session.delete(jobs_url, params=payload).json()
    def start_instance(self):
        """
        Start the instance for this Streaming Analytics service.
        """
        start_url = self._get_url('start_path')
        return self.rest_client.session.put(start_url, json={}).json()
    def stop_instance(self):
        """
        Stop the instance for this Streaming Analytics service.
        """
        stop_url = self._get_url('stop_path')
        return self.rest_client.session.put(stop_url, json={}).json()
    def get_instance_status(self):
        """
        Get the status the instance for this Streaming Analytics service.
        """
        status_url = self._get_url('status_path')
        return self.rest_client.session.get(status_url).json()
