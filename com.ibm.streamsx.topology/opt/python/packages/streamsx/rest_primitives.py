# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
import logging
import requests
import queue
import threading
import time
import json
import re

from pprint import pformat
import streamsx.topology.schema

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
        Args:
            json_rep(dict): The JSON response from a REST call.
            rest_client(_StreamsRestClient): The client used to make the REST call.
        """
        self.rest_client = rest_client
        self.rest_self = json_rep.get('self', None)
        self.json_rep = json_rep

    def __str__(self):
        return pformat(self.__dict__)

    # Override getattr to retrieve attribute from response JSON
    def __getattr__(self, key):
        if 'json_rep' in self.__dict__:
            json = self.__getattribute__('json_rep')
            if key in json:
                return json[key]
        # Fallback to default behaviour
        return self.__getattribute__(key)

    # Prevent setting one of the JSON attribute into the object
    def __setattr__(self, key, value):
        if 'json_rep' in self.__dict__:
            json = self.__getattribute__('json_rep')
            if key in json:
                raise AttributeError('"{0}" is an immutable attribute.'.format(key))
        super(_ResourceElement, self).__setattr__(key, value)

    def refresh(self):
        """Make a REST call to refresh the JSON object stored within the object
        Returns:
            _ResourceElement: The provided object instance is returned with updated value
        """
        self.json_rep = self.rest_client.make_request(self.rest_self)

    def _get_elements(self, url, key, eclass, id=None, name=None):
        """Generically get elements from an object.

        Args:
            url: url of children.
            key: key in the returned json.
            eclass: element class to create instances of.

        Returns: List of eclass instances
        """
        if id is not None and name is not None:
            raise ValueError("id and name cannot specified together")

        elements = []
        json_elements = self.rest_client.make_request(url)[key]
        for json_element in json_elements:
            if not _exact_resource(json_element, id):
                continue
            if not _matching_resource(json_element, name):
                continue
            elements.append(eclass(json_element, self.rest_client))

        return elements

    def _get_element_by_id(self, url, key, eclass, id):
        """Get a single element matching an id"""
        elements = self._get_elements(url, key, eclass, id=id)
        if not elements:
            raise ValueError("No resource matching: {0}".format(id))
        if len(elements) == 1:
            return elements[0]
        raise ValueError("Multiple resources matching: {0}".format(id))


class _StreamsRestClient(object):
    """Handles the session connection with the Streams REST API
    """
    def __init__(self, username, password):
        """
        Args:
            username(str): The username of an authorized Streams user.
            password(str): The password associated with the username.
        """
        # Create session to reuse TCP connection
        # https authentication
        self._username = username
        self._password = password

        self.session = requests.Session()
        self.session.auth = (username, password)

    def make_request(self, url):
        logger.debug('Beginning a REST request to: ' + url)
        return self.session.get(url).json()

    def __str__(self):
        return pformat(self.__dict__)


class _ViewDataFetcher(object):
    """A callable which, when invoked with a thread, begins fetching data from the
    supplied view and populates the `View.items` queue.
    """
    def __init__(self, view, tuple_getter):
        self.view = view
        self.tuple_getter = tuple_getter
        self.stop = threading.Event()
        self.items = queue.Queue()

        self._last_collection_time = -1
        self._last_collection_time_count = 0

    def __call__(self):
        while not self._stopped():
            _items = self._get_deduplicated_view_items()
            if _items is not None:
                for itm in _items:
                    self.items.put(itm)
            time.sleep(1)

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

def _get_view_json_tuple(item):
    """
    Get a tuple from a view with a schema
    tuple<rstring jsonString>
    """
    return json.loads(item.data['jsonString'])

def _get_view_string_tuple(item):
    """
    Get a tuple from a view with a schema
    tuple<rstring string>
    """
    return str(item.data['string'])

def _get_view_dict_tuple(item):
    """Tuple from REST was in JSON which has already been
    converted to a dic.
    """
    return item.data


class View(_ResourceElement):
    """The view element resource provides access to information about a view that is associated with an active job, and
    exposes methods to retrieve data from the View's Stream.
    """
    def __init__(self, json_view, rest_client):
        super(View, self).__init__(json_view, rest_client)    
        tuple_fn = _get_view_dict_tuple
        if len(self.attributes) == 1:
            attr_type = self.attributes[0]['type']
            attr_name = self.attributes[0]['name']
            if 'rstring' == attr_type:
                if 'jsonString' == attr_name:
                    tuple_fn = _get_view_json_tuple
                elif 'string' == attr_name:
                    tuple_fn = _get_view_string_tuple
        self._data_fetcher = None
        self._tuple_fn = tuple_fn

    def get_domain(self):
        return Domain(self.rest_client.make_request(self.domain), self.rest_client)

    def get_instance(self):
        return Instance(self.rest_client.make_request(self.instance), self.rest_client)

    def get_job(self):
        return Job(self.rest_client.make_request(self.job), self.rest_client)

    def stop_data_fetch(self):
        if self._data_fetcher is not None:
            self._data_fetcher.stop.set()
            self._data_fetcher = None

    def start_data_fetch(self):
        self.stop_data_fetch()
        self._data_fetcher = _ViewDataFetcher(self, self._tuple_fn)
        t = threading.Thread(target=self._data_fetcher)
        t.start()
        return self._data_fetcher.items

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
    def get_views(self, name=None):
        return self._get_elements(self.views, 'views', View, name=name)

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


class OperatorOutputPort(_ResourceElement):
    """Operator output port resource provides access to information about an output port
    for a specific operator.

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
    """ Exported stream resource represents a stream that has been exported by a job.
    """
    def get_operator_output_port(self):
        return OperatorOutputPort(self.rest_client.make_request(self.operatorOutputPort), self.rest_client)

    def _as_published_topic(self):
        """This stream as a PublishedTopic if it is published otherwise None
        """

        oop = self.get_operator_output_port()
        if not hasattr(oop, 'export'):
            return

        export = oop.export
        if export['type'] != 'properties':
            return

        seen_export_type = False
        topic = None

        for p in export['properties']:
            if p['type'] != 'rstring':
                continue
            if p['name'] == '__spl_exportType':
                if p['values'] == ['"topic"']:
                    seen_export_type = True
                else:
                    return
            if p['name'] == '__spl_topic':
                topic = p['values'][0]

        if seen_export_type and topic is not None:
            schema = None
            if hasattr(oop, 'tupleAttributes'):
                ta_url = oop.tupleAttributes
                ta_resp = self.rest_client.make_request(ta_url)
                schema = streamsx.topology.schema.StreamSchema(ta_resp['splType'])
            return PublishedTopic(topic[1:-1], schema)
        return


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

    def get_views(self, name=None):
        return self._get_elements(self.views, 'views', View, name=name)

    def get_hosts(self):
        return self._get_elements(self.hosts, 'hosts', Host)

    def get_domain(self):
        return Domain(self.rest_client.make_request(self.domain), self.rest_client)

    def get_jobs(self, name=None):
        """Retrieve jobs running in this instance.

        Args:
            name: Job name pattern to match.

        Returns:
            :py:class:`Job` objects. When ``id`` is not ``None`` a single matching `Job`
            is returned otherwise a list of `Job` objects.

        Raises:
            ValueError: ``id`` was specified and no matching job exists or multiple matching jobs exist.
        """
        return self._get_elements(self.jobs, 'jobs', Job, id, name)

    def get_job(self, id):
        return self._get_element_by_id(self.jobs, 'jobs', Job, str(id))

    def get_imported_streams(self):
        return self._get_elements(self.importedStreams, 'importedStreams', ImportedStream)

    def get_exported_streams(self):
        return self._get_elements(self.exportedStreams, 'exportedStreams', ExportedStream)

    def get_active_services(self):
        return self._get_elements(self.activeServices, 'activeServices', ActiveService)

    def get_resource_allocations(self):
        return self._get_elements(self.resourceAllocations, 'resourceAllocations', ResourceAllocation)

    def get_published_topics(self):
        """Get a list of published topics for this instance.

        Streams applications publish streams to a a topic that can be subscribed to by other
        applications. This allows a microservice approach where publishers
        and subscribers are independent of each other.

        A published stream has a topic and a schema. It is recommended that a
        topic is only associated with a single schema.

        Streams may be published and subscribed by applications regardless of the
        implementation language. For example a Python application can publish
        a stream of JSON tuples that are subscribed to by SPL and Java applications.

        Returns:
             list(PublishedTopic): List of currently published topics.
        """
        published_topics = []
        # A topic can be published multiple times
        # (typically with the same schema) but the
        # returned list only wants to contain a topic,schema
        # pair once. I.e. the list of topics being published is
        # being returned, not the list of streams.
        seen_topics = {}
        for es in self.get_exported_streams():
            pt = es._as_published_topic()
            if pt is not None:
                if pt.topic in seen_topics:
                    if pt.schema is None:
                        continue
                    if pt.schema in seen_topics[pt.topic]:
                        continue
                    seen_topics[pt.topic].append(pt.schema)
                else:
                    seen_topics[pt.topic] = [pt.schema]
                published_topics.append(pt)

        return published_topics


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


class PublishedTopic(object):
    """Metadata for a published topic.
    Args:
        topic: Published topic.
        schema: Schema of topic.
    """
    def __init__(self, topic, schema):
        self.topic = topic
        self.schema = schema

    def __repr__(self):
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
