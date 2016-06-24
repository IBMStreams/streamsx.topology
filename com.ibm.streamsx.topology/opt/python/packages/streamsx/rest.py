import requests
from pprint import pprint, pformat
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class StreamsRestConnection(object):
    """
    A StreamsRestClient provides convenience methods for connecting to the Streams REST API.
    """

    def __init__(self, username=None, password=None, session=None):
        # Create session to reuse TCP connection
        if session is None:
            # https authentication
            self._username = username
            self._password = password

            session = requests.Session()
            session.auth = (username, password)
            session.verify = False
        else:
            self._username = session.auth[0]
            self._password = session.auth[1]

        self.session = session

    @classmethod
    def from_session(cls, _session):
        return cls(session=_session)

    @classmethod
    def from_login(cls, _username, _password):
        return cls(username=_username, password=_password)


class StreamsRestClient(object):
    def __init__(self, rest_connection):
        self.rest_connection = rest_connection

    def make_request(self, url):
        return self.rest_connection.session.get(url).json()

    def __str__(self):
        return pformat(self.__dict__)


class View(StreamsRestClient):
    def __init__(self, json_view, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_view:
            if key == 'self':
                self.__dict__["rest_self"] = json_view['self']
            else:
                self.__dict__[key] = json_view[key]

    def get_domain(self):
        return Domain(self.make_request(self.domain), self.rest_connection)

    def get_instance(self):
        return Instance(self.make_request(self.instance), self.rest_connection)

    def get_job(self):
        return Job(self.make_request(self.job), self.rest_connection)

    def get_view_items(self):
        view_items = []
        for json_view_items in self.make_request(self.viewItems)['viewItems']:
            view_items.append(ViewItem(json_view_items, self.rest_connection))
        return view_items

    def __str__(self):
        return pformat(self.__dict__)


class ActiveView(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)

class ViewItem(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)

class ConfiguredView(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class Host(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class Job(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def get_views(self):
        views = []
        for json_view in self.make_request(self.views)['views']:
            views.append(View(json_view, self.rest_connection))
        return views

    def get_active_views(self):
        views = []
        for json_view in self.make_request(self.activeViews)['activeViews']:
            views.append(ActiveView(json_view, self.rest_connection))
        return views

    def get_domain(self):
        return Domain(self.make_request(self.domain), self.rest_connection)

    def get_instance(self):
        return Instance(self.make_request(self.instance), self.rest_connection)

    def get_hosts(self):
        hosts = []
        for json_rep in self.make_request(self.hosts)['hosts']:
            hosts.append(Host(json_rep, self.rest_connection))
        return hosts

    def get_operator_connections(self):
        operators_connections = []
        for json_rep in self.make_request(self.operatorConnections)['operatorConnections']:
            operators_connections.append(OperatorConnection(json_rep, self.rest_connection))
        return operators_connections

    def get_operators(self):
        operators = []
        for json_rep in self.make_request(self.operators)['operators']:
            operators.append(Operator(json_rep, self.rest_connection))
        return operators

    def get_pes(self):
        pes = []
        for json_rep in self.make_request(self.pes)['pes']:
            pes.append(PE(json_rep, self.rest_connection))
        return pes

    def get_pe_connections(self):
        pe_connections = []
        for json_rep in self.make_request(self.peConnections)['peConnections']:
            pe_connections.append(PEConnection(json_rep, self.rest_connection))
        return pe_connections

    def get_resource_allocations(self):
        resource_allocations = []
        for json_rep in self.make_request(self.resourceAllocations)['resourceAllocations']:
            resource_allocations.append(ResourceAllocation(json_rep, self.rest_connection))
        return resource_allocations

    def __str__(self):
        return pformat(self.__dict__)


class Operator(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class OperatorConnection(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class PE(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class PEConnection(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class ResourceAllocation(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class ActiveService(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class Installation(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class ImportedStream(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class ExportedStream(StreamsRestClient):
    def __init__(self, json_rep, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_rep:
            if key == 'self':
                self.__dict__["rest_self"] = json_rep['self']
            else:
                self.__dict__[key] = json_rep[key]

    def __str__(self):
        return pformat(self.__dict__)


class Instance(StreamsRestClient):
    def __init__(self, json_domain, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_domain:
            if key == 'self':
                self.__dict__["rest_self"] = json_domain['self']
            else:
                self.__dict__[key] = json_domain[key]

        self.active_version = ActiveVersion(json_domain['activeVersion'])

    def get_operators(self):
        operators = []
        for json_rep in self.make_request(self.operators)['operators']:
            operators.append(Operator(json_rep, self.rest_connection))
        return operators

    def get_operator_connections(self):
        operators_connections = []
        for json_rep in self.make_request(self.operatorConnections)['operatorConnections']:
            operators_connections.append(OperatorConnection(json_rep, self.rest_connection))
        return operators_connections

    def get_pes(self):
        pes = []
        for json_rep in self.make_request(self.pes)['pes']:
            pes.append(PE(json_rep, self.rest_connection))
        return pes

    def get_pe_connections(self):
        pe_connections = []
        for json_rep in self.make_request(self.peConnections)['peConnections']:
            pe_connections.append(PEConnection(json_rep, self.rest_connection))
        return pe_connections

    def get_views(self):
        views = []
        for json_view in self.make_request(self.views)['views']:
            views.append(View(json_view, self.rest_connection))
        return views

    def get_hosts(self):
        hosts = []
        for json_rep in self.make_request(self.hosts)['hosts']:
            hosts.append(Host(json_rep, self.rest_connection))
        return hosts

    def get_domain(self):
        return Domain(self.make_request(self.domain), self.rest_connection)

    def get_active_views(self):
        views = []
        for json_view in self.make_request(self.activeViews)['activeViews']:
            views.append(ActiveView(json_view, self.rest_connection))
        return views

    def get_configured_views(self):
        views = []
        for json_view in self.make_request(self.configuredViews)['configuredViews']:
            views.append(ConfiguredView(json_view, self.rest_connection))
        return views

    def get_jobs(self):
        jobs = []
        for json_rep in self.make_request(self.jobs)['jobs']:
            jobs.append(Job(json_rep, self.rest_connection))
        return jobs

    def get_imported_streams(self):
        imported_streams = []
        for json_rep in self.make_request(self.importedStreams)['importedStreams']:
            imported_streams.append(ImportedStream(json_rep, self.rest_connection))
        return imported_streams

    def get_exported_streams(self):
        exported_streams = []
        for json_rep in self.make_request(self.exportedStreams)['exportedStreams']:
            exported_streams.append(ExportedStream(json_rep, self.rest_connection))
        return exported_streams

    def get_active_services(self):
        active_services = []
        for json_rep in self.make_request(self.activeServices)['activeServices']:
            active_services.append(ActiveService(json_rep, self.rest_connection))
        return active_services

    def get_resource_allocations(self):
        resource_allocations = []
        for json_rep in self.make_request(self.resourceAllocations)['resourceAllocations']:
            resource_allocations.append(ResourceAllocation(json_rep, self.rest_connection))
        return resource_allocations

    def __str__(self):
        return pformat(self.__dict__)


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


class Domain(StreamsRestClient):
    def __init__(self, json_domain, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        for key in json_domain:
            if key == 'self':
                self.__dict__["rest_self"] = json_domain['self']
            self.__dict__[key] = json_domain[key]

        self.activeVersion = ActiveVersion(json_domain['activeVersion'])
        self.resourceTags = []
        for _json_resource_tag in json_domain['resourceTags']:
            self.resourceTags.append(ResourceTag(_json_resource_tag))

    def get_instances(self):
        instances = []
        for json_instance in self.make_request(self.instances)['instances']:
            instances.append(Instance(json_instance, self.rest_connection))
        return instances

    def get_hosts(self):
        hosts = []
        for json_rep in self.make_request(self.hosts)['hosts']:
            hosts.append(Host(json_rep, self.rest_connection))
        return hosts

    def get_active_services(self):
        active_services = []
        for json_rep in self.make_request(self.activeServices)['activeServices']:
            active_services.append(ActiveService(json_rep, self.rest_connection))
        return active_services

    def get_resource_allocations(self):
        resource_allocations = []
        for json_rep in self.make_request(self.resourceAllocations)['resourceAllocations']:
            resource_allocations.append(ResourceAllocation(json_rep, self.rest_connection))
        return resource_allocations

    def get_resources(self):
        resources = []
        json_resources = self.make_request(self.resource_url)['resources']
        for json_resource in json_resources:
            resources.append(Resource(json_resource, self.rest_connection))
        return resources

    def __str__(self):
        return pformat(self.__dict__)


class Resource(StreamsRestClient):
    def __init__(self, json_resource, rest_connection):
        StreamsRestClient.__init__(self, rest_connection)
        self.name = json_resource['name']
        self.resource = json_resource['resource']

    def get_resource(self):
        return self.make_request(self.resource)

    def __str__(self):
        return pformat(self.__dict__)


class StreamsContext(StreamsRestClient):
    def __init__(self, username, password, resource_url):
        rest_connection = StreamsRestConnection.from_login(username, password)
        StreamsRestClient.__init__(self, rest_connection)
        self.resource_url = resource_url

    def get_domains(self):
        domains = []
        for resource in self.get_resources():
            # Get list of domains
            if resource.name == "domains":
                for json_domain in resource.get_resource()['domains']:
                    domains.append(Domain(json_domain, self.rest_connection))
        return domains

    def get_instances(self):
        instances = []
        for resource in self.get_resources():
            # Get list of domains
            if resource.name == "instances":
                for json_rep in resource.get_resource()['instances']:
                    instances.append(Instance(json_rep, self.rest_connection))
        return instances

    def get_installations(self):
        installations = []
        for resource in self.get_resources():
            # Get list of domains
            if resource.name == "installations":
                for json_rep in resource.get_resource()['installations']:
                    installations.append(Installation(json_rep, self.rest_connection))
        return installations

    def get_resources(self):
        resources = []
        json_resources = self.make_request(self.resource_url)['resources']
        for json_resource in json_resources:
            resources.append(Resource(json_resource, self.rest_connection))
        return resources

    def __str__(self):
        return pformat(self.__dict__)