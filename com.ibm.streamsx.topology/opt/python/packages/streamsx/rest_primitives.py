# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019

"""
Primitive objects for REST bindings.

********
Overview
********

Contains classes representing primitive Streams objects, such as
:py:class:`Instance`, :py:class:`Job`, :py:class:`PE`, etc.

"""

__all__ = ['ActiveService', 'ActiveVersion', 'ApplicationBundle', 'ApplicationConfiguration', 'BaseImage', 'Domain',
           'ExportedStream', 'Host', 'ImportedStream', 'Installation', 'Instance', 'Job', 'Metric', 
           'OperatorConnection', 'OperatorInputPort', 'OperatorOutputPort', 'Operator', 'PEConnection', 'PE',
           'PublishedTopic', 'ResourceAllocation', 'Resource', 'ResourceTag', 'RestResource', 'StreamingAnalyticsService', 'Toolkit', 'ViewItem', 'View']

import logging
import requests
from requests import RequestException
from requests import Timeout
import queue
import os
import threading
import time
import json
import re
import socket
import numbers
import warnings
import xml.etree.ElementTree as ElementTree

from pprint import pformat
from urllib import parse

import streamsx.topology.context
import streamsx.topology.schema
import streamsx.rest

##############################################################
# NOTE verify is passed explictly when using session to
# work around requests defect: #3829
# https://github.com/requests/requests/issues/3829
##############################################################

logger = logging.getLogger('streamsx.rest')
_REST_CONNECT_TIMEOUT = 30.0     # timeout in seconds for creating a connection (including TLS)
# to specify no timeout, use 'None' as value
_TIMEOUTS = {
    'GET': (_REST_CONNECT_TIMEOUT, 90.0),      # various resources, also large resources like logs 
    'POST': (_REST_CONNECT_TIMEOUT, 120.0),    # authentication, upload bundle, submit bundle, mk app config
    'PUT': (_REST_CONNECT_TIMEOUT, 120.0),     # start/stop instance SASv1
    'PATCH': (_REST_CONNECT_TIMEOUT, 120.0),   # start/stop instance SASv2, update app config, update operators
    'DELETE': (_REST_CONNECT_TIMEOUT, 120.0),  # del toolkit, del app config, cancel job
    'HEAD': (_REST_CONNECT_TIMEOUT, 120.0),    # not used
    'OPTIONS': (_REST_CONNECT_TIMEOUT, 120.0)  # not used
    }


def _get_username(username=None):
    if not username:
       username = os.environ.get('STREAMS_USERNAME')
       if not username:
           import getpass
           username = getpass.getuser()
    return username

def _file_name(prefix, id_, suffix):
    return prefix + '_' + id_ + '_' + str(int(time.time())) + suffix

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
    """Stores JSON response from a REST call, and expose its properties as attributes.

    Attributes:
        json_rep(dict): The JSON representation of the resource, its properties can be accessed directly using dot
            notation on the object.
    """
    @classmethod
    def _new(cls, json_rep, rest_client, ts=None):
        """
        Indirect creation of a new instance of this class
        to allow instance reuse to avoid REST calls.
        """
        if cls._USE_CACHE:
            element = rest_client._cache_find(cls, json_rep['self'])
            if element:
                element.json_rep = json_rep
                element._last_modified = ts if ts else time.time()
                return element
        element = cls(json_rep, rest_client)
        element._last_modified = ts if ts else time.time()
        rest_client._cache_add(cls, element)
        return element

    @classmethod
    def _fetch(cls, url, rest_client):
        element = rest_client._cache_find(cls, url)
        if element:
            age = time.time() - element._last_modified
            if age > 2:
                element.refresh()
            return element
        return cls._new(rest_client.make_request(url), rest_client)

    _USE_CACHE = True

    def __init__(self, json_rep, rest_client):
        """
        Args:
            json_rep(dict): The JSON response from a REST call.
            rest_client(_StreamsRestClient): The client used to make the REST call.
        """
        self.rest_client = rest_client
        self.rest_self = json_rep.get('self', None)
        self.json_rep = json_rep
        self._last_modified = 0

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
        """Refresh the resource and update the attributes to reflect the latest status.
        """
        self.json_rep = self.rest_client.make_request(self.rest_self)
        self._last_modified = time.time()

    def _get_elements(self, url, key, eclass, id=None, name=None):
        """Get elements matching `id` or `name`

        Args:
            url(str): url of children.
            key(str): key in the returned JSON.
            eclass(subclass type of :py:class:`_ResourceElement`): element class to create instances of.
            id(str, optional): only return resources whose `id` property matches the given `id`
            name(str, optional): only return resources whose `name` property matches the given `name`

        Returns:
            list(_ResourceElement): List of `eclass` instances

        Raises:
            ValueError: both `id` and `name` are specified together
        """
        if id is not None and name is not None:
            raise ValueError("id and name cannot specified together")

        json_elements = self.rest_client.make_request(url)[key]
        ts = time.time()
        return [eclass._new(element, self.rest_client, ts=ts) for element in json_elements
                if _exact_resource(element, id) and _matching_resource(element, name)]

    def _get_element_by_id(self, url, key, eclass, id):
        """Get a single element matching an `id`

        Args:
            url(str): url of children.
            key(str): key in the returned JSON.
            eclass(subclass type of :py:class:`_ResourceElement`): element class to create instances of.
            id(str): return resources whose `id` property matches the given `id`

        Returns:
            _ResourceElement: Element of type `eclass` matching the given `id`

        Raises:
            ValueError: No resource matches given `id` or multiple resources matching given `id`
        """
        elements = self._get_elements(url, key, eclass, id=id)
        if not elements:
            raise ValueError("No resource matching: {0}".format(id))
        if len(elements) == 1:
            return elements[0]
        raise ValueError("Multiple resources matching: {0}".format(id))

def _handle_http_errors(res):
    # HTTP error responses are 4xx, server errors are 5xx
    if res.status_code >= 400:
        # logger.error("Response returned with error code: " + str(res.status_code))
        # logger.error(res.text)
        res.raise_for_status()


class _StreamsRestClient(object):
    _blocked_ssl_warn = False

    """Session connection with the Streams REST API
    
    Args:
        auth: A Callable that adds Authentication header(s) to requests
        timeouts (Number|tuple|dict): timeouts
        
    Example:
        >>> c = _StreamsRestClient(auth=None)               # default timeouts
        >>> c = _StreamsRestClient(auth=None, timeouts=60)  # 60 seconds for all
        >>> c = _StreamsRestClient(auth=None, timeouts=(30, 120))  # 30 seconds connect timeout, 120 seconds REST timeout
        >>> # specify some timeouts, for all others, the default apply
        >>> c = _StreamsRestClient(auth=None, timeouts={'GET': (10, 30), 'Post': (10, 60), 'options': 30, 'HEAD': None})
        >>> c._timeouts
    """
    def __init__(self, auth, timeouts=_TIMEOUTS):
        self.session = requests.Session()
        self.session.auth = auth
        self._cache = {}
        self._cache_lock = threading.Lock()
        _requests = _TIMEOUTS.keys() #['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS']
        if timeouts is None:
            # intentionally no timeouts
            self._timeouts = {key: None for key in _requests}
        elif isinstance(timeouts, numbers.Number) or isinstance(timeouts, tuple):
            # single timeout for all request methods
            self._timeouts = {key: timeouts if isinstance(timeouts, tuple) else (float(timeouts), float(timeouts)) for key in _requests}
        elif isinstance(timeouts, dict):
            # individual timeouts per request method
            self._timeouts = _TIMEOUTS.copy()
            for key, val in timeouts.items():
                key_upper = key.upper()
                if key_upper in _requests:
                    self._timeouts[key_upper] = val if val is None or isinstance(val, tuple) else (float(val), float(val))
        else:
            raise TypeError(timeouts)

    def timeout_for(self, request_method):
        """Returns the timeout for a request method. 
        
        Args:
            request_method (str): the request method. Valid values are 'GET', 'POST', 'PUT',
                'PATCH', 'DELETE', 'HEAD', and 'OPTIONS'. The request_method is case-insensitive.

        Returns:
            tuple: the timeouts for the given request method
        """
        return self._timeouts.get(request_method.upper(), None)

    def _cache_find(self, cls, url):
        if cls._USE_CACHE:
            try:
                self._cache_lock.acquire()
                elements = self._cache.get(cls)
                if elements is not None:
                    return elements.get(url)
            finally:
                self._cache_lock.release()

    def _cache_add(self, cls, element):
        if cls._USE_CACHE:
            try:
                self._cache_lock.acquire()
                elements = self._cache.get(cls)
                if elements is None:
                    elements = {}
                    self._cache[cls] = elements
                elements[element.rest_self] = element
            finally:
                self._cache_lock.release()

    def _cache_job_cancel(self, job):
        expire = time.time() - 60.0
        url = job.rest_self
        c = 0
        try:
            self._cache_lock.acquire()
            elements = self._cache.get(job.__class__)
            elements.pop(url, None)
            for ecls, elements in self._cache.items():
                if ecls == job.__class__:
                    continue
                remove = [u for u,e in elements.items() if e.json_rep.get('job') == url or e._last_modified < expire]
                for u in remove:
                    del elements[u]
        finally:
            self._cache_lock.release()

    def _block_ssl_warn(self):
        if self.session.verify is False and not _StreamsRestClient._blocked_ssl_warn:
            import warnings
            import urllib3
            warnings.simplefilter(action='once', category=urllib3.exceptions.InsecureRequestWarning)
            _StreamsRestClient._blocked_ssl_warn = True

    # Create session to reuse TCP connection
    # https authentication
    @staticmethod
    def _of_basic(username, password, timeouts=_TIMEOUTS):
        """
        Args:
            username(str): The username of an authorized Streams user.
            password(str): The password associated with the username.
            timeouts (Number|tuple|dict): timeouts for connect and REST requests
        """
        auth = (username, password)
        return _StreamsRestClient(auth, timeouts=timeouts)
    
    def make_request(self, url):
        logger.debug('Beginning a REST request to: ' + url)
        self._block_ssl_warn()
        headers={ 'Accept': 'application/json'}
        res = self.session.get(url, headers=headers, verify=self.session.verify, timeout=self._timeouts['GET'])
        _handle_http_errors(res)
        return res.json()

    def make_raw_streaming_request(self, url, mimetype=None):
        logger.debug('Beginning a REST request to: ' + url)
        self._block_ssl_warn()
        headers = {}
        if mimetype:
            headers['Accept'] = mimetype
        res = self.session.get(url, stream=True, headers=headers, verify=self.session.verify, timeout=self._timeouts['GET'])
        _handle_http_errors(res)
        return res

    def _retrieve_file(self, url, filename, dir_, mimetype):        
        logs = self.make_raw_streaming_request(url, mimetype)
        
        if dir_ is None:
            dir_ = os.getcwd()

        path = os.path.join(dir_, filename)
        try:
            with open(path, 'w+b') as logfile:
                for chunk in logs.iter_content(chunk_size=1024*64):
                    if chunk:
                        logfile.write(chunk)
        except IOError as e:
            logger.error("IOError({0}) writing application log files: {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            logger.error("Error while writing application log files")
            raise e

        return path

    def __str__(self):
        return pformat(self.__dict__)


class _BearerAuthHandler(requests.auth.AuthBase):
    def __init__(self):
        # Represents the epoch time in seconds at which
        # the token is no longer valid
        # Starts at 0 such that the first invocation of a REST request
        # Retrieves a token
        self._auth_expiry_time = 0

    @property
    def token(self):
        return self._bt

    @token.setter
    def token(self, token):
        self._bt = 'Bearer ' + token

    def __call__(self, r):
        # cur_time in seconds since Epoch
        cur_time = time.time()
        if cur_time >= self._auth_expiry_time:
            last = self._auth_expiry_time
            if last == 0:
                last = time.time()
            self._refresh_auth()
        r.headers['Authorization'] = self.token
        return r


class _ICPDAuthHandler(_BearerAuthHandler):
    def __init__(self, service_name, token):
        super(_ICPDAuthHandler, self).__init__()
        self._service_name = service_name
        self.token = token
        # user tokens practically do not expire; set expiry time to 10 years from now
        self._auth_expiry_time = time.time() + 10 * 365 * 86400

    def _refresh_auth(self):
        # the token has a life time of ~ 10 years. We do not refresh
        pass

class _ICPDExternalAuthHandler(_BearerAuthHandler):
    def __init__(self, endpoint, username, password, verify, service_name):
        super(_ICPDExternalAuthHandler, self).__init__()
        self._endpoint = endpoint
        self.__username = username
        self.__password = password
        self._service_name = service_name
        self._verify = verify
        self._cfg = self._create_cfg()

    def _refresh_auth(self):
        logger.debug("ICP4DExternal:Token refresh:")
        self._cfg = self._create_cfg()
        logger.debug("ICP4DExternal:Token refreshed:expiry:" + time.ctime(self._auth_expiry_time))

    def _create_cfg(self):
        import requests
        import urllib.parse as up
        pd = {'username': self.__username, 'password': self.__password}

        es = up.urlsplit(self._endpoint)

        if es.path.startswith('/streams/rest/instances/') or es.path.startswith('/streams/v1/instances/'):
            # Default to the default port
            cluster_port = 31843
            self._service_name = es.path.split('/')[-1]
        elif ':' in es.netloc:
            cluster_port = es.netloc.split(':')[1]
        else:
            cluster_port = 443

        cluster_ip = es.netloc.split(':')[0] if ':' in es.netloc else es.netloc

        auth_url = up.urlunsplit(('https', cluster_ip + ':' + str(cluster_port), '/icp4d-api/v1/authorize', None, None))
        r = requests.post(auth_url, json=pd, verify=self._verify, timeout=_TIMEOUTS['POST'])
        _handle_http_errors(r)
        token = r.json()['token']
        self.token = token
        # these tokens have ~ 12 hours lifetime, with 11 hours from now as expiry time we are comfortable
        self._auth_expiry_time = time.time() + 11 * 60 * 60
        
        details_url = up.urlunsplit(
            ('https', cluster_ip + ':' + str(cluster_port), 'zen-data/v2/serviceInstance/details', 'displayName=' + self._service_name, None))
        r = requests.get(details_url,
                         headers={"Authorization": "Bearer " + token},
                         verify=self._verify,
                         timeout=_TIMEOUTS['GET'])
        sr = r.json()
        mc = sr.get('_messageCode_')
        if not mc or mc != 'success' or not 'requestObj' in sr:
            raise RequestException('Query for Streams instance "' + self._service_name + '" failed: ' 
                             + json.dumps(sr, sort_keys=True, indent=2, separators=(',', ': ')))

        sro = sr['requestObj']

        try:
            service_id = sro['ID']
            service_type = sro['ServiceInstanceType']
            service_namespace = sro['ServiceInstanceNamespace']
            sca = sro['CreateArguments']
        except KeyError as ke:
            logger.critical('Mandatory JSON object missing in REST service response: requestObj/' + ke.args[0])
            raise RequestException('Mandatory JSON object missing in REST service response: requestObj/' + ke.args[0])
        
        try:
            metadata = sca['metadata']
            connection_info = sca['connection-info']
        except KeyError as ke:
            logger.critical('Mandatory JSON object missing in REST service response: requestObj/CreateArguments/' + ke.args[0])
            raise RequestException('Mandatory JSON object missing in REST service response: requestObj/CreateArguments/' + ke.args[0])

        try:
            instance = metadata['instance']
        except KeyError as ke:
            logger.critical('Mandatory JSON object missing in REST service response: requestObj/CreateArguments/metadata/' + ke.args[0])
            raise RequestException('Mandatory JSON object missing in REST service response: requestObj/CreateArguments/metadata/' + ke.args[0])

        try:
            service_name = instance['id']
        except KeyError as ke:
            logger.error('Mandatory JSON object missing in REST service response: requestObj/CreateArguments/metadata/instance/' + ke.args[0])
            raise RequestException('mandatory JSON object missing in REST service response: requestObj/CreateArguments/metadata/instance/' + ke.args[0])

        # already observed a 'None' connection_info, avoid KeyError later on
        if not connection_info:
            raise RequestException('Unable to retrieve connection details for Streams instance: ' + service_name)

        # Convert the external endpoints to use the passed in cluster ip.
        ebe = connection_info['externalBuildEndpoint']
        if ebe.startswith('https:'):
            bu = up.urlsplit(ebe)
            ebe_port = bu.port
            ebe_path = bu.path
        else: # CPD 2.5 switched to path-absolute
            ebe_port = cluster_port
            ebe_path = ebe
            
        build_url = up.urlunsplit(('https', cluster_ip + ':' + str(ebe_port), ebe_path, None, None))

        # optional build pools endpoint for edge
        build_pools_url = None
        try:
            ebpe = connection_info['externalBuildPoolsEndpoint']
            if ebpe.startswith('https:'):
                bpu = up.urlsplit(ebpe)
                ebpe_port = bpu.port
                ebpe_path = bpu.path
            else: # CPD 2.5 switched to path-absolute
                ebpe_port = cluster_port
                ebpe_path = ebpe

            build_pools_url = up.urlunsplit(('https', cluster_ip + ':' + str(ebpe_port), ebpe_path, None, None))
        except KeyError: 
            pass

        ere = connection_info['externalRestEndpoint']
        if ere.startswith('https:'):
            ru = up.urlsplit(ere)
            ere_port = ru.port
            ere_path = ru.path
        else: # CPD 2.5 switched to path-absolute
            ere_port = cluster_port
            ere_path = ere
        streams_url = up.urlunsplit(('https', cluster_ip + ':' + str(ere_port), ere_path, None, None))

        cfg = {
            'type': 'streams',
            'connection_info': {
                'serviceBuildEndpoint': build_url,
                'serviceRestEndpoint': streams_url
            },
            'user_token': token,
            'service_token': token,
            'service_token_expire': int(self._auth_expiry_time * 1000.0),
            'service_name': service_name,
            'service_namespace': service_namespace,
            'cluster_ip': cluster_ip,
            'cluster_port': cluster_port,
            'service_id': service_id,
            'externalClient':True,
        }
        if build_pools_url:
            cfg['connection_info']['serviceBuildPoolsEndpoint'] = build_pools_url
        return cfg

class _JWTAuthHandler(_BearerAuthHandler):
    def __init__(self, security_url, username, password, verify):
        super(_JWTAuthHandler, self).__init__()
        self._username = username
        self._password = password
        self._verify = verify
        self.security_url = security_url
        self._cfg = self._create_cfg()

    def _refresh_auth(self):
        logger.debug("JWTAuthHandler:Token refresh")
        self._cfg = self._create_cfg()
        logger.debug("JWT:Token refreshed:expiry: " + time.ctime(self._auth_expiry_time))
        
    def _create_cfg(self):
        import requests
        import urllib.parse as up

        pd = {'audience': 'streams'}
        r = requests.post(self.security_url, json=pd, verify=self._verify, auth=(self._username, self._password),
                          headers={'Content-Type' : 'application/json', 'Accept' : 'application/json'},
                          timeout=_TIMEOUTS['POST'])
        _handle_http_errors(r)
        self.token = r.json()['accessToken']
        self._auth_expiry_time=time.time() + 4 * 60

        es = up.urlsplit(self.security_url)

        cluster_port = es.port or 443
        cluster_ip = es.hostname


        cfg = {
                'type': 'streams',
                'service_token': self.token,
                'service_token_expire': int(self._auth_expiry_time * 1000.0),
                'cluster_ip': cluster_ip,
                'cluster_port': cluster_port,
                'externalClient':True,            
        }

        return cfg;



class _IAMAuthHandler(_BearerAuthHandler):
    def __init__(self, credentials):
        """
        Args:
            credentials: The credentials of the Streaming Analytics service.
        """
        super(_IAMAuthHandler, self).__init__()
        self._credentials = credentials
        self._api_key = self._credentials[_IAMConstants.API_KEY]

        # Determine if service is in production or staging/test
        v2url = self._credentials[_IAMConstants.V2_REST_URL]

        v2host = parse.urlparse(v2url).hostname
        if v2host.endswith('test.cloud.ibm.com') or ('stage1' in v2host and v2host.endswith('.bluemix.net')):
            self._token_url = _IAMConstants.TOKEN_URL_TEST
        else:
            self._token_url = _IAMConstants.TOKEN_URL

    def _refresh_auth(self):
        post_url = self._token_url + '?' + self._get_token_params(self._api_key)
        res = requests.post(post_url, headers = {'Accept' : 'application/json',
                                                 'Content-Type' : 'application/x-www-form-urlencoded'},
                            timeout=_TIMEOUTS['POST'])
        _handle_http_errors(res)
        res = res.json()

        self._auth_expiry_time = int(res[_IAMConstants.EXPIRATION]) - 30
        self._bearer_token = self.token = res[_IAMConstants.ACCESS_TOKEN]

    def _get_token_params(self, api_key):
        return parse.urlencode({_IAMConstants.GRANT_PARAM : _IAMConstants.GRANT_TYPE,
                                       _IAMConstants.API_KEY : api_key})



class _IAMStreamsRestClient(_StreamsRestClient):
    """Handles the session connection with the Streams REST API and Streaming Analytics service
    using IAM authentication.
    """

    # Thread local of used clients identified by service id.
    _CLIENTS = threading.local()

    # Re-use client across the same thread (Session is not thread safe).
    @staticmethod
    def _create(credentials, timeouts=_TIMEOUTS):
        clients = _IAMStreamsRestClient._CLIENTS
        if not hasattr(clients, '_clients'):
            clients._clients = {}
        service_id = credentials[_IAMConstants.SERVICE_ID]
        if service_id in clients._clients:
            return clients._clients[service_id]

        client = _IAMStreamsRestClient(credentials, timeouts=timeouts)
        clients._clients[service_id] = client
        return client
        
    def __init__(self, credentials, timeouts=_TIMEOUTS):
        """
        Args:
            credentials: The credentials of the Streaming Analytics service.
        """
        super(_IAMStreamsRestClient, self).__init__(_IAMAuthHandler(credentials), timeouts=timeouts)


class _ViewDataFetcher(object):
    """A callable which, when invoked with a thread, begins fetching data from the
    supplied view and populates the `View.items` queue.
    """
    def __init__(self, view, tuple_getter):
        self.view = view
        self.tuple_getter = tuple_getter
        self.stop = threading.Event()
        if view.bufferCapacityUnits == 'tuples':
            self.items = queue.Queue(view.bufferCapacityTuples)
        elif view.bufferCapacityUnits == 'seconds':
            self.items = queue.Queue(view.bufferCapacitySeconds * view.maximumTupleRate)
        else:
            self.items = queue.Queue(10000)

        self._last_collection_time = -1
        self._last_collection_time_count = 0

    def __call__(self):
        while not self._stopped():
            _items = self._get_deduplicated_view_items() or []
            for itm in _items:
                try:
                    self.items.put(itm, block=False)
                except queue.Full:
                    # Pop an item (and discard)
                    try:
                        self.items.get(block=False)
                    except queue.Empty:
                        pass
                    # Should not block as this should be
                    # the only producer
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
    """View on a stream.

    Attributes:
        id (str): An unique identifier for the view.
        name (str): View name.
        description (str): Description of the view.
        resourceType (str): Identifies the REST resource type, which is *view*.
        activateOption (str): Indicate when the view starts buffering data.
        maximumTupleRate (int): Maximum Number of tuples at which the view collects per second.
        logicalOperatorName (str): The logical name of the operator that contains the output port on which the view is
            created.
        bufferCapacitySeconds (int): Buffer size measured in seconds.
        bufferCapacityTuples (int): Buffer size measured in number of tuples.
        bufferCapacityUnits (str): Indicates whether the buffer capacity for the view is determined by *seconds*,
            *tuples* or *unknown*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> views = instances[0].get_views()
        >>> print (views[0].resourceType)
        view
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
        self._rest_timeout_logged_once = False

    def get_domain(self):
        """Get the Streams domain for the instance that owns this view.

        Returns:
            Domain: Streams domain for the instance owning this view.
        """
        if hasattr(self, 'domain'):
            return Domain._fetch(self.domain, self.rest_client)

    def get_instance(self):
        """Get the Streams instance that owns this view.

        Returns:
            Instance: Streams instance owning this view.
        """
        return Instance._fetch(self.instance, self.rest_client)

    def get_job(self):
        """Get the Streams job that owns this view.

        Returns:
            Job: Streams Job owning this view.
        """
        return Job._fetch(self.job, self.rest_client)

    def stop_data_fetch(self):
        """Stops the thread that fetches data from the Streams view server.
        """
        if self._data_fetcher:
            self._data_fetcher.stop.set()
            self._data_fetcher = None

    def start_data_fetch(self):
        """Starts a thread that fetches data from the Streams view server.

        Each item in the returned `Queue` represents a single tuple
        on the stream the view is attached to.
        
        Returns:
            queue.Queue: Queue containing view data.

        .. note:: This is a queue of the tuples coverted to Python
            objects, it is not a queue of :py:class:`ViewItem` objects.
        """
        self.stop_data_fetch()
        self._data_fetcher = _ViewDataFetcher(self, self._tuple_fn)
        t = threading.Thread(target=self._data_fetcher)
        t.start()
        return self._data_fetcher.items

    def fetch_tuples(self, max_tuples=20, timeout=None):
        """
        Fetch a number of tuples from this view.

        Fetching of data must have been started with
        :py:meth:`start_data_fetch` before calling this method.

        If ``timeout`` is ``None`` then the returned list will
        contain ``max_tuples`` tuples. Otherwise if the timeout is reached
        the list may contain less than ``max_tuples`` tuples.

        Args:
            max_tuples(int): Maximum number of tuples to fetch.
            timeout(float): Maximum time to wait for ``max_tuples`` tuples.

        Returns:
            list: List of fetched tuples.
        .. versionadded:: 1.12
        """
        tuples = list()
        if timeout is None:
            while len(tuples) < max_tuples:
                fetcher = self._data_fetcher
                if not fetcher:
                    break
                tuples.append(fetcher.items.get())
            return tuples

        timeout = float(timeout)
        end = time.time() + timeout
        while len(tuples) < max_tuples:
            qto = end - time.time()
            if qto <= 0:
                break
            try:
                fetcher = self._data_fetcher
                if not fetcher:
                    break
                tuples.append(fetcher.items.get(timeout=qto))
            except queue.Empty:
                break
        return tuples

    def display(self, duration=None, period=2):
        """Display a view within a Jupyter or IPython notebook.

        Provides an easy mechanism to visualize data on a stream
        using a view.

        Tuples are fetched from the view and displayed in a table
        within the notebook cell using a ``pandas.DataFrame``.
        The table is continually updated with the latest tuples from the view.

        This method calls :py:meth:`start_data_fetch` and will call
        :py:meth:`stop_data_fetch` when completed if `duration` is set.

        Args:
            duration(float): Number of seconds to fetch and display tuples. If ``None`` then the display will be updated until :py:meth:`stop_data_fetch` is called.
            period(float): Maximum update period.

        .. note::
            A view is a sampling of data on a stream so tuples that
            are on the stream may not appear in the view.

        .. note::
            Python modules `ipywidgets` and `pandas` must be installed
            in the notebook environment.
          
        .. warning::
            Behavior when called outside a notebook is undefined.

        .. versionadded:: 1.12
        """
        # Verify we are in a IPython env.
        get_ipython() # noqa : F821
        import ipywidgets as widgets
        vn = widgets.Text(value=self.description, description=self.name, disabled=True)
        active = widgets.Valid(value=True, description='Fetching', readout='Stopped')
        out = widgets.Output(layout={'border': '1px solid black'})
        hb = widgets.HBox([vn, active])
        vb = widgets.VBox([hb, out])
        display(vb) # noqa : F821
        self._display_thread = threading.Thread(target=lambda: self._display(out, duration, period, active))
        self._display_thread.start()
        
    def _display(self, out, duration, period, active):
        import pandas as pd
        import IPython
        tqueue = self.start_data_fetch()
        end = time.time() + float(duration) if duration is not None else None
        max_rows = pd.options.display.max_rows
        last = 0
        try:
            while self._data_fetcher and (duration is None or time.time() < end):
                # Slow down pace when view is busy
                gap = time.time() - last
                if gap < period:
                    time.sleep(period - gap)
                # Display latest tuples by removing earlier ones
                # Avoids display falling behind live data with
                # large view buffer
                tqs = tqueue.qsize()
                if tqs > max_rows:
                    tqs -= max_rows
                    for _ in range(tqs):
                        try:
                            tqueue.get(block=False)
                        except queue.Empty:
                            break
                tuples = self.fetch_tuples(max_rows, period)
                if not tuples:
                    if not self._data_fetcher:
                        break
                    with out:
                        display('No tuples') # noqa : F821
                else:
                    df = pd.DataFrame(tuples)
                    with out:
                        display(df) # noqa : F821
                out.clear_output(wait=True)
                last = time.time()
        except Exception as e:
            self.stop_data_fetch()
            active.value=False
            raise e
 
        self.stop_data_fetch()
        active.value=False

    def get_view_items(self):
        """Get a list of :py:class:`ViewItem` elements associated with this view.

        Returns:
            list(ViewItem): List of ViewItem(s) associated with this view.
        """
        try:
            view_items = [ViewItem(json_view_items, self.rest_client) for json_view_items
                          in self.rest_client.make_request(self.viewItems)['viewItems']]
            logger.debug("Retrieved " + str(len(view_items)) + " items from view " + self.name)
            return view_items
        except Timeout as to:
            # catches both ConnectTimeout and ReadTimeout error
            if not self._rest_timeout_logged_once:
                self._rest_timeout_logged_once = True
                logger.error("Timeout retrieving items for view " + self.name + ". timeouts = " + str(self.rest_client.timeout_for('GET')))
            return []


class ViewItem(_ResourceElement):
    """A stream tuple in view.

    Attributes:
        collectionTime (long): Epoch time when this viewItem is collected from the stream.
        data (dict): Content of this viewItem.
        resourceType (str): Identifies the REST resource type, which is *viewItem*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> views = instances[0].get_views()
        >>> viewitems = views[0].get_view_items()
        >>> print (viewitems[0].resourceType)
        viewItem
    """
    pass


class Host(_ResourceElement):
    """Resource in a Streams domain or instance.

    Attributes:
        name (str): Configuration name for the IBM Streams resource.
        resourceType (str): Identifies the REST resource type, which is *host*.
        ipAddress (str): IP address for the IBM Streams resource.
        processorCount (int): Number of processors on the IBM Streams resource.
        restrictedTags (list(str)): Set of resource tags that processing elements (PEs) must have to run on the IBM
            Streams resource.
        services (list(dict)): Name and status of each domain service that is designated to run on the IBM Streams
            resource.
        status(str): Status of the IBM Streams resource.
        tag(list(str)): Names of each tag that is assigned to the IBM Streams resource.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> domains = sc.get_domains()
        >>> hosts = domains[0].get_hosts()
        >>> print (hosts[0].resourceType)
        host
    """
    pass


class Job(_ResourceElement):
    """A running streams application.

    Attributes:
        id (str): job ID.
        name (str): Name of the job.
        resourceType (str): Identifies the REST resource type, which is *job*.
        health (str): Health indicator for the job. Some possible values for this property include *healthy*,
            *partiallyHealthy*, *partiallyUnhealthy*, *unhealthy*, and *unknown*.
        applicationName (str): Name of the streams processing application that this job is running.
        jobGroup (str): Streams 4.2/4.3 only. Identifies the job group to which this job belongs.
        startedBy (str): Identifies the user ID that started this job.
        status (str): Status of this job. Some possible values for this property include *canceling*, *running*,
            *canceled*, and *unknown*.
        submitTime (long): Epoch time when this job was submitted.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> jobs = instances[0].get_jobs()
        >>> print (jobs[0].health)
        healthy
    """

    def retrieve_log_trace(self, filename=None, dir=None):
        """Retrieves the application log and trace files of the job
        and saves them as a compressed tar file.

        An existing file with the same name will be overwritten.

        Args:
            filename (str): name of the created tar file. Defaults to `job_<id>_<timestamp>.tar.gz` where `id` is the job identifier and `timestamp` is the number of seconds since the Unix epoch, for example ``job_355_1511995995.tar.gz``.
            dir (str): a valid directory in which to save the archive. Defaults to the current directory.

        Returns:
            str: the path to the created tar file, or ``None`` if retrieving a job's logs is not supported in the version of IBM Streams to which the job is submitted.

        .. versionadded:: 1.8
        """

        if hasattr(self, "applicationLogTrace") and self.applicationLogTrace is not None:
            logger.debug("Retrieving application logs from: " + self.applicationLogTrace)
            if not filename:
                filename = _file_name('job', self.id, '.tar.gz')

            return self.rest_client._retrieve_file(self.applicationLogTrace, filename, dir, 'application/x-compressed')
        else:
            return None

    def get_views(self, name=None):
        """Get the list of :py:class:`~streamsx.rest_primitives.View` elements associated with this job.

        Args:
            name(str, optional): Returns view(s) matching `name`.  `name` can be a regular expression.  If `name`
            is not supplied, then all views associated with this instance are returned.

        Returns:
            list(streamsx.rest_primitives.View): List of views matching `name`.

        Retrieving a list of views that contain the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instances = sc.get_instances()
            >>> job = instances[0].get_jobs()[0]
            >>> views = job.get_views(name = ".*temperatureSensor*")
        """
        return self._get_elements(self.views, 'views', View, name=name)

    def get_domain(self):
        """Get the Streams domain that owns this job.

        Returns:
            Domain: Streams domain that owns this job.
        """
        if hasattr(self, 'domain'):
            return Domain._fetch(self.domain, self.rest_client)

    def get_instance(self):
        """Get the Streams instance that owns this job.

        Returns:
            Instance: Streams instance that owns this job.
        """
        return Instance._fetch(self.instance, self.rest_client)

    def get_hosts(self):
        """Get the list of :py:class:`Host` elements associated with this job.

        Returns:
            list(Host): List of Host elements associated with this job.
        """
        if hasattr(self, 'hosts'):
            return self._get_elements(self.hosts, 'hosts', Host)

    def get_operator_connections(self):
        """Get the list of :py:class:`OperatorConnection` elements associated with this job.

        Returns:
            list(OperatorConnection): List of OperatorConnection elements associated with this job.
        """
        return self._get_elements(self.operatorConnections, 'connections', OperatorConnection)

    def get_operators(self, name=None):
        """Get the list of :py:class:`Operator` elements associated with this job.

        Args:
            name(str): Only return operators matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all operators for this job are returned.

        Returns:
            list(Operator): List of Operator elements associated with this job.

        Retrieving a list of operators whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instances = sc.get_instances()
            >>> job = instances[0].get_jobs()[0]
            >>> operators = job.get_operators(name=".*temperatureSensor*")

        .. versionchanged:: 1.9 `name` parameter added.
        """
        return self._get_elements(self.operators, 'operators', Operator, name=name)

    def get_pes(self):
        """Get the list of :py:class:`PE` elements associated with this job.

        Returns:
            list(PE): List of PE elements associated with this job.

        """
        return self._get_elements(self.pes, 'pes', PE)

    def get_pe_connections(self):
        """Get the list of :py:class:`PEConnection` elements associated with this job.

        Returns:
            list(PEConnection): List of PEConnection elements associated with this job.
        """
        return self._get_elements(self.peConnections, 'connections', PEConnection)

    def get_resource_allocations(self):
        """Get the list of :py:class:`ResourceAllocation` elements associated with this job.

        Returns:
            list(ResourceAllocation): List of ResourceAllocation elements associated with this job.
        """
        if hasattr(self, 'resourceAllocations'):
            return self._get_elements(self.resourceAllocations, 'resourceAllocations', ResourceAllocation)

    def cancel(self, force=False):
        """Cancel this job.

        Args:
            force (bool, optional): Forcefully cancel this job.

        Returns:
            bool: True if the job was cancelled, otherwise False if an error occurred.
        """
        self.rest_client._cache_job_cancel(self)
        return self.rest_client._sc._delegator._cancel_job(self, force)

    def update_operators(self, job_config):
        """ Adjust a job configuration while the job is running

        Arguments:
            job_config {JobConfig} -- a job configuration overlay

        Returns:
            [JSON] -- The result of applying the new jobConfig?
        """
        return self.rest_client._sc._delegator._update_operators(self, job_config)

    def get_job_group(self):
        """
        Get the job group associated with this job.

        .. versionadded:: 1.13.13
        """
        # 4.x returned just the name for jobGroup.
        if self.jobGroup.startswith('https://'):
            return JobGroup._fetch(self.jobGroup, self.rest_client)
        return JobGroup({'name':self.jobGroup}, self.rest_client)

class JobGroup(_ResourceElement):
    """A job group definition.

        Attributes:
        name(str): Job group name.
        resourceType(str): Identifies the REST resource type, which is *jobGroup*.

        .. versionadded:: 1.13.13
    """
    pass

class Operator(_ResourceElement):
    """An operator invocation within a job.

    Attributes:
        name(str): Operator name.
        resourceType(str): Identifies the REST resource type, which is *operator*.
        operatorKind(str): SPL primitive operator type for this operator.
        indexWithinJob(int): Index of this operator within the job.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> operators = instances[0].get_operators()
        >>> print (operators[0].resourceType)
        operator
    """
    def get_metrics(self, name=None):
        """Get metrics for this operator.

        Args:
            name(str, optional): Only return metrics matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all metrics for this operator are returned.

        Returns:
             list(Metric): List of matching metrics.

        Retrieving a list of metrics whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instances = sc.get_instances()
            >>> operator = instances[0].get_operators()[0]
            >>> metrics = op.get_metrics(name='*temperatureSensor*')
        """
        return self._get_elements(self.metrics, 'metrics', Metric, name=name)

    def get_host(self):
        """Get resource this operator is currently executing in.
           If the operator is running on an externally
           managed resource ``None`` is returned.

        Returns:
            Host: Resource this operator is running on.

        .. versionadded:: 1.9
        """
        if hasattr(self, 'host') and self.host:
            return Host._fetch(self.host, self.rest_client)

    def get_pe(self):
        """Get the Streams processing element this operator is executing in.

        Returns:
            PE: Processing element for this operator.

        .. versionadded:: 1.9
        """
        return PE._fetch(self.pe, self.rest_client)

    def get_output_ports(self):
        """Get list of output ports for this operator.

        Returns:
            list(OperatorOutputPort): Output ports for this operator.

        .. versionadded:: 1.9
        """
        return self._get_elements(self.outputPorts, 'outputPorts', OperatorOutputPort)

    def get_input_ports(self):
        """Get list of input ports for this operator.

        Returns:
            list(OperatorInputPort): Input ports for this operator.

        .. versionadded:: 1.9
        """
        return self._get_elements(self.inputPorts, 'inputPorts', OperatorInputPort)

    def get_job(self):
        """Get the Streams job that owns this operator.

        Returns:
            Job: Streams Job owning this operator.

        """
        return Job._fetch(self.job, self.rest_client)


class OperatorConnection(_ResourceElement):
    """Connection between operators.

    Attributes:
        id(str): Unique ID of this operator connection within the instance.
        resourceType(str): Identifies the REST resource type, which is *operator*.
        required (bool): Indicates whether the connection is required.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> operatorconnections = instances[0].get_operator_connections()
        >>> print (operatorconnections[0].resourceType)
        operatorConnection
    """
    pass


class OperatorOutputPort(_ResourceElement):
    """Operator output port.

    Attributes:
        name(str): Name of this output port.
        resourceType(str): Identifies the REST resource type, which is *operatorOutputPort*.
        indexWithinOperator(int): Index of the output port within the operator.
        streamName(str): Name of the stream that is associated with this output port.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> exportedstreams = instances[0].get_exported_streams()
        >>> operatoroutputport = exportedstreams[0].get_operator_output_port()
        >>> print (operatoroutputport.resourceType)
        operatorOutputPort
    """
    def get_metrics(self, name=None):
        """Get metrics for this output port.

        Args:
            name(str, optional): Only return metrics matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all metrics for this output port are returned.

        Returns:
             list(Metric): List of matching metrics.

        Retrieving a list of metrics whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instances = sc.get_instances()
            >>> exportedstreams = instances[0].get_exported_streams()
            >>> operatoroutputport = exportedstreams[0].get_operator_output_port()
            >>> operatoroutputport.get_metrics(name='*temperatureSensor*')

        .. versionadded:: 1.9
        """
        return self._get_elements(self.metrics, 'metrics', Metric, name=name)

    def get_connections(self):
        """Get the list of :py:class:`OperatorConnection` elements associated with this port.

        Returns:
            list(OperatorConnection): List of OperatorConnection elements associated with this port.

        .. versionadded:: 1.13
        """
        return self._get_elements(self.connections, 'connections', OperatorConnection)

class OperatorInputPort(_ResourceElement):
    """Operator input port.

    Attributes:
        name(str): Name of this input port.
        resourceType(str): Identifies the REST resource type, which is *operatorInputPort*.
        indexWithinOperator(int): Index of the input port within the operator.

    .. versionadded:: 1.9
    """
    def get_metrics(self, name=None):
        """Get metrics for this input port.

        Args:
            name(str, optional): Only return metrics matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all metrics for this input port are returned.

        Returns:
             list(Metric): List of matching metrics.
        
        Retrieving a list of metrics whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instances = sc.get_instances()
            >>> operator = instances[0].get_operators()[0]
            >>> input_port = operator.get_input_ports()[0]
            >>> metrics = input_port.get_metrics(name='*temperatureSensor*')
        """
        return self._get_elements(self.metrics, 'metrics', Metric, name=name)

    def get_connections(self):
        """Get the list of :py:class:`OperatorConnection` elements associated with this port.

        Returns:
            list(OperatorConnection): List of OperatorConnection elements associated with this port.

        .. versionadded:: 1.13
        """
        return self._get_elements(self.connections, 'connections', OperatorConnection)


class Metric(_ResourceElement):
    """Streams custom or system metric.

    Attributes:
        name(str): Name of this metric.
        resourceType(str): Identifies the REST resource type, which is *metric*.
        description(str): Describes this metric.
        lastTimeRetrieved(str): Epoch time when the metric was most recently retrieved.
        metricKind(str): Kind of metric. Some possible values include *counter*, *gauge*, *time* and *unknown*.
        metricType(str): Type of metric. Some possible values include *system*, *custom* and *unknown*.
        value(int): Value for the metric.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> operators = instances[0].get_operators()
        >>> metrics = operators[0].get_metrics()
        >>> print (metrics[0].resourceType)
        metric
    """
    _USE_CACHE = False


class PE(_ResourceElement):
    """Processing element (PE) within a job.
    A processing element hosts one or more operators within a single job.

    Attributes:
        id(str): PE ID.
        resourceType(str): Identifies the REST resource type, which is *pe*.
        health(str): Health indicator for this PE. Some possible values include *healthy*, *partiallyHealthy*,
            *partiallyUnhealthy*, *unhealthy*, and *unknown*.
        indexWithinJob(int): Index of the PE within the job.
        launchCount(int): Number of times this PE was started manually or automatically because of failures.
        optionalConnections(str): Status of optional connections for this PE. Some possible values include *connected*,
            *disconnected*, *partiallyConnected*, and *unknown*.
        pendingTracingLevel(str): Describes a pending change to the granularity of the trace information that is
            stored for this PE. Some possible values include *off*, *error*, *debug* and *trace*.  The value is *None*,
            if no change is pending.
        processId(str): Operating system process ID for this PE.
        relocatable(bool): Indicates whether this PE can be relocated to a different resource.
        requiredConnections(str): Status of the required connections for this PE. Some possible values include
            *connected*, *disconnected*, *partiallyConnected*, and *unknown*.
        restartable(bool): Indicates whether this PE can be restarted.
        status(str): Status of this PE.
        statusReason(str): Additional information for the status of this PE.
        tracingLevel(str): Granularity of the trace information. Some possible values include *off*, *error*, *debug*
            and *trace*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> pes = instances.get_pes()
        >>> print(pes[0].resourceType)
        pe
    """

    def get_host(self):
        """Get resource this processing element is currently executing in.
           If the processing element is running on an externally
           managed resource ``None`` is returned.

        Returns:
            Host: Resource this processing element is running on.

        .. versionadded:: 1.9
        """
        if hasattr(self, 'host') and self.host:
            return Host._fetch(self.host, self.rest_client)

    def get_resource(self):
        """Get resource this processing element is currently executing in.

        Returns:
            Host: Resource this processing element is running on.

        .. versionadded:: 1.13.13
        """
        if hasattr(self, 'resource') and self.resource:
            return Resource._fetch(self.resource, self.rest_client)

    def retrieve_trace(self, filename=None, dir=None):
        """Retrieves the application trace files for this PE
        and saves them as a plain text file.

        An existing file with the same name will be overwritten.

        Args:
            filename (str): name of the created file. Defaults to `pe_<id>_<timestamp>.trace` where `id` is the PE identifier and `timestamp` is the number of seconds since the Unix epoch, for example ``pe_83_1511995995.trace``.
            dir (str): a valid directory in which to save the file. Defaults to the current directory.

        Returns:
            str: the path to the created file, or None if retrieving a job's logs is not supported in the version of streams to which the job is submitted.

        .. versionadded:: 1.9
        """
        if hasattr(self, "applicationTrace") and self.applicationTrace is not None:
            logger.debug("Retrieving PE trace: " + self.applicationTrace)
            if not filename:
                filename = _file_name('pe', self.id, '.trace')
            return self.rest_client._retrieve_file(self.applicationTrace, filename, dir, 'text/plain')

        else:
            return None

    def retrieve_console_log(self, filename=None, dir=None):
        """Retrieves the application console log (standard out and error)
        files for this PE and saves them as a plain text file.

        An existing file with the same name will be overwritten.

        Args:
            filename (str): name of the created file. Defaults to `pe_<id>_<timestamp>.stdouterr` where `id` is the PE identifier and `timestamp` is the number of seconds since the Unix epoch, for example ``pe_83_1511995995.trace``.
            dir (str): a valid directory in which to save the file. Defaults to the current directory.

        Returns:
            str: the path to the created file, or None if retrieving a job's logs is not supported in the version of streams to which the job is submitted.

        .. versionadded:: 1.9
        """        
        if hasattr(self, "consoleLog") and self.consoleLog is not None:
            logger.debug("Retrieving PE console log: " + self.consoleLog)
            if not filename:
                filename = _file_name('pe', self.id, '.stdouterr')
            return self.rest_client._retrieve_file(self.consoleLog, filename, dir, 'text/plain')

        else:
            return None

    def get_metrics(self, name=None):
        """Get metrics for this PE.

        Args:
            name(str, optional): Only return metrics matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all metrics for this PE are returned.

        Returns:
             list(Metric): List of matching metrics.
        
        Retrieving a list of metrics whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instances = sc.get_instances()
            >>> pe = instances.get_pes()[0]
            >>> metrics = pe.get_metrics(name='*temperatureSensor*')

        .. versionadded:: 1.9
        """
        return self._get_elements(self.metrics, 'metrics', Metric, name=name)

    def get_resource_allocation(self):
        """Get the :py:class:`ResourceAllocation` element tance.

        Returns:
            ResourceAllocation: Resource allocation used to access information about the resource where this PE is running.

        .. versionadded:: 1.9
        """
        if hasattr(self, 'resourceAllocation'):
            return ResourceAllocation._fetch(self.resourceAllocation, self.rest_client)

    def get_job(self):
        """Get the Streams job that owns this PE.

        Returns:
            Job: Streams Job owning this PE.
        """
        return Job._fetch(self.job, self.rest_client)


class PEConnection(_ResourceElement):
    """Stream connection between two PEs.

    Attributes:
        id(str): PE connection ID.
        resourceType(str): Identifies the REST resource type, which is *peConnection*.
        required(bool): Indicates whether this connection is required.
        status(str): Status of this connection. Some possible values include *connected*, *disconnected*, and
            *unknown*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> peconnections = instances.get_pe_connections()
        >>> print(peconnections[0].resourceType)
        peConnection
    """
    pass


class ResourceAllocation(_ResourceElement):
    """A resource that is allocated to an IBM Streams instance.

    Attributes:
        resourceType(str): Identifies the REST resource type, which is *resourceAllocation*.
        applicationResource(bool): Indicates whether this resource is an application resource, which is used to run
            streams processing applications.
        schedulerStatus(str): Indicates whether this resource is schedulable for the instance.
        status(str): Status of this resource for the instance.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> allocations = instances.get_resource_allocations()
        >>> print(allocations[0].resourceType)
        resourceAllocation
    """
    def get_resource(self):
        """Get the :py:class:`Resource` of the resource allocation.

        Returns:
            Resource: Resource for this allocation.

        .. versionadded:: 1.9
        """
        return Resource._fetch(self.resource, self.rest_client)

    def get_pes(self):
        """Get the list of :py:class:`PE` running on this resource
        in its instance.

        Returns:
            list(PE): List of PE running on this resource.

        .. note:: If ``applicationResource`` is `False` an empty list is returned.
        .. versionadded:: 1.9
        """
        if self.applicationResource:
            return self._get_elements(self.pes, 'pes', PE)
        else:
            return []

    def get_jobs(self, name=None):
        """Retrieves jobs running on this resource in its instance.

        Args:
            name (str, optional): Only return jobs containing property **name** that matches `name`. `name` can be a
                regular expression. If `name` is not supplied, then all jobs are returned.

        Returns:
            list(Job): A list of jobs matching the given `name`.
        
        .. note:: If ``applicationResource`` is `False` an empty list is returned.
        .. versionadded:: 1.9
        """
        if self.applicationResource:
            return self._get_elements(self.jobs, 'jobs', Job, None, name)
        else:
            return []


class ActiveService(_ResourceElement):
    """Domain or instance service.

    Attributes:
        resourceType(str): Identifies the REST resource type, which is *activeService*.
        leader(bool): If *True*, this service is a standby service.
        processId(str): Process ID of this service.
        startTime(long): Epoch time when this service started.
        status(str): Status of this service. Some possible values include *stopped*, *running*, *failed*, and
            *unknown*.
        type(str): Type of this service.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> services = instances.get_active_services()
        >>> print(services[0].resourceType)
        activeService
    """
    pass


class Installation(_ResourceElement):
    """IBM Streams installation.

    Attributes:
        resourceType(str): Identifies the REST resource type, which is *installation*.
        architecture(str): Hardware architecture on which product is installed.
        buildVersion(str): Product build ID.
        editionName(str): Product edition.
        fullProductVersion(str): Full product version, including any hot fix.
        minimumOSBaseVersion(str): Minimum operating system version requirement.
        minimumOSPatchVersion(str): Minimum operating system patch requirement.
        productName(str): Product name.
        productVersion(str): Product version.
    """
    pass


class ImportedStream(_ResourceElement):
    """Stream imported by a job.

    Attributes:
        resourceType(str): Identifies the REST resource type, which is *importedStream*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> importedstreams = instances[0].get_imported_streams()
        >>> print (importedstreams[0].resourceType)
        importedStream
    """
    pass


class ExportedStream(_ResourceElement):
    """Stream exported stream by a job.

    Attributes:
        resourceType(str): Identifies the REST resource type, which is *exportedStream*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> exportedstreams = instances[0].get_exported_streams()
        >>> print (exportedstreams[0].resourceType)
        exportedStream
    """
    def get_operator_output_port(self):
        """Get the output port of this exported stream.

        Returns:
            OperatorOutputPort: Output port of this exported stream.
        """
        return OperatorOutputPort._fetch(self.operatorOutputPort, self.rest_client)

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
    """IBM Streams instance.

    Attributes:
        id(str): Unique ID for this instance.
        resourceType(str): Identifies the REST resource type, which is *instance*.
        creationTime(long): Epoch time when this instance was created.
        creationuser(str): User ID that created this instance.
        health(str): Summarize status of the jobs in the instance. Some possible values include *healthy*,
            *partiallyHealthy*, *partiallyUnhealthy*, *unhealthy*, and *unknown*.
        owner(str): User ID that owns this instance.
        startTime(long): Epoch time when this instance was started.
        status(str): Status of this instance. Some possible values include *running*, *failed*, *stopped*, and
            *unknown*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> instances = sc.get_instances()
        >>> print (instances[0].resourceType)
        instance
    """

    def __init__(self, json_rep, rest_client):
        super(Instance, self).__init__(json_rep, rest_client)
        self._delegator = rest_client._sc._delegator

    @staticmethod
    def _is_service_def(config):
        return 'connection_info' in config and config.get('type', None) == 'streams' and 'service_token' in config

    @staticmethod
    def _find_service_def(config):
        """
        Returns a service definition dict with connection_info, type, service_name, service_id, service_namespace,
        refreshed service_token, service_token_expire, and user_token
        """
        if Instance._is_service_def(config):
            service = config
        else:
            service = config.get(streamsx.topology.context.ConfigParams.SERVICE_DEFINITION)

        if service and Instance._is_service_def(service):
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
    def _clear_service_info(config):
        del config['connection_info']
        del config['type']
        del config['service_token']
        if hasattr(config, 'user_token'):
            config.pop('user_token')

    @staticmethod
    def of_service(config):
        """Connect to an IBM Streams service instance running in Cloud Pak for Data.

        The instance is specified in `config`. The configuration may be code injected from the list of services
        in a Jupyter notebook running in ICPD or manually created. The code that selects a service instance by name is::

            # Two lines are code injected in a Jupyter notebook by selecting the service instance
            from icpd_core import ipcd_util
            cfg = icpd_util.get_service_details(name='instanceName', instance_type='streams')

            instance = Instance.of_service(cfg)

        SSL host verification is disabled by setting :py:const:`~streamsx.topology.context.ConfigParams.SSL_VERIFY`
        to ``False`` within `config` before calling this method::

            cfg[ConfigParams.SSL_VERIFY] = False
            instance = Instance.of_service(cfg)

        Args:
            config(dict): Configuration of IBM Streams service instance.

        Returns:
            Instance: Instance representing for IBM Streams service instance.

        .. note:: Only supported when running within the ICPD cluster,
            for example in a Jupyter notebook within a ICPD project.

        .. versionadded:: 1.12
        """
        service = Instance._find_service_def(config)
        if not service:
            raise ValueError()
        endpoint = service['connection_info'].get('serviceRestEndpoint')
        resource_url, name = Instance._root_from_endpoint(endpoint)

        sc = streamsx.rest.StreamsConnection(resource_url=resource_url, auth=_ICPDAuthHandler(name, service['service_token']))
        if streamsx.topology.context.ConfigParams.SSL_VERIFY in config:
                sc.session.verify = config[streamsx.topology.context.ConfigParams.SSL_VERIFY]
        return sc.get_instance(name)

    @staticmethod
    def _root_from_endpoint(endpoint):
        import urllib.parse as up
        esu = up.urlsplit(endpoint)
        
        if esu.path.startswith('/streams/rest/instances/'):
            # CPD < 3.5
            es = endpoint.split('/')
            name = es[len(es)-1]
            root_url = endpoint.split('/streams/rest/instances/')[0]
            resource_url = root_url + '/streams/rest/resources'
            return resource_url, name
        elif esu.path.startswith('/streams/v1/instances/'):
            # with CPD >= 3.5 '/streams/rest/...' changed to '/streams/v1/...'
            es = endpoint.split('/')
            name = es[len(es)-1]
            root_url = endpoint.split('/streams/v1/instances/')[0]
            # /streams/rest/resources --> streams/v1/roots
            resource_url = root_url + '/streams/v1/roots'
            return resource_url, name
        else:
            return None, None

    @staticmethod
    def of_endpoint(endpoint=None, service_name=None, username=None, password=None, verify=None):
        """
        Connect to a Cloud Pak for Data IBM Streams instance.

        Two configurations are supported.

        .. rubric:: Integrated configuration

        The Streams instance is defined using the Cloud Pak for Data
        deployment endpoint (URL) and the Streams service name.

        The endpoint is passed in as `endpoint` defaulting the the
        environment variable ``CP4D_URL``.
        An example is `https://cp4d_server:31843`.

        The Streams service name is passed in as `service_name` defaulting
        to the environment variable ``STREAMS_INSTANCE_ID``.

        .. rubric:: Standalone configuration

        The Streams instance is defined using its Streams REST api
        endpoint, which is its SWS service.

        The endpoint is passed in as `endpoint` defaulting the the
        environment variable ``STREAMS_REST_URL``.
        An example is `https://streams_sws_service:34679`.

        No service name is specified thus `service_name` should be passed
        as ``None`` or not set.

        Args:
            endpoint(str): Endpoint defining the Streams instance.
            service_name(str): Streams instance name for a integrated configuration.  This value is ignored for a standalone configuration.
            username(str): User name to authenticate as. Defaults to the environment variable ``STREAMS_USERNAME`` or the operating system identifier if not set.
            password(str): Password for authentication. Defaults to the environment variable ``STREAMS_PASSWORD`` or the operating system identifier if not set.
            verify: SSL verification. Set to ``False`` to disable SSL verification. Defaults to SSL verification being enabled.

        Returns:
            Instance: Connection to Streams instance or ``None`` if insufficient configuration was provided.

        .. versionadded:: 1.13
        """
        possible_integ = True
        if not endpoint:
            endpoint = os.environ.get('CP4D_URL')
            if not endpoint:
                endpoint = os.environ.get('STREAMS_REST_URL')
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
            url = auth._cfg['connection_info'].get('serviceRestEndpoint')
            return streamsx.rest._InstanceSc.get_instance(url, auth, verify)

        else:
            # this is a stand-alone config
            parsed = parse.urlparse(endpoint)
            resource_url = parse.urlunparse((parsed.scheme, parsed.netloc, "/streams/rest/resources", None, None, None))
            # TODO standalone: Streams in CPD3.5: /streams/rest/resources changes to /streams/v1/roots

            # Create a connection using basic authentication.  This will be
            # used to attempt to discover the security service.  If there
            # is no security service, this connection will continue to be
            # used; otherwise, it will be replaced by one using the 
            # security service.
            sc = streamsx.rest.StreamsConnection(resource_url=resource_url, username=username, password=password)
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
                        sc = streamsx.rest.StreamsConnection(resource_url=resource_url, auth=auth)
                        if verify is not None:
                            sc.rest_client.session.verify = verify
                        break
                    except:
                        pass
                  

            # There should be exactly one instance in this configuration.
            return sc.get_instances()[0]

    def get_operators(self, name=None):
        """Get the list of :py:class:`Operator` elements associated with this instance.

        Args:
            name(str): Only return operators matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all operators for this instance are returned.

        Returns:
            list(Operator): List of Operator elements associated with this instance.

        Retrieving a list of operators whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instance = sc.get_instances()[0]
            >>> operators = instance.get_operators(name=".*temperatureSensor*")

        .. versionchanged:: 1.9 `name` parameter added.
        """
        return self._get_elements(self.operators, 'operators', Operator, name=name)

    def get_operator_connections(self):
        """Get the list of :py:class:`OperatorConnection` elements associated with this instance.

        Returns:
            list(OperatorConnection): List of OperatorConnection elements associated with this instance.
        """
        return self._get_elements(self.operatorConnections, 'connections', OperatorConnection)

    def get_pes(self):
        """Get the list of :py:class:`PE` elements associated with this instance resource.

        Returns:
            list(PE): List of PE elements associated with this instance.
        """
        return self._get_elements(self.pes, 'pes', PE)

    def get_pe_connections(self):
        """Get the list of :py:class:`PEConnection` elements associated with this instance.

        Returns:
            list(PEConnection): List of PEConnection elements associated with this instance.
        """
        return self._get_elements(self.peConnections, 'connections', PEConnection)

    def get_views(self, name=None):
        """Get the list of :py:class:`~streamsx.rest_primitives.View` elements associated with this instance.

        Args:
            name(str, optional): Returns view(s) matching `name`.  `name` can be a regular expression.  If `name`
            is not supplied, then all views associated with this instance are returned.

        Returns:
            list(streamsx.rest_primitives.View): List of views matching `name`.

        Retrieving a list of views whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instance = sc.get_instances()[0]
            >>> view = instance.get_views(name=".*temperatureSensor*")
        """
        return self._get_elements(self.views, 'views', View, name=name)

    def get_hosts(self):
        """Get the list of :py:class:`Host` element associated with this instance.

        Returns:
            list(Host): List of Host element associated with this instance.
        """
        if hasattr(self, 'hosts'):
            return self._get_elements(self.hosts, 'hosts', Host)

    def get_domain(self):
        """Get the Streams domain that owns this instance.

        Returns:
            Domain: Streams domain owning this instance.
        """
        if hasattr(self, 'domain'):
            return Domain._fetch(self.domain, self.rest_client)

    def get_jobs(self, name=None):
        """Retrieves jobs running in this instance.

        Args:
            name (str, optional): Only return jobs containing property **name** that matches `name`. `name` can be a
                regular expression. If `name` is not supplied, then all jobs are returned.

        Returns:
            list(Job): A list of jobs matching the given `name`.
        
        Retrieving a list of jobs whose name contains the string "temperatureSensor" could be performed as followed
        Example:
            >>> from streamsx import rest
            >>> sc = rest.StreamingAnalyticsConnection()
            >>> instance = sc.get_instances()[0]
            >>> jobs = instance.get_jobs(name=".*temperatureApplication*")
        """
        return self._get_elements(self.jobs, 'jobs', Job, None, name)

    def get_job_groups(self, name=None):
        """Retrieves job groups defined in this instance.

        Args:
            name (str, optional): Only return job groups containing property **name** that matches `name`. `name` can be a
                regular expression. If `name` is not supplied, then all job groups are returned.

        Returns:
            list(JobGroup): A list of job groups matching the given `name`.

        Only supported for Streams 5.0 and later.

        .. versionadded:: 1.13.13
        """
        if hasattr(self, 'jobGroups'):
            return self._get_elements(self.jobGroups, 'jobGroups', JobGroup, None, name)

    def get_job(self, id):
        """Retrieves a job matching the given `id`

        Args:
            id (str): Job `id` to match.

        Returns:
            Job: Job matching the given `id`

        Raises:
            ValueError: No resource matches given `id` or multiple resources matching given `id`
        """
        return self._get_element_by_id(self.jobs, 'jobs', Job, str(id))

    def get_imported_streams(self):
        """Get the list of :py:class:`ImportedStream` elements associated with this instance.

        Returns:
            list(ImportedStream): List of ImportedStream elements associated with this instance.
        """
        return self._get_elements(self.importedStreams, 'importedStreams', ImportedStream)

    def get_exported_streams(self):
        """Get the list of :py:class:`ExportedStream` elements associated with this instance.

        Returns:
            list(ExportedStream): List of ExportedStream elements associated with this instance.
        """
        return self._get_elements(self.exportedStreams, 'exportedStreams', ExportedStream)

    def get_active_services(self):
        """Get the list of :py:class:`ActiveService` elements associated with this instance.

        Returns:
            list(ActiveService): List of ActiveService elements associated with this instance.
        """
        if hasattr(self, 'activeServices'):
            return self._get_elements(self.activeServices, 'activeServices', ActiveService)

    def get_resource_allocations(self):
        """Get the list of :py:class:`ResourceAllocation` elements associated with this instance.

        Returns:
            list(ResourceAllocation): List of ResourceAllocation elements associated with this instance.
        """
        if hasattr(self, 'resourceAllocations'):
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

    def upload_bundle(self, bundle):
        """Upload a Streams application bundle (sab) to the instance.

        Uploading a bundle allows job submission from the returned
        :py:class:`ApplicationBundle`.

        Args:
            bundle(str): path to a Streams application bundle (sab file)
                containing the application to be uploaded.

        Returns:
            ApplicationBundle: Application bundle representing the
            uploaded bundle. 

        .. note::
            When an instance does not support uploading a bundle the
            returned `ApplicationBundle` represents the local file
            ``bundle`` tied to this instance. The returned object
            may still be used for job submission.
         
        .. versionadded:: 1.11
        """
        return self._delegator._upload_bundle(self, bundle)

    def submit_job(self, bundle, job_config=None):
        """Submit a application to be run in this instance.

        Args:
            bundle(str): path to a Streams application bundle (sab file)
                containing the application to be submitted
            job_config(JobConfig): a job configuration overlay

        
        Returns:
            Job: Resulting job instance.

        .. versionadded:: 1.11
        """
        return self.upload_bundle(bundle).submit_job(job_config)

    def get_application_configurations(self, name=None):
        """Retrieves application configurations for this instance.

        Args:
            name (str, optional): Only return application configurations containing property **name** that matches `name`. `name` can be a
                regular expression. If `name` is not supplied, then all application configurations are returned.

        Returns:
            list(ApplicationConfiguration): A list of application configurations matching the given `name`.
        
        .. versionadded 1.12
        """
        if hasattr(self, 'applicationConfigurations'):
           return self._get_elements(self.applicationConfigurations, 'applicationConfigurations', ApplicationConfiguration, None, name)

    def create_application_configuration(self, name, properties, description=None):
        """Create an application configuration.

        Args:
            name (str, optional): Only return application configurations containing property **name** that matches `name`. `name` can be a
        .. versionadded 1.12
        """
        if not hasattr(self, 'applicationConfigurations'):
            raise NotImplementedError()

        cv = ApplicationConfiguration._props(name, properties, description)

        res = self.rest_client.session.post(self.applicationConfigurations,
            headers = {'Accept' : 'application/json'},
            json=cv,
            timeout=self.rest_client.timeout_for('POST'))
        _handle_http_errors(res)
        return ApplicationConfiguration(res.json(), self.rest_client)


class ResourceTag(object):
    """Resource tag defined in a Streams domain

    Attributes:
        definition_format_properties(bool): Indicates whether the resource definition consists of one or more
            properties.
        description(str): Tag description.
        name(str): Tag name.
        properties_definition(list(str)): Contains the properties of the resource definition. Only present if
            `definition_format_properties` is *True*.
        reserved(bool): If *True*, this tag is defined by IBM Streams, and cannot be modified.
    """
    def __init__(self, json_resource_tag):
        self.definition_format_properties = json_resource_tag['definitionFormatProperties']
        self.description = json_resource_tag['description']
        self.name = json_resource_tag['name']
        self.properties_definition = json_resource_tag['propertiesDefinition']
        self.reserved = json_resource_tag['reserved']

    def __str__(self):
        return pformat(self.__dict__)


class ActiveVersion(object):
    """Contains IBM Streams installation information

    Attributes:
        architecture(str): Hardware architecture on which product is installed.
        build_version(str): Product build ID.
        edition_name(str): Product edition.
        full_product_version(str): Full product version, including any hot fix.
        minimum_os_base_version(str): Minimum operating system version requirement.
        minimum_os_patch_version(str): Minimum operating system patch requirement.
        product_name(str): Product name.
        product_version(str): Product version.
    """
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

    Attributes:
        topic(str): Published topic
        schema(str): Schema of topic
    """
    def __init__(self, topic, schema):
        """
        Args:
            topic: Published topic.
            schema: Schema of topic.
        """
        self.topic = topic
        self.schema = schema

    def __repr__(self):
        return pformat(self.__dict__)


class Domain(_ResourceElement):
    """IBM Streams domain. A domain contains instances that support
    running Streams applications as jobs.

    Attributes:
        id(str): Unique ID for this domain.
        resourceType(str): Identifies the REST resource type, which is *domain*.
        creationTime(long): Epoch time when this domain was created.
        creationuser(str): User ID that created this domain.
        status(str): Status of this domain.  Some possible values include *running*, *stopping*, *stopped*,
            *starting*, *removing*, and *unknown*.

    Example:
        >>> from streamsx import rest
        >>> sc = rest.StreamingAnalyticsConnection()
        >>> domains = sc.get_domains()
        >>> print (domains[0].resourceType)
        domain
    """
    def get_instances(self):
        """Get the list of :py:class:`Instance` elements associated with this domain.

        Returns:
            list(Instance): List of Instance elements associated with this domain.
        """
        return self._get_elements(self.instances, 'instances', Instance)

    def get_hosts(self):
        """Get the list of :py:class:`Host` elements associated with this domain.

        Returns:
            list(Host): List of Host elements associated with this domain.
        """
        if hasattr(self, 'hosts'):
            return self._get_elements(self.hosts, 'hosts', Host)

    def get_active_services(self):
        """Get the list of :py:class:`ActiveService` elements associated with this domain.

        Returns:
            list(ActiveService): List of ActiveService elements associated with this domain.
        """
        if hasattr(self, 'activeServices'):
            return self._get_elements(self.activeServices, 'activeServices', ActiveService)

    def get_resource_allocations(self):
        """Get the list of :py:class:`ResourceAllocation` elements associated with this domain.

        Returns:
            list(ResourceAllocation): List of ResourceAllocation elements associated with this domain.
        """
        if hasattr(self, 'resourceAllocations'):
            return self._get_elements(self.resourceAllocations, 'resourceAllocations', ResourceAllocation)

    def get_resources(self):
        """Get the list of :py:class:`Resource` elements associated with this domain.

        Returns:
            list(Resource): List of Resource elements associated with this domain.
        """
        return self._get_elements(self.resources, 'resources', Resource)

class Resource(_ResourceElement):
    """A resource available to a IBM Streams domain.

    Attributes:
        id(str): Resource identifier.
        displayName(str): Resource display name.
        ipAddress(str): IP address.
        status(str): Resource status.
        tags(list[str]): Tags assigned to resource.

    .. versionadded:: 1.9
    """
    def get_metrics(self, name=None):
        """Get metrics for this resource.

        Args:
            name(str, optional): Only return metrics matching `name`, where `name` can be a regular expression.  If
                `name` is not supplied, then all metrics for this resource are returned.

        Returns:
             list(Metric): List of matching metrics.
        """
        return self._get_elements(self.metrics, 'metrics', Metric, name=name)

class RestResource(_ResourceElement):
    """HTTP REST resource identifier.

    Attributes:
        name(str): Resource name.
        resource(str): A string that identifies the URI for the resource.

    .. versionchanged:: 1.9 Changed to `RestResource` from `Resource`.
    """
    def get_resource(self):
        """Make a request against this REST resource.
           Returns:
               dict: JSON response.
        """
        return self.rest_client.make_request(self.resource)


class StreamingAnalyticsService(object):
    """Streaming Analytics service running on IBM Cloud.
    """
    def __init__(self, rest_client, credentials):
        # If IAM, create a V2 delegator, if basic http auth, create a V1 delegator
        #
        # Delegators are required because we need to keep StreamingAnalyticsService
        # around for backwards compatibility, yet it also needs to work with basic
        # and IAM authentication.
        if 'v2_rest_url' in credentials and 'userid' not in credentials and 'password' not in credentials:
            self._delegator = _StreamingAnalyticsServiceV2Delegator(rest_client, credentials)
        else:
            self._delegator = _StreamingAnalyticsServiceV1Delegator(rest_client, credentials)

    def submit_job(self, bundle, job_config=None):
        """Submit a Streams Application Bundle (sab file) to
        this Streaming Analytics service.
        
        Args:
            bundle(str): path to a Streams application bundle (sab file)
                containing the application to be submitted
            job_config(JobConfig): a job configuration overlay
        
        Returns:
            dict: JSON response from service containing 'name' field with unique
                job name assigned to submitted job, or, 'error_status' and
                'description' fields if submission was unsuccessful.
        """
        return self._delegator._submit_job(bundle=bundle, job_config=job_config)

    def cancel_job(self, job_id=None, job_name=None):
        """Cancel a running job.

        Args:
            job_id (str, optional): Identifier of job to be canceled.
            job_name (str, optional): Name of job to be canceled.

        Returns:
            dict: JSON response for the job cancel operation.
        """
        return self._delegator.cancel_job(job_id=job_id, job_name = job_name)

    def start_instance(self):
        """Start the instance for this Streaming Analytics service.

        Returns:
            dict: JSON response for the instance start operation.
        """
        return self._delegator.start_instance()

    def stop_instance(self):
        """Stop the instance for this Streaming Analytics service.

        Returns:
            dict: JSON response for the instance start operation.
        """
        return self._delegator.stop_instance()

    def get_instance_status(self):
        """Get the status the instance for this Streaming Analytics service.

        Returns:
            dict: JSON response for the instance status operation.
        """
        return self._delegator.get_instance_status()

class _StreamingAnalyticsServiceV2Delegator(object):
    """Delegator for IAM access to a Streaming Analytics service
    """
    def __init__(self, rest_client, credentials):
        """
        Args:
            rest_client (_IAMStreamsRestClient): The client used to make REST calls.
            credentials (str): credentials for accessing Streaming Analytics service.
        """
        self.rest_client = rest_client
        self._credentials = credentials
        self._v2_rest_url = self._credentials[_IAMConstants.V2_REST_URL]
        self._jobs_url = None

    def _get_jobs_url(self):
        """Get & save jobs URL from the status call."""
        if self._jobs_url is None:
            self.get_instance_status()
            if self._jobs_url is None:
                raise ValueError("Cannot obtain jobs URL")
        return self._jobs_url

    def _upload_bundle(self, instance, bundle):
        return _FileBundle(self, instance, bundle, {'self':None}, self.rest_client)

    def _submit_bundle(self, bundle, job_config):
        sr = self._submit_job(bundle._bundle_path, job_config)
        return sr['id']

    def _cancel_job(self, job, force):
        try:
            self.cancel_job(job_id=job.id)
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise

    def _submit_job(self, bundle, job_config):
        sab_name = os.path.basename(bundle)

        job_options = job_config.as_overlays() if job_config else {}

        with open(bundle, 'rb') as bundle_fp:
            files = [
                ('bundle_file', (sab_name, bundle_fp, 'application/octet-stream')),
                ('job_options', ('job_options', json.dumps(job_options), 'application/json'))
                ]
            res = self.rest_client.session.post(self._get_jobs_url(),
                headers = {'Accept' : 'application/json'},
                files=files,
                timeout=None)
            _handle_http_errors(res)
            return res.json()


    def cancel_job(self, job_id=None, job_name=None):
        if job_id is None and job_name is None:
            raise ValueError("Please specify either the job id or job name when cancelling a job.")
        
        if job_id is None:
            # Get the job id using the job name, since it's required by the REST API
            res = self.rest_client.make_request(self.get_jobs_url())
            _handle_http_errors(res)
            for job in res['resources']:
                if job['name'] == job_name:
                    # Find the correct job_id, set it
                    job_id = job['id']

        # Cancel the job using the job id
        cancel_url = self._get_jobs_url() + '/' + str(job_id)
        headers = { 'Accept' : 'application/json'}
        res = self.rest_client.session.delete(cancel_url, headers=headers, timeout=self.rest_client.timeout_for('DELETE'))
        _handle_http_errors(res)
        return res.json()

    def start_instance(self):
        res = self.rest_client.session.patch(self._v2_rest_url, json={'state' : 'STARTED'},
                                             headers={
                                                 'Content-Type' : 'application/json',
                                                 'Accept' : 'application/json'},
                                             timeout=None)
        _handle_http_errors(res)
        return res.json()

    def stop_instance(self):
        res = self.rest_client.session.patch(self._v2_rest_url, json={'state' : 'STOPPED'},
                                             headers = { 'Content-Type' : 'application/json',
                                                         'Accept' : 'application/json'},
                                             timeout=None)
        _handle_http_errors(res)
        return res.json()

    def get_instance_status(self):
        resp = self.rest_client.make_request(self._v2_rest_url)
        # Since we are here, see if we can save the jobs url
        if self._jobs_url is None and 'jobs' in resp:
            self._jobs_url = resp['jobs']
        return resp


class _StreamingAnalyticsServiceV1Delegator(object):
    """Delegator for pre-IAM access to a Streaming Analytics service
    """
    def __init__(self, rest_client, credentials):
        """
        Args:
            rest_client (_StreamsRestClient): The client used to make the REST call.
            credentials (str): credentials for accessing Streaming Analytics service.
        """
        self.rest_client = rest_client
        self._credentials = credentials

    def _get_url(self, req_name):
        return self._credentials['rest_url'] + self._credentials[req_name]

    def _upload_bundle(self, instance, bundle):
        return _FileBundle(self, instance, bundle, {'self':None}, self.rest_client)

    def _submit_bundle(self, bundle, job_config):
        sr = self._submit_job(bundle._bundle_path, job_config)
        return sr['id']

    def _cancel_job(self, job, force):
        return self.cancel_job(job_id=job.id)

    def _submit_job(self, bundle, job_config):
        sab_name = os.path.basename(bundle)

        url = self._get_url('jobs_path')
        params = {'bundle_id': sab_name}
        job_options = {}
        if job_config is not None:
            job_config._add_overlays(job_options)

        with open(bundle, 'rb') as bundle_fp:
            files = [
                ('bundle_file', (sab_name, bundle_fp, 'application/octet-stream')),
                ('job_options', ('job_options', json.dumps(job_options), 'application/json'))
                ]
            return self.rest_client.session.post(url=url, params=params, files=files, timeout=None).json()

    def cancel_job(self, job_id=None, job_name=None):
        """Cancel a running job.

        Args:
            job_id (str, optional): Identifier of job to be canceled.
            job_name (str, optional): Name of job to be canceled.

        Returns:
            dict: JSON response for the job cancel operation.
        """
        payload = {}
        if job_name is not None:
            payload['job_name'] = job_name
        if job_id is not None:
            payload['job_id'] = job_id

        jobs_url = self._get_url('jobs_path')
        res = self.rest_client.session.delete(jobs_url, params=payload, timeout=self.rest_client.timeout_for('DELETE'))
        _handle_http_errors(res)
        return res.json()

    def start_instance(self):
        """Start the instance for this Streaming Analytics service.

        Returns:
            dict: JSON response for the instance start operation.
        """
        start_url = self._get_url('start_path')
        res = self.rest_client.session.put(start_url, json={}, timeout=None)
        _handle_http_errors(res)
        return res.json()

    def stop_instance(self):
        """Stop the instance for this Streaming Analytics service.

        Returns:
            dict: JSON response for the instance stop operation.
        """
        stop_url = self._get_url('stop_path')
        res = self.rest_client.session.put(stop_url, json={}, timeout=None)
        _handle_http_errors(res)
        return res.json()

    def get_instance_status(self):
        """Get the status the instance for this Streaming Analytics service.

        Returns:
            dict: JSON response for the instance status operation.
        """
        status_url = self._get_url('status_path')
        res = self.rest_client.session.get(status_url)
        _handle_http_errors(res)
        return res.json()

class _IAMConstants(object):
    V2_REST_URL = 'v2_rest_url'
    """The credentials key for the REST url of the Streaming Analytics service
    """

    SERVICE_ID = 'iam_serviceid_crn'
    """Service identifier"""

    API_KEY = 'apikey'
    """The credentials key for the api key which can be used to retrieve bearer authentication
    tokens for REST requests using IAM.
    """

    EXPIRATION = 'expiration'
    """The key used to retrieve the expiration time of the bearer authentication token from
    the IAM token response.
    """

    ACCESS_TOKEN = 'access_token'
    """The key of the bearer authentication token in the IAM token response.
    """

    GRANT_PARAM = 'grant_type'
    GRANT_TYPE = 'urn:ibm:params:oauth:grant-type:apikey'

    TOKEN_URL = 'https://iam.cloud.ibm.com/oidc/token'
    """The url from which to receive bearer authentication tokens for Authorizing REST requests on production IBM Cloud.
    """

    TOKEN_URL_TEST = 'https://iam.test.cloud.ibm.com/oidc/token'
    """The url from which to receive bearer authentication tokens for Authorizing REST requests on test/staging IBM Cloud.
    """

class ApplicationBundle(_ResourceElement):
    """Application bundle tied to an instance.

    .. versionadded:: 1.11
    """
    def __init__(self, _delegator, instance, json_rep, rest_client):
        super(ApplicationBundle, self).__init__(json_rep, rest_client)
        self._instance = instance
        self._delegator = _delegator

    def submit_job(self, job_config=None):
        """Submit this Streams Application Bundle (sab file) to
        its associated instance.
        
        Args:
            job_config(JobConfig): a job configuration overlay
        
        Returns:
            Job: Resulting job instance.
        """
        job_id = self._delegator._submit_bundle(self, job_config)
        return self._instance.get_job(job_id)

# An ApplicationBundle for cases when we cannot upload the
# sab file (e.g. Streaming Analytics, Streams 4.2/4.3
class _FileBundle(ApplicationBundle):
    def __init__(self, _delegator, instance, bundle, json_rep, rest_client):
        super(_FileBundle, self).__init__(_delegator, instance, json_rep, rest_client)
        self._bundle_path = os.path.abspath(bundle)

# As of 1.11 several methods are always driven through delegators
# to allow the same API vary the underlying implementation.
# A delegator has at least these methods:
#
# _upload_bundle - Uploads a bundle to the instance, or if that's
#                  not supported return an ApplicationBundle that
#                  represents the local file
#
# _submit_bundle - Submit an ApplicationBundle as a running job
#
# _cancel_job - Cancel a running job

def _streams_delegator(sc):
    root_resources = sc.rest_client.make_request(sc.resource_url)
    has_domains = False
    for resource in root_resources['resources']:
        if resource['name'] == 'domains':
            has_domains = True
            break
  
    if has_domains:
        return _StreamsV4Delegator(sc.rest_client)
    return _StreamsRestDelegator(sc.rest_client)

class _StreamsV4Delegator(object):
    """Delegator for a IBM Streams 4.2/4.3 instance.
    """
    def __init__(self, rest_client):
        self.rest_client = rest_client

    def _upload_bundle(self, instance, bundle):
        return _FileBundle(self, instance, bundle, {'self':None}, self.rest_client)

    def _submit_bundle(self, bundle, job_config):
        return streamsx.st._submit_bundle(bundle._bundle_path, job_config,
            domain_id=bundle._instance.get_domain().id, instance_id=bundle._instance.id)

    def _cancel_job(self, job, force):
        """Cancel job using streamtool."""
        import streamsx.st as st
        if st._has_local_install:
            return st._cancel_job(job.id, force,
                domain_id=job.get_instance().get_domain().id, instance_id=job.get_instance().id)
        return False


class _UploadedBundle(ApplicationBundle):
    def _app_id(self):
        return self.bundleId

class ApplicationConfiguration(_ResourceElement):
    """An application configuration.
   
    Application configurations are used for secure storage and
    retrieval of name/value pairs.

    An application configuration maintains a set of properties
    that an application can access at runtime. These are typically
    used to maintain connection endpoint and credentials for sources
    and sinks.

    Attributes:
        name (str): Name of the configuration.
        description (str): Description for the configuration.
        properties (dict): Property values stored for the configuration.
        creationTime (long): Epoch time when this configuraiton was created.
        lastModifiedTime (long): Epoch time when this configuration was last modified.

    .. versionadded 1.12
    """
    @staticmethod
    def _props(name=None, properties=None, description=None):
        cv = {}
        if name:
            cv['name'] = str(name)
        if description:
            cv['description'] = str(description)
        if properties is not None:
            acp = {}
            for k,v in properties.items():
                acp[str(k)] = None if v is None else str(v)
            cv['properties'] = acp
        return cv

    def update(self, properties=None, description=None):
        """Update this application configuration.

        To create or update a property provide its key-value
        pair in `properties`.

        To delete a property provide its key with the value ``None``
        in properties.

        Args:
            properties (dict): Property values to be updated. If ``None`` the properties are unchanged.
            description (str): Description for the configuration. If ``None`` the description is unchanged.

        Returns:
            ApplicationConfiguration: self
        """
        cv = ApplicationConfiguration._props(properties=properties, description=description)
        res = self.rest_client.session.patch(self.rest_self,
                                             headers = {'Accept' : 'application/json',
                                                        'Content-Type' : 'application/json'},
                                             json=cv,
                                             timeout=self.rest_client.timeout_for('PATCH'))
        _handle_http_errors(res)
        self.json_rep = res.json()
        return self

    def delete(self):
        """Delete this application configuration.
        """
        res = self.rest_client.session.delete(self.rest_self, timeout=self.rest_client.timeout_for('DELETE'))
        _handle_http_errors(res)


class _StreamsRestDelegator(object):
    """Delegator for IBM Streams instances where the
       Streams REST API provides actions.
    """
    def __init__(self, rest_client):
        self.rest_client = rest_client

    def _upload_bundle(self, instance, bundle):
        self.rest_client._block_ssl_warn()
        app_bundle_url = instance.self + '/applicationbundles'

        sab_name = os.path.basename(bundle)
        with open(bundle, 'rb') as bundle_fp:
            res = self.rest_client.session.post(app_bundle_url,
                headers = {'Accept' : 'application/json', 'Content-Type': 'application/x-jar'},
                data=bundle_fp,
                verify=self.rest_client.session.verify,
                timeout=None)
            _handle_http_errors(res)
            if res.status_code != 200:
                raise ValueError(str(res))
            return _UploadedBundle(self, instance, res.json(), self.rest_client)

    def _submit_bundle(self, bundle, job_config):
        self.rest_client._block_ssl_warn()
        job_options = job_config.as_overlays() if job_config else {}
        app_id = bundle._app_id()
        res = self.rest_client.session.post(bundle._instance.jobs,
            headers = {'Accept' : 'application/json'},
            json={'application': app_id, 'jobConfigurationOverlay':job_options, 'preview':False},
            verify=self.rest_client.session.verify,
            timeout=None)
        _handle_http_errors(res)
        if res.status_code != 201:
            raise ValueError(str(res))
        return res.json()['id']

    def _cancel_job(self, job, force):
        self.rest_client._block_ssl_warn()
        cancel_url = job.instance + '/jobs/' + job.id
        res = self.rest_client.session.delete(cancel_url,
                headers = {'Accept' : 'application/json'},
                verify=self.rest_client.session.verify,
                timeout=self.rest_client.timeout_for('DELETE'))

        if res.status_code == 204:
            return True
        if res.status_code == 404:
            return False

        res.raise_for_status()

        return False

    def _update_operators(self, job, job_config):
        self.rest_client._block_ssl_warn()

        job_options = job_config.as_overlays()

        # Update the job operators using the job id
        update_url = job.instance + '/jobs/' + job.id

        # --- 1/13/20 'preview':False is ignored until 1Q20 fix ---
        res = self.rest_client.session.patch(update_url,
                headers = {'Accept' : 'application/json'},
                json = {'jobConfigurationOverlay':job_options},
                verify=self.rest_client.session.verify,
                timeout=self.rest_client.timeout_for('PATCH'))
        _handle_http_errors(res)
        if res.status_code != 200:
            raise ValueError(str(res))
        return 0
        # return res.json()['results']

class Toolkit(_ResourceElement):
    """IBM Streams toolkit.

    Attributes:
        id(str): Unique ID for this instance.
        resourceType(str): Identifies the REST resource type, which is *toolkit*. 
        name(str): The name of the toolkit.
        version(str): The version of the toolkit.
        requiredProductVersion(str): The earliest version of Streams required by the toolkit.
        path(str): The full path to the toolkit.

    Example:
        >>> from streamsx.build import BuildService
        >>> build_service = BuildService.of_endpoint()
        >>> toolkits = build_service.get_toolkits()
        >>> print (toolkits[0].resourceType)
        toolkit

        .. versionadded:: 1.13
    """
    def __init__(self, json_rep, rest_client):
        super(Toolkit, self).__init__(json_rep, rest_client)

    def get_index(self):
        res = self.rest_client.make_raw_streaming_request(self.index, 'text/xml')
        _handle_http_errors(res)
        return res.text        

    def delete(self):
        res = self.rest_client.session.delete(self.self, 
                headers = {'Accept' : 'application/json'},
                verify=self.rest_client.session.verify,
                timeout=self.rest_client.timeout_for('DELETE'))

        # 204 is success
        if res.status_code == 204:
            return True
        if res.status_code == 404:
            # not found
            return False

        res.raise_for_status()

        return False
    
    @staticmethod
    def _toolkits_url(sc):
        toolkits_url = None
        for resource in sc.get_resources():
            if resource.name == 'toolkits':
                toolkits_url = resource.resource
                break;
            if resource.name == 'buildPools':
                buildpools_json = sc.rest_client.make_request(resource.resource)['buildPools']
                for pool in [_BuildPool(json_rep, sc.rest_client) for json_rep in buildpools_json]:
                    if pool.type == 'application' and hasattr(pool, 'toolkits'):
                        toolkits_url = pool.toolkits
                        break;
        if toolkits_url:
            return toolkits_url
        raise ValueError('The toolkits REST API is not supported by the Streams instance')

    class Dependency:
        """
        The name, and range of versions, of a toolkit required by another
        toolkit.
        
        Attributes:
            name(str): the name of the required toolkit
            version(str): the range of versions required of the toolkit
        """
        def __init__(self, name, version):
            self.name = name
            self.version = version

        def __str__(self):
            return self.name  + ' ' + self.version
           
    @property
    def dependencies(self):
        """
        Find all the dependencies for this toolkit.
        
        Returns:
            list(Dependency):  List of dependencies of this toolkit.  If this
            toolkit does not have any dependencies, this will be an empty list.
        """
        deps = []
        index = self.get_index()
        root = ElementTree.fromstring(index)
        toolkit_element = root.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkit}toolkit')
        dependency_elements = toolkit_element.findall('{http://www.ibm.com/xmlns/prod/streams/spl/toolkit}dependency')
        for dependency_element in dependency_elements:
            name = dependency_element.find('{http://www.ibm.com/xmlns/prod/streams/spl/common}name').text
            version = dependency_element.find('{http://www.ibm.com/xmlns/prod/streams/spl/common}version').text
            deps.append(Toolkit.Dependency(name, version))
        return deps


class _BuildPool(_ResourceElement):
    """A build pool.
    
    Attributes:
        baseimages(str): optional URL to query the base images
        buildInactivityTimeout(int): 15,
        buildProcessingTimeout": 15,
        buildProcessingTimeoutMaximum": 15,
        buildProductVersion": "",
        buildingCount": 0,
        name(str): The name of the build pool
        resourceType(str): Identifies the REST resource type, which is *buildPool*
        resourceWaitTimeout(int):
        restid(str): The pool identifier
        sizeMaximum(int):
        sizeMinimum(int):
        status(str): 
        type(str): The type of the build pool. The type can be *application*, or *image*.
        waitingCount(int)

    Example:
        >>> from streamsx.build import BuildService
        >>> build_service = BuildService.of_endpoint()
        >>> buildpools = build_service._get_builPools()
        >>> print (buildpools[0].resourceType)
        buildPool

        .. versionadded:: 1.15
    """
    def __init__(self, json_rep, rest_client):
        super(_BuildPool, self).__init__(json_rep, rest_client)

class BaseImage(_ResourceElement):
    """A base image used for an Edge image build using the EDGE context type.
    
    Attributes:
        buildPool(str): REST URL of the build pool that contains the image
        id(str): identifier in the form *registry/prefix/imagename:tag*
        name(str): the image name
        prefix(str): the image prefix
        registry(str): the registry where the image is stored
        resourceType(str): the REST resource type, which is *image*
        restid(str): identifier in the form *registry/prefix/imagename:tag*
        tag(str): the image tag
    
    Example:
        >>> from streamsx.build import BuildService
        >>> build_service = BuildService.of_endpoint()
        >>> baseimages = build_service.get_base_images()
        >>> print(type(baseimages[0]))
        <class 'streamsx.rest_primitives.BaseImage'>
        >>> print (baseimages[0].resourceType)
        image

        .. versionadded:: 1.15
    """
    def __init__(self, json_rep, rest_client):
        super(BaseImage, self).__init__(json_rep, rest_client)
