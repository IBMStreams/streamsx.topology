import json
import os
import re
import logging
import tempfile
import shutil

from zipfile import ZipFile

from .rest_primitives import (RestResource,_exact_resource,_StreamsRestClient, Toolkit,_handle_http_errors)

logger = logging.getLogger('streamsx.rest')

class BuildConnection:
    def __init__(self, username=None, password=None, build_url=None, auth=None):
        if auth:
            pass
        elif username and password:
            pass
        else:
            raise ValueError("Must supply either a IBM Cloud VCAP Services or a username, password"
                             " to the BuildConnection constructor.")

        if not build_url and 'STREAMS_BUILD_URL' in os.environ:
            build_url = os.environ['STREAMS_BUILD_URL']

        self._resource_url = re.sub('/builds$','/resources',build_url)

        if auth:
            self.rest_client = _StreamsRestClient(auth)
        else:
            self.rest_client = _StreamsRestClient._of_basic(username, password)


        #self.rest_client._sc = self # ?
        self.session = self.rest_client.session
        
        self._toolkits_url = None

    def _get_elements(self, resource_name, eclass, id=None):
        # TODO: error handling.  The URL might not be correct, in which
        # case we get problems here.
        for resource in self.get_resources():
            if resource.name == resource_name:
                elements = []
                for json_element in resource.get_resource()[resource_name]:
                    if _exact_resource(json_element, id):
                        elements.append(eclass(json_element, self.rest_client))
                return elements

    @property
    def resource_url(self):
        # TODO what is st.get_resource_api?
#        self._build_url = self._build_url or st.get_build_api()
        return self._resource_url

    @property
    def toolkits_url(self):
        if not self._toolkits_url:
            for resource in self.get_resources():
                if resource.name == 'toolkits':
                    self._toolkits_url = resource.resource
                    break;
            else:
                raise ValueError('Toolkits api is not supported by the build host') # TODO better error type/string
        return self._toolkits_url

    def get_toolkits(self):
        return self._get_elements('toolkits', Toolkit)

    def put_toolkit(self, path):
        # TODO
        # This probably should not be here, it should be in a Toolkits class.
        # or maybe a class method in Toolkit

        # TODO
        # Handle path does not exist, is not readable, is not a directory

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

                # reference _submit_job or _upload_bundle
                # This probably should be in a delegator for V5
            
                with open(filename, 'rb') as toolkit_fp:
                    res = self.rest_client.session.post(self.toolkits_url,
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
                    # new toolkit was returned, because the toolkit did not 
                    # replace an existing one.

                    if len(new_toolkits) == 1:
                        return new_toolkits[0]    
                    return None                            


    def get_resources(self):
        """Retrieves a list of all known Streams high-level REST resources.

        Returns:
            :py:obj:`list` of :py:class:`~.rest_primitives.RestResource`: List of all Streams high-level REST resources.
        """
        json_resources = self.rest_client.make_request(self.resource_url)['resources']
        return [RestResource(resource, self.rest_client) for resource in json_resources]


    def __str__(self):
        return pformat(self.__dict__)
        
