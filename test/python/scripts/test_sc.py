# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import unittest.mock
import sys
import os
import time
import requests
import random
from glob import glob

from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
import streamsx.scripts.sc as sc
from streamsx.build import BuildService
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from io import StringIO
import zipfile

# Tests SC script.
# Requires environment setup for a ICP4D Streams instance.
# @unittest.skipUnless(
#     "CP4D_URL" in os.environ
#     and "STREAMS_INSTANCE_ID" in os.environ
#     and "STREAMS_USERNAME" in os.environ
#     and "STREAMS_PASSWORD" in os.environ,
#     "requires Streams REST API setup",
# )

# REST API failures raise HTTPError instance, which, when printed, show
# the default error message for the status code.  We often have useful
# messages in the response, but those are hidden from the user.  We ought
# to consider making it easier for the user to see the specific error message
# from the REST API call.
# Handle HTTPError, and show the detailed error message contained
# in the response.
def _handle_http_error(err):
    try:
        response = err.response
        text_json = json.loads(response.text)
        messages = text_json["messages"]
        for message in messages:
            print(message["message"])
            logger.error(message["message"])
    except:
        pass
    raise err


class TestSC(unittest.TestCase):
    def setUp(self):
        self.build_server = BuildService.of_endpoint(verify=False)

        if self.build_server.resource_url is None:
            self.skipTest("Build REST API is not available")
        else:
            # Delete all toolkits in case they were left over from previous test
            self.delete_test_toolkits()

            # Get a list of all toolkit_paths, each element representing 1 toolkit_path
            toolkit_paths = self.get_local_toolkit_paths()

            # Randomly shuffle toolkits
            random.shuffle(toolkit_paths)

            # Take half of the toolkits and place them on the buildserver
            self.uploaded_toolkits_paths = toolkit_paths[: len(toolkit_paths) // 2]
            self.post_toolkits(self.uploaded_toolkits_paths)

            # Other half is local toolkits, format it to a string by seperating paths w/ a ':' for use in SC command
            self.local_toolkits_paths = toolkit_paths
            self.local_toolkit_paths_string = ""
            for tk_path in self.local_toolkits_paths:
                self.local_toolkit_paths_string += tk_path + ":"

            # Name of SPL app to build
            self.main_composite = "samplemain::main"

            # Operations such as adding or deleting toolkits sometimes are not
            # effective immediately.  Even though toolkits were deleted
            # successfully, it may take some time for them to be completely
            # removed.  For this reason, we sleep between tests, after deleting
            # all the toolkits known to this test suite.
            time.sleep(5)

    def tearDown(self):
        self.delete_test_toolkits()

    def get_local_toolkit_paths(self):
        # Get a list of all toolkit_paths, each element representing 1 toolkit_path
        toolkit_paths = []
        os.chdir(
            "/home/streamsadmin/hostdir/streamsx.topology/test/python/scripts/toolkits"
        )
        cwd = os.getcwd()
        toolkits = glob("*/")
        for tk in toolkits:
            toolkit_paths.append(cwd + "/" + tk)
        return toolkit_paths

    def _run_sc(self, spl_app_to_build, toolkits_path=None):
        args = ["--disable-ssl-verify", "-M", spl_app_to_build]
        if toolkits_path:
            args.insert(3, "-t")
            args.insert(4, toolkits_path)
        return sc.main(args=args)

    def delete_test_toolkits(self):
        # delete the test toolkits, in case they have been left behind by a test failure.
        uploaded_toolkit_objects = self._get_toolkit_objects(
            self.get_local_toolkit_paths()
        )
        toolkit_names = [tk.name for tk in uploaded_toolkit_objects]

        remote_toolkits = self.build_server.get_toolkits()
        for toolkit in remote_toolkits:
            if toolkit.name in toolkit_names:
                self.assertTrue(toolkit.delete())

    def post_toolkits(self, toolkit_paths):
        try:
            for toolkit in toolkit_paths:
                tk = self.build_server.upload_toolkit(toolkit)
                self.assertIsNotNone(tk)
        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)

    def check_sab_correct_dependencies(self, sab_path, required_dependencies):
        # Read the dependencies/toolkits used to build the sab
        sab_toolkits = self._parse_dependencies(sab_path)
        for tk in required_dependencies:
            if tk in sab_toolkits:
                continue
            else:
                self.fail(
                    "Dependency/toolkit {} with version {} not in sab".format(
                        tk.name, tk.version
                    )
                )

    def test_1(self):
        # tk_1 should be version 3.0.0, tk_3 should have version 2.0.0
        os.chdir(
            "/home/streamsadmin/hostdir/streamsx.topology/test/python/scripts/apps/com.example.test_app_1/"
        )
        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        # Check sab has correct dependencies
        sab_path = self.get_sab_path(self.main_composite)

        if not os.path.isfile(sab_path):
            self.fail("Sab does not exist")

        required_dependencies = []
        req1 = self._LocalToolkit("test_tk_1", "3.0.0", None)
        req2 = self._LocalToolkit("test_tk_3", "2.0.0", None)
        required_dependencies.extend([req1, req2])

        self.check_sab_correct_dependencies(sab_path, required_dependencies)

    # def test_2(self):
    #     # tk_1 should be version 3.0.0, tk_3 should have version 2.0.0
    #     os.chdir('/home/streamsadmin/hostdir/streamsx.topology/test/python/scripts/apps/com.example.test_app_2/')
    #     self._run_sc(self.main_composite, self.local_toolkit_paths_string)

    #     # Check sab has correct dependencies
    #     sab_path = self.get_sab_path(self.main_composite)

    #     if not os.path.isfile(sab_path):
    #         self.fail("Sab does not exist")

    #     self.check_sab_correct_dependencies(sab_path, None)

    def get_sab_path(self, path):
        return path.replace(":", ".", 1).replace(":", "") + ".sab"

    def _get_toolkit_objects(self, toolkit_paths):
        toolkit_objects = []
        for tk_path in toolkit_paths:
            if os.path.isfile(tk_path + "info.xml"):
                # Get the name & version of the toolkit that is in the directory tk_path
                root = ET.parse(tk_path + "info.xml").getroot()
                identity = root.find(
                    "{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}identity"
                )
                name = identity.find(
                    "{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}name"
                )
                version = identity.find(
                    "{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}version"
                )

                toolkit_name = name.text
                toolkit_version = version.text

                tk = self._LocalToolkit(toolkit_name, toolkit_version, tk_path)
                toolkit_objects.append(tk)
        return toolkit_objects

    class _LocalToolkit:
        def __init__(self, name, version, path):
            self.name = name
            self.version = version
            self.path = path

        def __eq__(self, other):
            return self.name == other.name and self.version == other.version

    def _parse_dependencies(self, sab_path):
        deps = []
        # Parse bundleInfo.xml for the dependencies of the app you want to build sab file for
        zip = zipfile.ZipFile(sab_path)
        content = None
        with zip.open("bundleInfo.xml") as f:
            content = f.read()

        if not content:
            self.fail("Error in reading bundle info")

        root = ET.fromstring(content)
        dependency_elements = root.findall(
            "{http://www.ibm.com/xmlns/prod/streams/spl/bundleInfoModel}toolkit"
        )
        for dependency_element in dependency_elements:
            name = dependency_element.get("name")
            version = dependency_element.get("version")
            tk = self._LocalToolkit(name, version, None)
            deps.append(tk)
        return deps
