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
import json
from pathlib import Path

from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
import streamsx.scripts.sc as sc
from streamsx.build import BuildService
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from io import StringIO
import zipfile

my_path = Path(__file__).parent

def _handle_http_error(err):
    # REST API failures raise HTTPError instance, which, when printed, show
    # the default error message for the status code.  We often have useful
    # messages in the response, but those are hidden from the user.  We ought
    # to consider making it easier for the user to see the specific error message
    # from the REST API call.
    # Handle HTTPError, and show the detailed error message contained
    # in the response.
    try:
        response = err.response
        text_json = json.loads(response.text)
        messages = text_json["messages"]
        for message in messages:
            print(message["message"])
    except:
        pass
    raise err


# Tests SC script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(
    "CP4D_URL" in os.environ
    and "STREAMS_INSTANCE_ID" in os.environ
    and "STREAMS_USERNAME" in os.environ
    and "STREAMS_PASSWORD" in os.environ,
    "requires Streams REST API setup",
)
class TestSC(unittest.TestCase):
    def setUp(self):
        self.build_server = BuildService.of_endpoint(verify=False)

        if self.build_server.resource_url is None:
            self.skipTest("Build REST API is not available")
        else:
            # delete all the test toolkits from the buildserver, in case they were left behind by a previous test failure.
            self.delete_test_toolkits()

            # Get a list of paths for all the test toolkits, each element representing the path to a given test toolkit
            toolkit_paths = self.get_test_toolkit_paths()

            # Randomly shuffle the toolkits
            random.shuffle(toolkit_paths)

            # Take half of the toolkits and place them on the buildserver
            self.uploaded_toolkits_paths = toolkit_paths[: len(toolkit_paths) // 2]
            self.post_test_toolkits(self.uploaded_toolkits_paths)

            # Other half is local toolkits, combine all local toolkit paths into 1 string by seperating paths w/ a ':' to pass into SC command via -t arg
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

    def _run_sc(self, spl_app_to_build, local_toolkits_path=None):
        args = ["--disable-ssl-verify", "-M", spl_app_to_build]
        if local_toolkits_path:
            args.insert(3, "-t")
            args.insert(4, local_toolkits_path)
        return sc.main(args=args)

    def get_test_toolkit_paths(self):
        # Get a list of all the test toolkit paths, each element representing 1 toolkit_path
        toolkit_paths = []
        path = (my_path / "toolkits").resolve()
        os.chdir(
            path
        )
        cwd = os.getcwd()

        # Get all direct subfolders in toolkits folder
        toolkits = glob("*/")

        # For each toolkit in toolkits folder, get the path, add it to list
        for tk in toolkits:
            toolkit_paths.append(cwd + "/" + tk)
        return toolkit_paths

    def delete_test_toolkits(self):
        # delete all the test toolkits from the buildserver, in case they were left behind by a previous test failure.
        test_toolkit_objects = self._get_toolkit_objects(self.get_test_toolkit_paths())
        toolkit_names = [tk.name for tk in test_toolkit_objects]

        remote_toolkits = self.build_server.get_toolkits()
        for toolkit in remote_toolkits:
            if toolkit.name in toolkit_names:
                self.assertTrue(toolkit.delete())

    def post_test_toolkits(self, toolkit_paths):
        # Given a list of test toolkit paths, upload these toolkits to the buildserver
        try:
            for toolkit in toolkit_paths:
                tk = self.build_server.upload_toolkit(toolkit)
                self.assertIsNotNone(tk)
        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)

    def check_sab_correct_dependencies(self, sab_file, required_dependencies):
        """ Given the name of a sab file, and a list of required _LocalToolkit objects, check if the required dependencies/toolkits are used in the sab

        Arguments:
            sab_file {String} -- The name of the .sab file created by the tests
            required_dependencies {List} -- List of _LocalToolkit objects, each representing a required dependency/toolkit
        """
        # Parse the sab file for the dependencies used to build it
        sab_toolkits = self._parse_dependencies(sab_file)

        # Check if the required dependencies are in the sab
        for tk in required_dependencies:
            if tk in sab_toolkits:
                # Dependency satisfied
                continue
            else:
                # Dependency not satisfied, print the toolkits used, and print which one was not included
                print([(tk.name, tk.version) for tk in sab_toolkits])
                self.fail(
                    "Dependency/toolkit {} with version {} not in sab".format(
                        tk.name, tk.version
                    )
                )

    def get_sab_filename(self, main_composite):
        """ Get the sab name from the path of the main composite

        Arguments:
            main_composite {String} -- The name of the main composite Ex. 'samplemain::main'

        Returns:
            [String] -- The filename of the sab
        """
        # Ex 'samplemain::main' -> 'samplemain.main.sab'
        return main_composite.replace(":", ".", 1).replace(":", "") + ".sab"

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

    def _parse_dependencies(self, sab_file):
        """ Parse the sab_file for the dependencies/toolkits used in it

        Arguments:
            sab_file {.sab} -- A .sab file

        Returns:
            [List] -- A list of _LocalToolkit objects representing the dependencies/toolkits used in the sab
        """
        deps = []
        # Parse bundleInfo.xml for the dependencies of the app you want to build sab file for
        zip = zipfile.ZipFile(sab_file)
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

    def delete_sab(self, sab_file):
        """ Deletes the .sab and the _jobConfig.json file, if it exists, after a test finishes

        Arguments:
            sab_file {String} -- The filename of the sab
        """
        if os.path.isfile(sab_file):
            os.remove(sab_file)

        jsonFile = sab_file.replace(".sab", "_jobConfig.json")

        if os.path.isfile(jsonFile):
            os.remove(jsonFile)

    def test_specific_version(self):
        # Test build of sab w/ specific version of toolkit
        # Build test_app_3, requiring toolkit tk_4 w/ version 2.6.3
        # 2 versions of tk_4 available, v1.0.0, and v2.6.3 , chosen version should be 2.6.3
        path = (my_path / "apps/com.example.test_app_3/").resolve()
        os.chdir(
            path
        )

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        sab_file = self.get_sab_filename(self.main_composite)

        if not os.path.isfile(sab_file):
            self.fail("Sab does not exist")

        # Check sab has correct dependencies
        required_dependencies = []
        req1 = self._LocalToolkit("test_tk_4", "2.6.3", None)
        required_dependencies.extend([req1])
        self.check_sab_correct_dependencies(sab_file, required_dependencies)

        self.delete_sab(sab_file)

    def test_inclusive_tk_version_cutoff(self):
        # Test inclusive aspect on both sides of toolkit version
        # Build test_app_4, requiring toolkit tk_1 w/ version [1.0.0,2.0.0), and tk_3 w/ version (2.0.0,4.0.0]
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 1.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 4.0.0
        path = (my_path / "apps/com.example.test_app_4/").resolve()
        os.chdir(
            path
        )

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        sab_file = self.get_sab_filename(self.main_composite)

        if not os.path.isfile(sab_file):
            self.fail("Sab does not exist")

        # Check sab has correct dependencies
        required_dependencies = []
        req1 = self._LocalToolkit("test_tk_1", "1.0.0", None)
        req1 = self._LocalToolkit("test_tk_3", "4.0.0", None)
        required_dependencies.extend([req1])
        self.check_sab_correct_dependencies(sab_file, required_dependencies)

        self.delete_sab(sab_file)

    def test_exclusive_tk_version_cutoff(self):
        # Test exclusive aspect on both sides of toolkit version
        # Build test_app_5, requiring toolkit tk_1 w/ version (1.0.0,2.0.0], and tk_3 w/ version [2.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 2.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        path = (my_path / "apps/com.example.test_app_5/").resolve()
        os.chdir(
            path
        )

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        sab_file = self.get_sab_filename(self.main_composite)

        if not os.path.isfile(sab_file):
            self.fail("Sab does not exist")

        # Check sab has correct dependencies
        required_dependencies = []
        req1 = self._LocalToolkit("test_tk_1", "2.0.0", None)
        req1 = self._LocalToolkit("test_tk_3", "2.0.0", None)
        required_dependencies.extend([req1])
        self.check_sab_correct_dependencies(sab_file, required_dependencies)

        self.delete_sab(sab_file)

    def test_simple_1(self):
        # Build test_app_1, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_3 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        path = (my_path / "apps/com.example.test_app_1/").resolve()
        os.chdir(
            path
        )

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        sab_file = self.get_sab_filename(self.main_composite)

        if not os.path.isfile(sab_file):
            self.fail("Sab does not exist")

        # Check sab has correct dependencies
        required_dependencies = []
        req1 = self._LocalToolkit("test_tk_1", "3.0.0", None)
        req2 = self._LocalToolkit("test_tk_3", "2.0.0", None)
        required_dependencies.extend([req1, req2])
        self.check_sab_correct_dependencies(sab_file, required_dependencies)

        self.delete_sab(sab_file)

    def test_simple_2(self):
        # Test that sab fails to build when a dependency/toolkit version is not met
        # Build test_app_2, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_2 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_2 available, v0.5.0, v0.5.7 and v0.8.0 , No suitable version, thus should error out
        path = (my_path / "apps/com.example.test_app_2/").resolve()
        os.chdir(
            path
        )

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        # Check sab doesn't exist
        sab_file = self.get_sab_filename(self.main_composite)
        if os.path.isfile(sab_file):
            self.fail("Sab should not exist")

    def test_simple_3(self):
        # Build test_app_6, requiring no dependencies
        path = (my_path / "apps/com.example.test_app_6/").resolve()
        os.chdir(
            path
        )
        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        sab_file = self.get_sab_filename(self.main_composite)

        if not os.path.isfile(sab_file):
            self.fail("Sab does not exist")

        self.delete_sab(sab_file)
