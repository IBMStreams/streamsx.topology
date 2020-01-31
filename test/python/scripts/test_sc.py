# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import unittest.mock
import sys
import os
import time
import shutil
import requests
import random
from glob import glob
import json
from pathlib import Path
import re
import subprocess

from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
import streamsx.scripts.sc as sc
from streamsx.build import BuildService
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from io import StringIO
import zipfile

my_path = Path(__file__).parent

def cpd_setup():
    env = os.environ
    return (("CP4D_URL" in env and "STREAMS_INSTANCE_ID" in env) or \
         ('STREAMS_BUILD_URL' in env and 'STREAMS_REST_URL')) \
         and \
         "STREAMS_USERNAME" in os.environ and \
         "STREAMS_PASSWORD" in os.environ

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
@unittest.skipUnless(cpd_setup(), "requires Streams REST API setup")
class TestSC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Call shell script to create the info.xml and toolkit.xml for each toolkit, and the info.xml for each app to build
        # Also updates toolkit names so different users can run tests concurrently
        # Ex. 'com.example.test_tk_2' -> 'com.example.tmyuuwkpjittfjla.test_tk_2'
        script = (my_path / "sc_test_files/create_test_sc_files.sh").resolve()
        subprocess.call([script])

    @classmethod
    def tearDownClass(cls):
        # Call shell script to delete the info.xml and toolkit.xml for each toolkit, and the info.xml for each app to build
        script = (my_path / "sc_test_files/delete_test_sc_files.sh").resolve()
        subprocess.call([script])

    def setUp(self):
        self.build_server = BuildService.of_endpoint(verify=False)

        if self.build_server.resource_url is None:
            self.skipTest("Build REST API is not available")
        else:
            # delete all the test toolkits from the buildserver, in case they were left behind by a previous test failure.
            file_name = (my_path / "sc_test_files/test_sc_old_toolkit_name.txt").resolve()
            old_toolkit_name = None
            if os.path.isfile(file_name):
                with open(file_name) as f:
                    old_toolkit_name = f.readline().strip()
            test_toolkits_deleted = self.delete_test_toolkits(old_toolkit_name)

            # If toolkits were not deleted, fail current test, go to next test (which tries to delete test toolkits again)
            if not test_toolkits_deleted:
                self.fail('Current test toolkits or test toolkits from previous tests not deleted properly.')

            # Get a list of paths for all the test toolkits, each element representing the path to a given test toolkit
            self.test_toolkit_paths = self.get_test_toolkit_paths()

            # Get the random toolkit name, so we can use it to check correct dependencies
            # Ex. 'com.example.tmyuuwkpjittfjla.test_tk_2' -> 'com.example.tmyuuwkpjittfjla.'
            tk1 = self._get_toolkit_objects([self.test_toolkit_paths[0]])[0]
            self.random_name_variable = re.sub("test_tk_.", "", tk1.name)

            # Store the randomly generated toolkit name, thus in case of network failure, on next run on test suite,
            # can read old randomly generated toolkit name, and delete toolkits off buildserver
            with open((my_path / "sc_test_files/test_sc_old_toolkit_name.txt").resolve(), "w") as file1:
                file1.write(self.random_name_variable)

            # Randomly shuffle the toolkits
            random.shuffle(self.test_toolkit_paths)

            # Take half of the toolkits and place them on the buildserver
            self.uploaded_toolkits_paths = self.test_toolkit_paths[: len(self.test_toolkit_paths) // 2]
            self.post_toolkits(self.uploaded_toolkits_paths)

            # Other half is local toolkits, combine all local toolkit paths into 1 string by seperating paths w/ a ':' to pass into SC command via -t arg
            self.local_toolkits_paths = self.test_toolkit_paths[len(self.test_toolkit_paths) // 2 :]
            self.local_toolkit_paths_string = ""
            for tk_path in self.local_toolkits_paths:
                # Prevents adding an extra ':' at the very end of the string
                if self.local_toolkit_paths_string:
                    self.local_toolkit_paths_string += ":"
                self.local_toolkit_paths_string += tk_path

            # Name of SPL app to build
            self.main_composite = "samplemain::main"
            self.output_directory = 'output'
            self.sab_file = "samplemain.main.sab"

            # Operations such as adding or deleting toolkits sometimes are not
            # effective immediately.  Even though toolkits were deleted
            # successfully, it may take some time for them to be completely
            # removed.  For this reason, we sleep between tests, after deleting
            # all the toolkits known to this test suite.
            time.sleep(5)

    def tearDown(self):
        self.delete_test_toolkits()

        # Delete the .sab and the _jobConfig.json file, if it exists, after a test finishes
        sab_file = self._sab_path()
        if os.path.isfile(sab_file):
            os.remove(sab_file)
        jsonFile = sab_file.replace(".sab", "_jobConfig.json")
        if os.path.isfile(jsonFile):
            os.remove(jsonFile)
        if os.path.isdir(self.output_directory):
            # Should be empty at this point
            shutil.rmtree(self.output_directory)

    def _sab_path(self):
        return os.path.join(self.output_directory, self.sab_file)

    def _check_sab(self):
        if not os.path.isfile(self._sab_path()):
            self.fail("Sab does not exist")

    def _check_no_sab(self):
        if os.path.isfile(self._sab_path()):
            self.fail("Sab should not exist")

    def _run_sc(self, spl_app_to_build, local_toolkits_path=None, output_directory=None, compile_time_arguments=None):
        args = ["--disable-ssl-verify", "-M", spl_app_to_build]
        if local_toolkits_path:
            args.extend(['-t', local_toolkits_path])
        if output_directory:
            args.extend(['--output-directory', output_directory])
            self.output_directory = output_directory
        if compile_time_arguments:
            args.extend(compile_time_arguments)
        return sc.main(args=args)

    def get_test_toolkit_paths(self):
        # Get a list of all the test toolkit paths, each element representing 1 toolkit_path
        toolkit_paths = []
        path = (my_path / "toolkits").resolve()

        # Get all direct subfolders in toolkits folder
        toolkits = path.glob("*/")

        # For each toolkit in toolkits folder, get the path, add it to list
        for tk in toolkits:
            if os.path.isdir(tk):
                toolkit_paths.append(str(tk))
        return toolkit_paths

    def delete_test_toolkits(self, old_toolkit_name=None):
        # delete all the test toolkits from the buildserver, in case they were left behind by a previous test failure.

        # 2 cases ..
        # Case 1 - Test in current run of test suite finishes, succesfully or not, need to delete test toolkits
        # Case 2 - Network failure on test in previous run of test suite (with a different randomly generated name), ..
        # thus toolkits with previous random name stuck on buildserver, need to delete

        deleted_all_toolkits = False
        test_toolkit_objects = self._get_toolkit_objects(self.get_test_toolkit_paths())
        toolkit_names = [tk.name for tk in test_toolkit_objects]
        while not deleted_all_toolkits:
            # Assume no test toolkits are on buildsever, check to make sure
            toolkit_still_on_buildserver = False
            remote_toolkits = self.build_server.get_toolkits()
            for toolkit in remote_toolkits:
                # Case 2
                if old_toolkit_name:
                    if old_toolkit_name in toolkit.name:
                        # test toolkit found to still on buildserver, delete
                        self.assertTrue(toolkit.delete())
                        toolkit_still_on_buildserver = True
                # Case 1
                if toolkit.name in toolkit_names:
                    # test toolkit found to still on buildserver, delete
                    self.assertTrue(toolkit.delete())
                    toolkit_still_on_buildserver = True

            # if a test toolkit is found to still be on buildserver, sleep and retry again
            if toolkit_still_on_buildserver:
                time.sleep(5)
            # No test toolkits located on buildserver, break out of loop
            else:
                deleted_all_toolkits = True
        return deleted_all_toolkits

    def post_toolkits(self, toolkit_paths):
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

    def _get_toolkit_objects(self, toolkit_paths):
        toolkit_objects = []
        for tk_path in toolkit_paths:
            my_file = tk_path + "/toolkit.xml"
            if os.path.isfile(my_file):
                # Get the name & version of the toolkit that is in the directory tk_path
                # Open XML file and strip all namespaces from XML tags
                # Ex. <info:dependencies> -> <dependencies>
                XML_data = None
                with open(my_file, "r") as file:
                    XML_data = file.read()
                it = ET.iterparse(StringIO(XML_data))
                for _, el in it:
                    if "}" in el.tag:
                        el.tag = el.tag.split("}", 1)[1]
                root = it.root
                toolkit_tag = root.find("toolkit")

                toolkit_name = toolkit_tag.get("name")
                toolkit_version = toolkit_tag.get("version")

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

    def test_specific_version(self):
        # Test build of sab w/ specific version of toolkit
        # Build test_app_3, requiring toolkit tk_4 w/ version 2.6.3
        # 2 versions of tk_4 available, v1.0.0, and v2.6.3 , chosen version should be 2.6.3
        path = (my_path / "apps/test_app_3/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        self._check_sab()

        # Check sab has correct dependencies
        tk_name = self.random_name_variable + "test_tk_4"
        req1 = self._LocalToolkit(tk_name, "2.6.3", None)
        required_dependencies = [req1]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)

    def test_inclusive_tk_version_cutoff(self):
        # Test inclusive aspect on both sides of toolkit version
        # Build test_app_4, requiring toolkit tk_1 w/ version [1.0.0,2.0.0), and tk_3 w/ version (2.0.0,4.0.0]
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 1.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 4.0.0
        path = (my_path / "apps/test_app_4/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        self._check_sab()

        # Check sab has correct dependencies
        tk1_name = self.random_name_variable + "test_tk_1"
        tk3_name = self.random_name_variable + "test_tk_3"
        req1 = self._LocalToolkit(tk1_name, "1.0.0", None)
        req2 = self._LocalToolkit(tk3_name, "4.0.0", None)
        required_dependencies = [req1, req2]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)

    def test_exclusive_tk_version_cutoff(self):
        # Test exclusive aspect on both sides of toolkit version
        # Build test_app_5, requiring toolkit tk_1 w/ version (1.0.0,2.0.0], and tk_3 w/ version [2.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 2.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        path = (my_path / "apps/test_app_5/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        self._check_sab()

        # Check sab has correct dependencies
        tk1_name = self.random_name_variable + "test_tk_1"
        tk3_name = self.random_name_variable + "test_tk_3"
        req1 = self._LocalToolkit(tk1_name, "2.0.0", None)
        req2 = self._LocalToolkit(tk3_name, "2.0.0", None)
        required_dependencies = [req1, req2]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)

    def test_simple_1(self):
        # Build test_app_1, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_3 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        path = (my_path / "apps/test_app_1/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        self._check_sab()

        # Check sab has correct dependencies
        tk1_name = self.random_name_variable + "test_tk_1"
        tk3_name = self.random_name_variable + "test_tk_3"
        req1 = self._LocalToolkit(tk1_name, "3.0.0", None)
        req2 = self._LocalToolkit(tk3_name, "2.0.0", None)
        required_dependencies = [req1, req2]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)

    def test_simple_2(self):
        # Test that sab fails to build when a dependency/toolkit version is not met
        # Build test_app_2, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_2 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_2 available, v0.5.0, v0.5.7 and v0.8.0 , No suitable version, thus should error out
        path = (my_path / "apps/test_app_2/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        # Check sab doesn't exist
        self._check_no_sab()

    def test_simple_3(self):
        # Build test_app_6, requiring no dependencies
        path = (my_path / "apps/test_app_6/").resolve()
        os.chdir(path)
        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        self._check_sab()

    def test_simple_4(self):
        # Build test_app_8, (SPLMM app) requiring no dependencies
        path = (my_path / "apps/test_app_8/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string, compile_time_arguments=['3', 'foo=bar'])

        self._check_sab()

    def test_simple_5(self):
        # Build test_app_8, (SPLMM app) requiring no dependencies
        # test_app_8 fails w/o compile time args
        path = (my_path / "apps/test_app_8/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string, compile_time_arguments=['3'])

        # Check sab doesn't exist
        self._check_no_sab()

    def test_simple_6(self):
        # Build test_app_9, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_3 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        path = (my_path / "apps/test_app_9/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string, compile_time_arguments=['3', 'foo=bar'])

        self._check_sab()

        # Check sab has correct dependencies
        tk1_name = self.random_name_variable + "test_tk_1"
        tk3_name = self.random_name_variable + "test_tk_3"
        req1 = self._LocalToolkit(tk1_name, "3.0.0", None)
        req2 = self._LocalToolkit(tk3_name, "2.0.0", None)
        required_dependencies = [req1, req2]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)

    def test_compile_time_args(self):
        # Build test_app_7, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_3 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        # test_app_7 fails w/o compile time args
        path = (my_path / "apps/test_app_7/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string)

        # Check sab doesn't exist
        self._check_no_sab()

    def test_compile_time_args_2(self):
        # Build test_app_7, requiring toolkit tk_1 w/ version [1.0.0,4.0.0), and tk_3 w/ version [1.0.0,4.0.0)
        # 3 versions of tk_1 available, v1.0.0, v2.0.0 and v3.0.0 , chosen version should be 3.0.0
        # 3 versions of tk_3 available, v1.0.0, v2.0.0 and v4.0.0 , chosen version should be 2.0.0
        # test_app_7 succesfully builds given comptile time args
        path = (my_path / "apps/test_app_7/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string, compile_time_arguments=['hello=a,b,c', 'foo=bar'])

        self._check_sab()

        # Check sab has correct dependencies
        tk1_name = self.random_name_variable + "test_tk_1"
        tk3_name = self.random_name_variable + "test_tk_3"
        req1 = self._LocalToolkit(tk1_name, "3.0.0", None)
        req2 = self._LocalToolkit(tk3_name, "2.0.0", None)
        required_dependencies = [req1, req2]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)

    def test_output_directory(self):
        # Test build of sab w/ specific version of toolkit and outputting to a given output directory
        # Build test_app_3, requiring toolkit tk_4 w/ version 2.6.3
        # 2 versions of tk_4 available, v1.0.0, and v2.6.3 , chosen version should be 2.6.3
        path = (my_path / "apps/test_app_3/").resolve()
        os.chdir(path)

        self._run_sc(self.main_composite, self.local_toolkit_paths_string, output_directory='temp')

        self._check_sab()

        # Check sab has correct dependencies
        tk_name = self.random_name_variable + "test_tk_4"
        req1 = self._LocalToolkit(tk_name, "2.6.3", None)
        required_dependencies = [req1]
        self.check_sab_correct_dependencies(self._sab_path(), required_dependencies)
