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
import re
import subprocess

from streamsx.build import BuildService
import xml.etree.ElementTree as ET
from io import StringIO
import streamsx.scripts.streamtool as streamtool


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


# Tests rmtoolkit script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(cpd_setup(), "requires Streams REST API setup")
class Testrmtoolkit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Call shell script to create the info.xml and toolkit.xml for each toolkit, and the info.xml for each app to build
        # Also updates toolkit names so different users can run tests concurrently
        # Ex. 'com.example.test_tk_2' -> 'com.example.tmyuuwkpjittfjla.test_tk_2'
        script = (my_path / "toolkits/create_test_sc_files.sh").resolve()
        subprocess.call([script])

    @classmethod
    def tearDownClass(cls):
        # Call shell script to delete the info.xml and toolkit.xml for each toolkit, and the info.xml for each app to build
        script = (my_path / "toolkits/delete_test_sc_files.sh").resolve()
        subprocess.call([script])

    def setUp(self):
        self.build_server = BuildService.of_endpoint(verify=False)

        if self.build_server.resource_url is None:
            self.skipTest("Build REST API is not available")
        else:
            # delete all the test toolkits from the buildserver, in case they were left behind by a previous test failure.
            file_name = (my_path / "test_sc_old_toolkit_name.txt").resolve()
            old_toolkit_name = None
            if os.path.isfile(file_name):
                with open(file_name) as f:
                    old_toolkit_name = f.readline().strip()
            test_toolkits_deleted = self._delete_test_toolkits(old_toolkit_name)

            # If toolkits were not deleted, fail current test, go to next test (which tries to delete test toolkits again)
            if not test_toolkits_deleted:
                self.fail('Current test toolkits or test toolkits from previous tests not deleted properly.')

            # Get a list of paths for all the test toolkits, each element representing the path to a given test toolkit
            self.test_toolkit_paths = self._get_test_toolkit_paths()

            self.test_toolkit_objects = self._get_toolkit_objects(self.test_toolkit_paths)
            self.test_toolkit_names = [x.name for x in self.test_toolkit_objects]
            # Get and store the randomly generated toolkit name, thus in case of network failure, on next run on test suite,
            # can read old randomly generated toolkit name, and delete toolkits off buildserver
            # Ex. 'com.example.tmyuuwkpjittfjla.test_tk_2' -> 'com.example.tmyuuwkpjittfjla.'
            tk1 = self._get_toolkit_objects([self.test_toolkit_paths[0]])[0]
            self.random_name_variable = re.sub("test_tk_.", "", tk1.name)
            with open((my_path / "test_sc_old_toolkit_name.txt").resolve(), "w") as file1:
                file1.write(self.random_name_variable)

            # Upload these toolkits to the buildserver
            try:
                for toolkit in self.test_toolkit_paths:
                    tk = self.build_server.upload_toolkit(toolkit)
                    self.assertIsNotNone(tk)
            except requests.exceptions.HTTPError as err:
                _handle_http_error(err)

            # Operations such as adding or deleting toolkits sometimes are not
            # effective immediately.  Even though toolkits were deleted
            # successfully, it may take some time for them to be completely
            # removed.  For this reason, we sleep between tests, after deleting
            # all the toolkits known to this test suite.
            time.sleep(5)

    def tearDown(self):
        self._delete_test_toolkits()

    def _run_rmtoolkit(self, args):
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "rmtoolkit")

        rc, return_message = streamtool.run_cmd(args=args)
        # Operations such as adding or deleting toolkits sometimes are not
        # effective immediately. It may take some time for them to be completely
        # removed.  For this reason, we sleep after deleting
        time.sleep(5)
        return rc, return_message

    def _get_test_toolkit_paths(self):
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

    def _delete_test_toolkits(self, old_toolkit_name=None):
        # delete all the test toolkits from the buildserver, in case they were left behind by a previous test failure.

        # 2 cases ..
        # Case 1 - Test in current run of test suite finishes, succesfully or not, need to delete test toolkits
        # Case 2 - Network failure on test in previous run of test suite (with a different randomly generated name), ..
        # thus toolkits with previous random name stuck on buildserver, need to delete

        deleted_all_toolkits = False
        test_toolkit_objects = self._get_toolkit_objects(self._get_test_toolkit_paths())
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

    def _check_toolkit_deleted(self, toolkit_id=None, toolkit_name=None, toolkit_regex=None):
        """ Given a id, or name, or regex, check whether there are test toolkits on the buildserver matching these.
            If yes, -> fail, these were supposed to be deleted
            if not, -> success

        Keyword Arguments:
            toolkit_id {String} -- Id of the single toolkit that should have been deleted (default: {None})
            toolkit_name {String} -- Name of the toolkit that should have been deleted (all versions) (default: {None})
            toolkit_regex {String} -- Uncompiled regex matching the toolkits that should have been deleted (default: {None})
        """

        # Get the new set of remote toolkits after we have deleted one or more test toolkits
        remote_toolkits = self.build_server.get_toolkits()
        remote_test_tk_objects = [x for x in remote_toolkits if x.name in self.test_toolkit_names]

        if toolkit_id:
            # Get the id's of all test toolkits on the buildserver
            remote_test_tk_ids = [x.id for x in remote_test_tk_objects]
            if toolkit_id in remote_test_tk_ids:
                self.fail("Toolkit with id {} is still present on the build server".format(toolkit_id))

        elif toolkit_name:
            # Get the names of all test toolkits on the buildserver
            remote_test_tk_names = [x.name for x in remote_test_tk_objects]
            if toolkit_name in remote_test_tk_names:
                self.fail("Toolkit with name {} is still present on the build server".format(toolkit_name))

        elif toolkit_regex:
            # Get the names of all test toolkits on the buildserver that match the regex
            p = re.compile(toolkit_regex)
            remote_test_tk_names = [x.name for x in remote_test_tk_objects]
            matched_toolkits = [x for x in remote_test_tk_names if p.match(x)]
            if matched_toolkits:
                self.fail("Toolkit matching regex {} is still present on the build server".format(toolkit_regex))

    def _get_remote_test_tk_objects(self):
        # Get the remote toolkit objects of our test toolkits

        # Operations such as adding or deleting toolkits sometimes are not
        # effective immediately. For this reason, need to sleep
        # until the test toolkits are present, or we reach timeout

        # Set a 1 min timeout for the test toolkits to show up on build server
        timeout = time.time() + 60*1

        remote_test_tk_objects = []
        # Ensure that all test toolkits have been posted on the build server
        while len(remote_test_tk_objects) != len(self.test_toolkit_objects):
            time.sleep(10)
            remote_toolkits = self.build_server.get_toolkits()
            remote_test_tk_objects = [x for x in remote_toolkits if x.name in self.test_toolkit_names]

            if (len(remote_test_tk_objects) != len(self.test_toolkit_objects)) and time.time() > timeout:
                self.fail('Test toolkits failed to show up on build server after 1 minute')

        return remote_test_tk_objects

    # Delete a single test toolkit with id
    def test_simple_1(self):
        remote_test_tk_objects = self._get_remote_test_tk_objects()

        # Chose 1 random toolkit to delete
        random_tk_to_delete = random.choice(remote_test_tk_objects)

        # Delete random toolkit by id
        args = ['-i', random_tk_to_delete.id]
        rc, return_message = self._run_rmtoolkit(args)

        # Check random toolkit is deleted
        self._check_toolkit_deleted(toolkit_id=random_tk_to_delete.id)
        self.assertEqual(rc, 0)

    # Delete all test toolkits with name
    def test_simple_2(self):
        remote_test_tk_objects = self._get_remote_test_tk_objects()

        # Chose 1 random toolkit to delete
        random_tk_to_delete = random.choice(remote_test_tk_objects)

        # Delete random toolkit by name
        args = ['-n', random_tk_to_delete.name]
        rc, return_message = self._run_rmtoolkit(args)

        # Check random toolkit is deleted
        self._check_toolkit_deleted(toolkit_name=random_tk_to_delete.name)
        self.assertEqual(rc, 0)

    # Delete all test toolkits by regex pattern
    def test_simple_3(self):
        remote_test_tk_names = [x.name for x in self._get_remote_test_tk_objects()]

        # Regex pattern to delete toolkits by
        pattern = self.random_name_variable + 'test_tk_1'
        p = re.compile(pattern)

        # Check at least 1 remote test toolkit object matches our pattern, so we can check later that it is deleted
        matched_toolkits = [x for x in remote_test_tk_names if p.match(x)]
        assert len(matched_toolkits) > 0

        # Delete random toolkit by pattern
        args = ['-r', pattern]
        rc, return_message = self._run_rmtoolkit(args)

        # Check random toolkit is deleted
        self._check_toolkit_deleted(toolkit_regex=pattern)
        self.assertEqual(rc, 0)
