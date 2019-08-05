# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import unittest.mock
import sys
import os
import time
import requests

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
@unittest.skipUnless(
    "CP4D_URL" in os.environ
    and "STREAMS_INSTANCE_ID" in os.environ
    and "STREAMS_USERNAME" in os.environ
    and "STREAMS_PASSWORD" in os.environ,
    "requires Streams REST API setup",
)
class TestSC(unittest.TestCase):

    # def setUp(self):
    #     self.build_server = BuildService.of_endpoint(verify=False)

    #     if self.build_server.resource_url is None:
    #         self.skipTest("Build REST API is not available")
    #     else:
    #         # self.build_server.session.verify = False
    #         # self.delete_test_toolkits()

    #         # Operations such as adding or deleting toolkits sometimes are not
    #         # effective immediately.  Even though toolkits were deleted
    #         # successfully, it may take some time for them to be completely
    #         # removed.  For this reason, we sleep between tests, after deleting
    #         # all the toolkits known to this test suite.
    #         time.sleep(5)

    # def tearDown(self):
    #     self.delete_test_toolkits()

    def _run_sc(self, spl_app_to_build, toolkits_path=None):
        args = ["--disable-ssl-verify", '-M', spl_app_to_build]
        if toolkits_path:
            args.insert(3, '-t')
            args.insert(4, toolkits_path)
        return sc.main(args=args)

    # delete the test toolkits, in case they have been left behind by
    # a test failure.
    def delete_test_toolkits(self):
        pass
        # toolkit_names = [
        #     type(self).bingo_toolkit_name,
        #     type(self).cards_toolkit_name,
        #     type(self).games_toolkit_name,
        # ]

        # remote_toolkits = self.build_server.get_toolkits()
        # for toolkit in remote_toolkits:
        #     if toolkit.name in toolkit_names:
        #         self.assertTrue(toolkit.delete())

    def post_toolkit(self, toolkit_path):
        pass
        # bingo = self.build_server.upload_toolkit(toolkit_path)
        # self.assertIsNotNone(bingo)
        # self.assertEqual(bingo.name, expected_toolkit_name)
        # self.assertEqual(bingo.version, expected_toolkit_version)
        # self.assertEqual(bingo.requiredProductVersion, '4.2')
        # self.assertEqual(bingo.resourceType, 'toolkit')

    def check_sab_correct_dependencies(self, sab_path, required_dependencies):

        contains_all_correct_dependencies = False

        # Read the dependencies/toolkits used to build the sab
        zip = zipfile.ZipFile(sab_path)
        content = None
        with zip.open("bundleInfo.xml") as f:
            content = f.read()

        if not content:
            self.fail("Error in reading bundle info")
        root = ET.fromstring(content)

        # if sab contains correct dependencies return True, else fail
        if contains_all_correct_dependencies:
            return True
        self.fail("Sab does not contain all the correct dependencies")


    def test_build_transportation_sab(self):
        newPath = '/home/streamsadmin/hostdir/transportation/streamsx.transportation/com.ibm.streamsx.transportation'
        os.chdir(newPath)
        self.stream_install_path = '/opt/ibm/InfoSphere_Streams/4.3.0.0'
        spl_app_to_build = 'com.ibm.streamsx.transportation.nextbus.services::AgencyLocationsService'
        # toolkits_path = self.stream_install_path + '/toolkits:home/streamsadmin/hostdir/streamsx.topology/test/python/scripts/toolkits'
        # self._run_sc(spl_app_to_build, toolkits_path)

        spl_sab = self.get_sab_path(spl_app_to_build)

        if os.path.isfile(spl_sab):
            self.check_sab_correct_dependencies(spl_sab, None)

    def test_1(self):
        # Collect all the toolkits, regardless of version

    def get_sab_path(self, path):
        return path.replace(':', '.', 1).replace(':', '') + '.sab'