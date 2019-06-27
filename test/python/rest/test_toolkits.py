import logging
import time
import unittest
import urllib.parse
import xml.etree.ElementTree as ElementTree

from streamsx.topology.tester import Tester
from streamsx.topology.context import ConfigParams, JobConfig
from streamsx.rest import StreamsConnection
from streamsx.rest_primitives import *


logger = logging.getLogger('streamsx.test.toolkits_test')

def _get_distributed_sc():
    # 4.3 on-prem
    if 'STREAMS_DOMAIN_ID' in os.environ:
        sc = StreamsConnection()
        sc.session.verify = False
        return sc
    return Instance.of_endpoint(verify=False).rest_client._sc

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
        messages = text_json['messages']
        for message in messages:
            print (message['message'])
            logger.error(message['message'])
    except:
        pass
    raise err

# Tests of the toolkit methods provided throught the Build REST API
class TestDistributedRestToolkitAPI(unittest.TestCase):
    # toolkits used in these tests.
    bingo_0_path = './toolkits/bingo_tk0' # version 1.0.0
    bingo_1_path = './toolkits/bingo_tk1' # version 1.0.1
    bingo_2_path = './toolkits/bingo_tk2' # version 1.0.2
    cards_path = './toolkits/cards_tk'
    games_path = './toolkits/games_tk'

    bingo_toolkit_name = 'com.example.bingo'
    cards_toolkit_name = 'com.example.cards'
    games_toolkit_name = 'com.example.games'
    
    bingo_0_version = '1.0.0'
    bingo_1_version = '1.0.1'
    bingo_2_version = '1.0.2'

    @classmethod
    def setUpClass(cls):
        """
        Initialize the logger and get the SWS username, password, and REST URL.
        :return: None
        """
        cls.is_v2 = None
        cls.logger = logger

    def setUp(self):
        Tester.setup_distributed(self)
        self.sc = _get_distributed_sc()
        if self.sc.build_resource_url is None:
            self.skipTest("Build REST API is not available")
        else:

            self.sc.session.verify = False
            self.test_config[ConfigParams.STREAMS_CONNECTION] = self.sc

            self.delete_test_toolkits()

            # Operations such as adding or deleting toolkits sometimes are not
            # effective immediately.  Even though toolkits were deleted 
            # successfully, it may take some time for them to be completely
            # removed.  For this reason, we sleep between tests, after deleting
            # all the toolkits known to this test suite.
            time.sleep(5)


    def tearDown(self):
        self.delete_test_toolkits()

    # Find all toolkits matching a toolkit name.  Optionally a version
    # may also be specified, in which case only a toolkit matching the
    # name and version will be returned.
    def find_matching_toolkits(self, name, version = None):
        toolkits = self.sc.get_toolkits()

        return [toolkit for toolkit in toolkits if (toolkit.name == name and (version is None or toolkit.version == version))]

    # Verify that a toolkit with a given name exists.  If a version is
    # specified, also verify that the version matches.
    def assert_toolkit_exists(self, name, version=None):
        matches = self.find_matching_toolkits(name, version)
        self.assertGreaterEqual (len(matches), 1)

    # Verify that a toolkit with a given name and optional version does not
    # exist.
    def assert_toolkit_not_exists(self, name, version=None):
        matches = self.find_matching_toolkits(name, version)
        self.assertEqual (len(matches), 0)


    # Sometimes toolkits seem not to be listed immediately after they are
    # posted.  This waits for a toolkit to appear in the toolkit list, 
    # retrying a limited number of times until it does.
    def wait_for_toolkit(self, name, version=None, retries=30):
        retry = 0
        while retry < retries:
            matches = self.find_matching_toolkits(name, version)
        
            if matches: 
                break

            else:
                time.sleep(1)
                retry += 1
        else:
            self.fail('Toolkit ' + name + ' not found')


    # delete the test toolkits, in case they have been left behind by 
    # a test failure.
    def delete_test_toolkits(self):
        
        toolkit_names = [type(self).bingo_toolkit_name,
                         type(self).cards_toolkit_name,
                         type(self).games_toolkit_name]

        toolkits = self.sc.get_toolkits()
        for toolkit in toolkits:
            if toolkit.name in toolkit_names:
                self.assertTrue(toolkit.delete())


    # Verify get_toolkits() does not fail.  We don't know what toolkits
    # exist on the test host, so we can't do much more until we push our own
    # test toolkits.  It is probably safe to assume that the standard spl
    # toolkit will always be present, so there will be at least one toolkit.
    def test_get_toolkits(self):
        try:
            toolkits = self.sc.get_toolkits()
            self.assertGreaterEqual(len(toolkits), 1)
            self.assertIsNotNone(toolkits[0].name)
            # for toolkit in toolkits:
            #    print(toolkit.name + ' ' + toolkit.version + ": " + toolkit.id)

        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)


    # Post a test toolkit. Verify that it succeeded and returned a Toolkit
    # object, and its attributes are as expected.  Also get the new list
    # of toolkits and verify that the new toolkit is there.  As the testing
    # might be done in a shared environment, we can't verify that there are
    # no other changes to the list of toolkits.
    def test_post_toolkit(self):

        try:
            toolkit_path = type(self).bingo_1_path
            expected_toolkit_name = type(self).bingo_toolkit_name
            expected_toolkit_version = type(self).bingo_1_version

            bingo = Toolkit.from_local_toolkit(self.sc, toolkit_path)
            self.assertIsNotNone(bingo)
            self.assertEqual(bingo.name, expected_toolkit_name)
            self.assertEqual(bingo.version, expected_toolkit_version)
            self.assertEqual(bingo.requiredProductVersion, '4.2')
            self.assertEqual(bingo.resourceType, 'toolkit')
            
            # We don't know what the values the following attributes will have,
            # but we verify that the expected attributes do at least have values.
            self.assertIsNotNone(bingo.id)
            self.assertIsNotNone(bingo.index)
            self.assertIsNotNone(bingo.path)
            self.assertIsNotNone(bingo.restid)
            self.assertIsNotNone(bingo.self)
            
            # verify that the toolkit is now in the list of all toolkits.
            self.wait_for_toolkit(expected_toolkit_name)
            
            # delete the toolkit and verify that it is no longer in the list
            self.assertTrue(bingo.delete())
            self.assert_toolkit_not_exists(expected_toolkit_name)
            
            # deleting it a second time should fail
            self.assertFalse(bingo.delete())
            
            # post it again, then find it in the list and delete it through the
            # Toolkit object in the list.
            bingo = Toolkit.from_local_toolkit(self.sc, toolkit_path)
            self.assertIsNotNone(bingo)
            self.assertEqual(expected_toolkit_name, bingo.name)
            self.assertEqual(expected_toolkit_version, bingo.version)
            
            # verify that the toolkit is now in the list of all toolkits.
            self.wait_for_toolkit(expected_toolkit_name, expected_toolkit_version)

            self.assertTrue(bingo.delete())

        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)

    # Test posting a toolkit, then getting its index.  Do some sanity
    # checks on the index.
    def test_get_index(self):

        try:
            toolkit_path = type(self).bingo_1_path
            expected_toolkit_name = type(self).bingo_toolkit_name
            expected_toolkit_version = type(self).bingo_1_version

            bingo = Toolkit.from_local_toolkit(self.sc, toolkit_path)
            self.assertIsNotNone(bingo)
            
            index = bingo.get_index()
            self.assertIsNotNone(index)

            self.assertTrue(bingo.delete())

            # The index is xml.  Verify that it can be parsed and is from the 
            # correct toolkit.
            root = ElementTree.fromstring(index)
            toolkit_element = root.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkit}toolkit')
            toolkit_name = toolkit_element.attrib['name']
            toolkit_version = toolkit_element.attrib['version']
            self.assertEqual(expected_toolkit_name, toolkit_name)
            self.assertEqual(expected_toolkit_version, toolkit_version)
            
        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)

    # Test posting different versions of a toolkit.  Posting a version
    # equal to one that is currently deployed should fail,
    # but posting a different version should succeed.
    def test_post_multiple_versions(self):
        try:
            toolkits = self.sc.get_toolkits()

            self.assertNotIn(type(self).bingo_toolkit_name, [toolkit.name for toolkit in toolkits])

            # first post version 1.0.1
            bingo1 = Toolkit.from_local_toolkit(self.sc, type(self).bingo_1_path)
            self.assertIsNotNone(bingo1)
            
            self.wait_for_toolkit(type(self).bingo_toolkit_name, type(self).bingo_1_version)
            
            # post version 1.0.1 again.  It should return None
            self.assertIsNone(Toolkit.from_local_toolkit(self.sc, type(self).bingo_1_path))
            
            # post version 1.0.0.  The version does not match any existing
            # version, so it should get posted.
            bingo0 = Toolkit.from_local_toolkit(self.sc, type(self).bingo_0_path)
            self.assertIsNotNone(bingo0)
            
            # Version 1.0.0 is now in the list, and version 1.0.1 is still there
            self.wait_for_toolkit(type(self).bingo_toolkit_name, type(self).bingo_0_version)
            self.assert_toolkit_exists(type(self).bingo_toolkit_name, type(self).bingo_1_version)
            self.assert_toolkit_not_exists(type(self).bingo_toolkit_name, type(self).bingo_2_version)
            
            # post version 1.0.2.  It does not replace version 1.0.1, but they
            # both continue to exist.
            bingo2 = Toolkit.from_local_toolkit(self.sc, type(self).bingo_2_path)
            self.assertIsNotNone(bingo2)
            
            self.wait_for_toolkit(type(self).bingo_toolkit_name, type(self).bingo_2_version)
            self.assert_toolkit_exists(type(self).bingo_toolkit_name, type(self).bingo_1_version)
            self.assert_toolkit_exists(type(self).bingo_toolkit_name, type(self).bingo_1_version)
            
            
            self.assertTrue (bingo0.delete())
            self.assertTrue (bingo1.delete())
            self.assertTrue (bingo2.delete())

        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)

    # Test getting the dependencies of a toolkit.
    def test_dependencies(self):
        try:
            # Games depends on both cards and bingo.

            bingo = Toolkit.from_local_toolkit(self.sc,type(self).bingo_0_path)
            self.assertIsNotNone(bingo)
            
            cards = Toolkit.from_local_toolkit(self.sc,type(self).cards_path)
            self.assertIsNotNone(cards)
            
            games = Toolkit.from_local_toolkit(self.sc,type(self).games_path)
            self.assertIsNotNone(games)
            
            # bingo and cards have no dependencies
            self.assertEqual(len(bingo.dependencies), 0)
            self.assertEqual(len(cards.dependencies), 0)
            
            games_dependencies = games.dependencies
            self.assertEqual(len(games_dependencies), 2)
            self.assertEqual(games_dependencies[0].name, 'com.example.bingo')
            self.assertEqual(games_dependencies[0].version, '[1.0.0,2.0.0)')
            self.assertEqual(games_dependencies[1].name, 'com.example.cards')
            self.assertEqual(games_dependencies[1].version, '[1.0.0,1.1.0)')
            
            self.assertTrue(games.delete())
            self.assertTrue(cards.delete())
            self.assertTrue(bingo.delete())

        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)


    # test posting from a bad path
    def test_post_bad_path(self):
        try:
            # path does not exist
            not_exists = 'toolkits/fleegle_tk'
            with self.assertRaises(ValueError):
                fleegle = Toolkit.from_local_toolkit(self.sc, not_exists)
                
            # path is an individual file
            file_exists = 'toolkits/bingo_tk0/toolkit.xml'
            with self.assertRaises(ValueError):
                fleegle = Toolkit.from_local_toolkit(self.sc, file_exists)
                
            # path is malformed garbage
            garbage_path = './toolkits/bingo_tk0\000/snork'
            with self.assertRaises(ValueError):
                fleegle = Toolkit.from_local_toolkit(self.sc, garbage_path)

        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)


    # Test getting toolkit by id.
    def test_get_toolkit(self):
        try:
            toolkit_path = type(self).bingo_1_path
            expected_toolkit_name = type(self).bingo_toolkit_name
            expected_toolkit_version = type(self).bingo_1_version
            
            bingo = Toolkit.from_local_toolkit(self.sc, toolkit_path)
            self.assertIsNotNone(bingo)
            
            # verify that the toolkit is now in the list of all toolkits.
            self.wait_for_toolkit(expected_toolkit_name, expected_toolkit_version)
            
            found = self.sc.get_toolkit(bingo.id)
            self.assertIsNotNone(found)
            self.assertEqual(found.name, expected_toolkit_name)
            self.assertEqual(found.version, expected_toolkit_version)
            self.assertEqual(found.requiredProductVersion, '4.2')
            self.assertEqual(found.resourceType, 'toolkit')
            
            # We don't know what the values the following attributes will have,
            # but we verify that the expected attributes do at least have values.
            self.assertIsNotNone(found.id)
            self.assertIsNotNone(found.index)
            self.assertIsNotNone(found.path)
            self.assertIsNotNone(found.restid)
            self.assertIsNotNone(found.self)
            
            # The ID is streams-toolkits/name-version.
            toolkit_id = 'streams-toolkits/' + expected_toolkit_name + '-' + expected_toolkit_version
            found = self.sc.get_toolkit(toolkit_id)
            
            # Using just the name fails
            with self.assertRaises(ValueError):
                self.sc.get_toolkit('streams-toolkits/' + expected_toolkit_name)
                
                self.assertTrue(bingo.delete())

        except requests.exceptions.HTTPError as err:
            _handle_http_error(err)
