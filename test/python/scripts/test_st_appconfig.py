# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import sys
import os
import time
import requests
import uuid
import re 


from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
from streamsx.rest import Instance
import streamsx.scripts.streamtool as streamtool


from contextlib import contextmanager
from io import StringIO


@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


@unittest.skipUnless(
    "CP4D_URL" in os.environ
    and "STREAMS_INSTANCE_ID" in os.environ
    and "STREAMS_USERNAME" in os.environ
    and "STREAMS_PASSWORD" in os.environ,
    "requires Streams REST API setup",
)
class TestAppconfig(unittest.TestCase):

    # Create the application config
    def _make_appconfig(
        self, config_name, props=None, prop_file=None, description=None
    ):
        args = ["--disable-ssl-verify", "mkappconfig", config_name]
        if description:
            args.extend(["--description", description])
        i = 5
        if props:
            for prop in props:
                args.insert(i, "--property")
                i += 1
                args.insert(i, prop)
                i += 1
        if prop_file:
            args.extend(["--propfile", prop_file])

        return streamtool.main(args=args)

    def _ls_appconfig(self, fmt=None):
        args = ["--disable-ssl-verify", "lsappconfig"]
        if fmt:
            args.extend(["--fmt", fmt])
        return streamtool.main(args=args)

    def _remove_appconfig(self, config_name, noprompt=False):
        args = ["--disable-ssl-verify", "rmappconfig", config_name]
        if noprompt:
            args.insert(3, "--noprompt")
        return streamtool.main(args=args)

    def _get_appconfig(self, config_name):
        args = ["--disable-ssl-verify", "getappconfig", config_name]
        return streamtool.main(args=args)

    def _ch_appconfig(self, config_name, props=None, description=None):
        args = ["--disable-ssl-verify", "chappconfig", config_name]
        if description:
           args.extend(["--description", description])
        i = 5
        if props:
            for prop in props:
                args.insert(i, "--property")
                i += 1
                args.insert(i, prop)
                i += 1
        return streamtool.main(args=args)

    def _get_configs(self, name):
        instance = Instance.of_endpoint(username= self.username, verify=False)
        configs = instance.get_application_configurations(name=name)[0]
        return configs


    def setUp(self):
        self.instance = os.environ['STREAMS_INSTANCE_ID']
        self.stringLength = 10
        self.username = os.environ['STREAMS_USERNAME']
        self.name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        self.appconfigs_to_remove = [self.name]

    ###########################################
    # mkappconfig
    ###########################################

    # Able to create an appconfig w/ no properties or description
    def test_create_simple_appconfig(self):
        output, error, rc = self.get_output(lambda: self._make_appconfig(self.name))
        # Check success message
        correctOut = "The {} application configuration was created successfully for the {} instance".format(
            self.name, self.instance
        )
        self.assertEqual(output, correctOut)

        configs = self._get_configs(self.name)
        self.assertEqual(rc, 0)

        # Check correct properties and description
        self.assertEqual(configs.properties, {})
        self.assertEqual(configs.description, "")

    # If try to update existing appconfig via mkconfig command (either properties or description), it throws error
    def test_cannot_create_or_update_existing_appconfig(self):
        self._make_appconfig(self.name)
        output, error, rc = self.get_output(lambda: self._make_appconfig(self.name))

        # Check error message
        correctOut = "The {} application configuration already exists in the following {} instance".format(
            self.name, self.instance
        )
        error = error.splitlines()

        self.assertEqual(error[-1], correctOut)
        self.assertEqual(rc, 1)

    # mkappconfig with --property can only be used to specify 1 key/value pair
    def test_appconfig_correct_property_format(self):
        props = ["key1=value1,key2=value2"]

        output, error, rc = self.get_output(lambda: self._make_appconfig(self.name, props=props))

        # Check error message
        correctOut = "The format of the following property specification is not valid: {}. The correct syntax is: <name>=<value>".format(
            props[0]
        )

        error = error.splitlines()

        self.assertEqual(error[-1], correctOut)
        self.assertEqual(rc, 1)

    # mkappconfig with --property check correct format
    def test_appconfig_correct_property_format_2(self):
        props = ["key1=value1", "key1=value1,key2=value2"]
        output, error, rc = self.get_output(lambda: self._make_appconfig(self.name, props=props))
        # Check error message
        correctOut = "The format of the following property specification is not valid: {}. The correct syntax is: <name>=<value>".format(
            props[1]
        )
        error = error.splitlines()

        self.assertEqual(error[-1], correctOut)
        self.assertEqual(rc, 1)

    # Test - mkappconfig w/ both prop_file and —property, keys given in --property override ones specified in prop_file
    def test_make_correct_property(self):
        props2 = ["key1=value2", "key2=value3", "key6=value7"]
        propFile = "test_st_appconfig_properties.txt"

        rc = self._make_appconfig(self.name, props=props2, prop_file=propFile)

        configs = self._get_configs(self.name)

        # Check properties are correct
        my_file = open(propFile)
        prop_list = [line.rstrip() for line in my_file if not line.isspace()]
        my_file.close()
        testProps = self.create_props_dict(prop_list)
        testProps = self.create_props_dict(props2, testProps)

        self.assertEqual(configs.properties, testProps)
        self.assertEqual(configs.description, "")
        self.assertEqual(rc, 0)

    def create_props_dict(self, props, config_props=None):
        """Helper function that creates a property dictionary consisting of key-value pairs

        Arguments:
            props {List} -- A list consisting of elements of the form <KEY>=<VALUE>

        Keyword Arguments:
            config_props {Dictionary} -- An existing dictionary to which new key-value pairs from props are added to, or updated (default: {None})

        Returns:
            config_props {Dictionary} -- The property dictionary with the key-value pairs
        """
        if not config_props:
            config_props = {}
        if props:
            for prop in props:
                name_value_pair = prop.split("=")
                if len(name_value_pair) != 2:
                    print(
                        "The format of the following property specification is not valid: {}. The correct syntax is: <name>=<value>".format(
                            prop
                        )
                    )
                    exit()
                config_props[name_value_pair[0]] = name_value_pair[1]

        return config_props

    def generateRandom(self):
        """ Helper function that generates random key-value pairs of the form <KEY>=<VALUE> and returns them in a list

        Returns:
            propList [List] -- A list consisting of elements of the form <KEY>=<VALUE> pairs
        """
        propList = []
        for _ in range(10):
            key = "KEY_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            value = "VALUE_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            propList.append(key + "=" + value)
        return propList

    def get_output(self, my_function):
        """ Helper function that gets the ouput from executing my_function

        Arguments:
            my_function {} -- The function to be executed

        Returns:
            stdout {String} -- Output of my_function
            stderr {String} -- Errors and exceptions from executing my_function
            rc {int} -- 0 indicates succces, 1 indicates error or failure
        """
        rc = None
        with captured_output() as (out, err):
            rc = my_function()
        stdout = out.getvalue().strip()
        stderr = err.getvalue().strip()
        return stdout, stderr, rc

    ###########################################
    # rmappconfig
    ###########################################

    # If no appconfig exists by given name, print out error message
    def test_remove_nonexistant_appconfig(self):
        output, error, rc = self.get_output(lambda: self._remove_appconfig(self.name))
        correctOut = "The {} application configuration does not exist in the {} instance".format(
            self.name, self.instance
        )

        error = error.splitlines()

        self.assertEqual(error[-1], correctOut)
        self.assertEqual(rc, 1)

    # If succesful removal of appconfig, print out success messages
    def test_remove_simple_appconfig(self):
        self._make_appconfig(self.name)
        output, error, rc = self.get_output(lambda: self._remove_appconfig(self.name, noprompt=True))
        correctOut = "The {} application configuration was removed successfully for the {} instance".format(
            self.name, self.instance
        )
        self.assertEqual(output, correctOut)
        self.assertEqual(rc, 0)

    ###########################################
    # chappconfig
    ###########################################

    # If trying to update nonexistant appconfig, print out error message
    def test_update_nonexistant_appconfig(self):
        props = self.generateRandom()
        output, error, rc = self.get_output(lambda: self._ch_appconfig(self.name, props=props))
        correctOut = "The {} application configuration does not exist in the {} instance".format(
            self.name, self.instance
        )

        error = error.splitlines()

        self.assertEqual(error[-1], correctOut)
        self.assertEqual(rc, 1)

    # Create appconfig with properties and description, update appconfig w/o --property or --description command,
    # make sure doesn’t overwrite/clear existing properties and description
    def test_update_correct_property_and_description(self):
        props = self.generateRandom()
        description = uuid.uuid4().hex.upper()[0 : self.stringLength]

        self._make_appconfig(self.name, props=props, description=description)
        output, error, rc = self.get_output(lambda: self._ch_appconfig(self.name))
        correctOut = "The {} application configuration was updated successfully for the {} instance".format(
            self.name, self.instance
        )

        self.assertEqual(output, correctOut)

        configs = self._get_configs(self.name)

        self.assertEqual(configs.properties, self.create_props_dict(props))
        self.assertEqual(configs.description, description)
        self.assertEqual(rc, 0)

    # Check updated properites and description are correct
    def test_update_correct_property_and_description_2(self):
        props = self.generateRandom()
        description = uuid.uuid4().hex.upper()[0 : self.stringLength]

        newProps = self.generateRandom()
        newDescription = uuid.uuid4().hex.upper()[0 : self.stringLength]

        self._make_appconfig(self.name, props=props, description=description)

        output, error, rc = self.get_output(
            lambda: self._ch_appconfig(self.name, props=newProps, description=newDescription)
        )

        # Check correct output message
        correctOut = "The {} application configuration was updated successfully for the {} instance".format(
            self.name, self.instance
        )
        self.assertEqual(output, correctOut)

        configs = self._get_configs(self.name)

        propsDict = self.create_props_dict(props=props)
        propsDict = self.create_props_dict(props=newProps, config_props=propsDict)

        # Check that new property and description is correct
        self.assertEqual(configs.properties, propsDict)
        self.assertEqual(configs.description, newDescription)

        self.assertEqual(rc, 0)

    ###########################################
    # getappconfig
    ###########################################

    # If config exists, but no properties, print out error message
    def test_get_nonexistant_property(self):
        self._make_appconfig(self.name)

        output, error, rc = self.get_output(lambda: self._get_appconfig(self.name))
        correctOut = "The {} application configuration has no properties defined".format(
            self.name
        )

        error = error.splitlines()

        self.assertEqual(error[-1], correctOut)
        self.assertEqual(rc, 1)

    # getAppconfig outputs corrects property data
    def test_get_correct_property(self):
        props = self.generateRandom()
        self._make_appconfig(self.name, props=props)

        output, error, rc= self.get_output(lambda: self._get_appconfig(self.name))
        output = output.splitlines()

        self.assertEqual(set(output), set(props))
        self.assertEqual(rc, 0)

    ###########################################
    # lsappconfig
    ###########################################

    def split_string(self, my_string):
        """ Helper function that splits my_string by 2 or more whitespaces

        Arguments:
            my_string {String} -- [description]

        Returns:
            {List} -- List containing the elements of my_string that are seperated by 2 or more whitespaces
        """
        return re.split(r"\s{2,}", my_string.strip())

    ###########################################
        # Tf fmt
    ###########################################

    # Create a single appconfig, check correct ouput in default Tf format
    def test_lsappconfig_simple(self):
        self._make_appconfig(self.name)
        output, error, rc= self.get_output(lambda: self._ls_appconfig())
        output = output.splitlines()

        # Check headers outputs correctly
        true_headers = ["Id", "Owner", "Created", "Modified", "Description"]
        headers = self.split_string(output[0])
        self.assertEqual(true_headers, headers)

        # Can't assume this is the only appconfig , find the line w/ our config, then do checks
        config = None
        for line in output:
            if self.name in line:
                config = line

        # If we can't find our appconfig in the output, should fail
        if not config :
            self.fail("Config should be in output")

        # Check details of appconfig are correct
        appConfig = self.split_string(config)
        self.assertEqual(self.name, appConfig[0])
        self.assertEqual(self.username, appConfig[1])
        self.assertTrue(len(appConfig) == 4) # len 4 bc no description given
        self.assertEqual(rc, 0)

    # Create 2 appconfigs w/ names and descriptions, check correct ouput in default Tf format
    def test_lsappconfig_complex(self):
        # Create 2 appconfigs
        description1 = 'askmdakdlmldkmqwmdlqkwmdlkqdmqwklm'
        description2 = '394902384902358230952903892304890234820394'
        self._make_appconfig(self.name, description=description1)
        name2 = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        self._make_appconfig(name2, description=description2)
        self.appconfigs_to_remove.append(name2)

        output, error, rc= self.get_output(lambda: self._ls_appconfig())
        output = output.splitlines()

        # Check headers outputs correctly
        true_headers = ["Id", "Owner", "Created", "Modified", "Description"]
        headers = self.split_string(output[0])
        self.assertEqual(true_headers, headers)

        # Can't assume this is the only appconfig , find the line w/ our config, then do checks
        config1 = None
        config2 = None
        for line in output:
            if self.name in line:
                config1 = line
            elif name2 in line:
                config2 = line

        # If we can't find our appconfig in the output, should fail
        if not config1 or not config2 :
            self.fail("Configs should be in output")

        # Check details of appconfig1 are correct
        appConfig1 = self.split_string(config1)
        self.assertTrue(len(appConfig1) == 5) # bc appconfig has 5 columns
        self.assertEqual(appConfig1[0], self.name)
        self.assertEqual(appConfig1[1], self.username)
        self.assertEqual(appConfig1[4], description1)

        # Check details of appconfig2 are correct
        appConfig2 = self.split_string(config2)
        self.assertTrue(len(appConfig2) == 5) # bc appconfig has 5 columns
        self.assertEqual(appConfig2[0], name2)
        self.assertEqual(appConfig2[1], self.username)
        self.assertEqual(appConfig2[4], description2)

        self.assertEqual(rc, 0)

    ###########################################
        # Mf fmt
    ###########################################

    def get_lsappconfig_Mf_fmt(self, output):
        """ Helper function that gets a single appconfig block outputed from lsappconfig in Mf format

        Arguments:
            output {String} -- A string given by the ouput of lsappconfig in Mf format

        Returns:
            config {String} -- A string of the form (given below) that represents a single config outputed from lsappconfig in Mf format
            output {String} -- A string given by the output of lsappconfig in Mf format
        """
        # Ex of config
        # =================================================
        # Id          : TEST1
        # Owner       : streamsadmin
        # Created     : 6/21/19, 11:46:56 AM PDT
        # Modified    : 6/24/19, 3:16:09 PM PDT
        # Description : blahbblah
        # =================================================


        # Check if output is only 1 config, then there is no more output after that
        if len(output) == 7:
            config = output
            output = None
        else:
            config = output[:7]
            output = output[6:]

        return config, output

    # Create a single appconfig, check correct ouput in Mf format
    def test_lsappconfig_simple_Mf_fmt(self):
        self._make_appconfig(self.name)
        output, error, rc= self.get_output(lambda: self._ls_appconfig(fmt='%Mf'))
        output = output.splitlines()

        # Iterate through each config block in output till we find the config we want check
        appConfig = None
        while output:
            config, output = self.get_lsappconfig_Mf_fmt(output)
            if any(self.name in substring for substring in config):
                appConfig = config
                break

        if not appConfig:
            self.fail("Config should be in output")

        # Check details of appconfig are correct
        ids = self.split_string(appConfig[1])
        owner = self.split_string(appConfig[2])
        created = self.split_string(appConfig[3])
        modified = self.split_string(appConfig[4])
        description = self.split_string(appConfig[5])

        true_headers = ["Id", "Owner", "Created", "Modified", "Description"]
        headers = [ids[0], owner[0], created[0], modified[0], description[0]]
        self.assertEqual(true_headers, headers)

        self.assertEqual(ids[2], self.name)
        self.assertEqual(owner[2], self.username)
        self.assertTrue(len(description) == 2)

        self.assertEqual(rc, 0)


    ###########################################
        # Nf fmt
    ###########################################

    # Create a single appconfig, check correct ouput in Nf format
    def test_lsappconfig_simple_Nf_fmt(self):
        self._make_appconfig(self.name)
        output, error, rc= self.get_output(lambda: self._ls_appconfig(fmt='%Nf'))
        output = output.splitlines()


        # Can't assume this is the only appconfig, find the line w/ our appconfig, then do checks
        appconfig1 = None
        for line in output:
            if self.name in line:
                appconfig1 = line

        # If we can't find our appconfig in the output, should fail
        if not appconfig1 :
            self.fail("config should be in output")

        # Check details of appconfig are correct
        self.assertTrue("Id" in appconfig1)
        self.assertTrue("Owner" in appconfig1)
        self.assertTrue("Created" in appconfig1)
        self.assertTrue("Modified" in appconfig1)
        self.assertTrue("Description" in appconfig1)

        self.assertTrue(self.name in appconfig1)
        self.assertTrue(self.username in appconfig1)

        self.assertEqual(rc, 0)

    # Create 2 appconfigs w/ names and descriptions, check correct ouput in Nf format
    def test_lsappconfig_complex_Nf_fmt(self):
        # Create 2 appconfigs
        description1 = 'askmdakdlmldkmqwmdlqkwmdlkqdmqwklm'
        description2 = '394902384902358230952903892304890234820394'
        self._make_appconfig(self.name, description=description1)
        name2 = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        self._make_appconfig(name2, description=description2)
        self.appconfigs_to_remove.append(name2)
        output, error, rc= self.get_output(lambda: self._ls_appconfig(fmt='%Nf'))
        output = output.splitlines()


        # Can't assume this is the only appconfig, find the line w/ our appconfig, then do checks
        appconfig1 = None
        appconfig2 = None
        for line in output:
            if self.name in line:
                appconfig1 = line
            elif name2 in line:
                appconfig2 = line

        # If we can't find our appconfig in the output, should fail
        if not appconfig1 or not appconfig2:
            self.fail("configs should be in output")

        # Check details of appconfig are correct
        self.assertTrue("Id" in appconfig1)
        self.assertTrue("Owner" in appconfig1)
        self.assertTrue("Created" in appconfig1)
        self.assertTrue("Modified" in appconfig1)
        self.assertTrue("Description" in appconfig1)

        self.assertTrue(self.name in appconfig1)
        self.assertTrue(self.username in appconfig1)
        self.assertTrue(description1 in appconfig1)

        # Check details of appconfig are correct
        self.assertTrue("Id" in appconfig2)
        self.assertTrue("Owner" in appconfig2)
        self.assertTrue("Created" in appconfig2)
        self.assertTrue("Modified" in appconfig2)
        self.assertTrue("Description" in appconfig2)

        self.assertTrue(name2 in appconfig2)
        self.assertTrue(self.username in appconfig2)
        self.assertTrue(description2 in appconfig2)

        self.assertEqual(rc, 0)

    def tearDown(self):
        for app in self.appconfigs_to_remove:
            self._remove_appconfig(app, noprompt=True)
