# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import sys
import os
import time
import requests
import uuid


from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ConfigParams
from streamsx.rest import Instance
import streamsx.scripts.streamtool as streamtool


from contextlib import contextmanager
from io import StringIO

@unittest.skipUnless(
    "ICP4D_DEPLOYMENT_URL" in os.environ
    and "STREAMS_INSTANCE_ID" in os.environ
    and "STREAMS_USERNAME" in os.environ
    and "STREAMS_PASSWORD" in os.environ,
    "requires Streams REST API setup",
)
@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class TestAppconfig(unittest.TestCase):

    instance = "zen-edge-sample-icp1-blitz-env"
    stringLength = 10

    # Create the application config
    def _make_appconfig(
        self, config_name, props=None, prop_file=None, description=None
    ):
        args = []
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "mkappconfig")
        args.insert(2, config_name)
        if (description):
            args.insert(3, "--description")
            args.insert(4, description)
        i = 5
        if props:
            for prop in props:
                args.insert(i, "--property")
                i += 1
                args.insert(i, prop)
                i += 1
        if prop_file:
            args.insert(i, "--propfile")
            i += 1
            args.insert(i, prop_file)

        return streamtool.main(args=args)

    def _ls_appconfig(self):
        args = []
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "lsappconfig")
        streamtool.main(args=args)

    def _remove_appconfig(self, config_name, noprompt=False):
        args = []
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "rmappconfig")
        args.insert(2, config_name)
        if noprompt:
            args.insert(3, "--noprompt")
        streamtool.main(args=args)

    def _get_appconfig(self, config_name):
        args = []
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "getappconfig")
        args.insert(2, config_name)

        return streamtool.main(args=args)

    def _ch_appconfig(self, config_name, props=None, description=None):
        args = []
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "chappconfig")
        args.insert(2, config_name)
        if (description):
            args.insert(3, "--description")
            args.insert(4, description)
        i = 5
        if props:
            for prop in props:
                args.insert(i, "--property")
                i += 1
                args.insert(i, prop)
                i += 1
        return streamtool.main(args=args)

    def _get_configs(self, name):
        instance = Instance.of_endpoint(
            username='stest05',
            verify=False)
        configs = instance.get_application_configurations(name = name)[0]
        return configs

    ###########################################
    # mkappconfig
    ###########################################

    # Test -
    #   1. Able to create an appconfig w/ no properties or description
    #   2. If try to update existing appconfig via mkconfig command (either properties or description), it throws error
    def test_mk_1(self):
        # 1
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        with captured_output() as (out, err):
            self._make_appconfig(name)
        output = out.getvalue().strip()

        # Check success message
        correctOut = "The {} application configuration was created successfully for the {} instance".format(
            name, self.instance
        )
        self.assertEqual(output, correctOut)

        configs = self._get_configs(name)

        # Check correct properties and description
        self.assertEqual(configs.properties, {})
        self.assertEqual(configs.description, "")

        # 2
        with captured_output() as (out, err):
            self._make_appconfig(name)
        output = out.getvalue().strip()

        # Check error message
        correctOut = "The {} application configuration already exists in the following {} instance".format(
            name, self.instance
        )
        self.assertEqual(output, correctOut)

        self._remove_appconfig(name, noprompt=True)

    # Test - mkappconfig with --property can only be used to specify 1 key/value pair
    def test_mk_2(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props = ["key1=value1,key2=value2"]
        with captured_output() as (out, err):
            self._make_appconfig(name, props=props)
        output = out.getvalue().strip()

        # Check error message
        correctOut = "The format of the following property specification is not valid: {}. The correct syntax is: <name>=<value>".format(
            props[0]
        )
        self.assertEqual(output, correctOut)

    # Test - mkappconfig with --property check correct format
    def test_mk_3(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props = ["key1=value1", "key1=value1,key2=value2"]
        with captured_output() as (out, err):
            self._make_appconfig(name, props=props)
        output = out.getvalue().strip()
        # Check error message
        correctOut = "The format of the following property specification is not valid: {}. The correct syntax is: <name>=<value>".format(
            props[1]
        )
        self.assertEqual(output, correctOut)

    # Test - mkappconfig w/ both prop_file and —property, keys given in --property override ones specified in prop_file
    def test_mk_4(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props2 = ["key1=value2", "key2=value3", "key6=value7"]
        propFile = "temp.txt"

        self._make_appconfig(name, props=props2, prop_file=propFile)

        configs = self._get_configs(name)

        # Check properties are correct
        my_file = open(propFile)
        prop_list = [line.rstrip() for line in my_file if not line.isspace()]
        my_file.close()
        testProps = self.create_props_dict(prop_list)
        testProps = self.create_props_dict(props2, testProps)

        self.assertEqual(configs.properties, testProps)
        self.assertEqual(configs.description, "")

        self._remove_appconfig(name, noprompt=True)

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
        for i in range(10):
            key = "KEY_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            value = "VALUE_" + uuid.uuid4().hex.upper()[0 : self.stringLength]
            propList.append(key + "=" + value)
        return propList

    ###########################################
    # rmappconfig
    ###########################################

    # Test - If no appconfig exists by given name, print out error message
    def test_rm_1(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        with captured_output() as (out, err):
            self._remove_appconfig(name)
        output = out.getvalue().strip()
        correctOut = "The {} application configuration does not exist in the {} instance".format(
            name, self.instance
        )
        self.assertEqual(output, correctOut)

    # Test - If succesful removal of appconfig, print out success messages
    def test_rm_2(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        with captured_output() as (out, err):
            self._make_appconfig(name)
            self._remove_appconfig(name, noprompt=True)
        output = out.getvalue().strip()
        correctOut = "The {} application configuration was created successfully for the {} instance\n".format(
            name, self.instance
        )
        correctOut += "The {} application configuration was removed successfully for the {} instance".format(
            name, self.instance
        )
        self.assertEqual(output, correctOut)

    ###########################################
    # chappconfig
    ###########################################

    # Test - If trying to update nonexistant appconfig, print out error message
    def test_ch_1(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props = self.generateRandom()
        with captured_output() as (out, err):
            self._ch_appconfig(name, props=props)
        output = out.getvalue().strip()
        correctOut = "The {} application configuration does not exist in the {} instance".format(
            name, self.instance
        )
        self.assertEqual(output, correctOut)

    # Test
    #   Create appconfig with properties and description, update appconfig w/o --property or --description command,
    #   make sure doesn’t overwrite/clear existing properties and description
    def test_ch_2(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props = self.generateRandom()
        description = uuid.uuid4().hex.upper()[0 : self.stringLength]

        with captured_output() as (out, err):
            self._make_appconfig(name, props=props, description=description)
            self._ch_appconfig(name)
        output = out.getvalue().strip()
        correctOut = "The {} application configuration was created successfully for the {} instance\n".format(
            name, self.instance
        )
        correctOut += "The {} application configuration was updated successfully for the {} instance".format(
            name, self.instance
        )

        self.assertEqual(output, correctOut)

        configs = self._get_configs(name)

        self.assertEqual(configs.properties, self.create_props_dict(props))
        self.assertEqual(configs.description, description)

        self._remove_appconfig(name, noprompt=True)

    # Test - Check updated properites and description are correct
    def test_ch_3(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props = self.generateRandom()
        description = uuid.uuid4().hex.upper()[0 : self.stringLength]

        newProps = self.generateRandom()
        newDescription = uuid.uuid4().hex.upper()[0 : self.stringLength]

        with captured_output() as (out, err):
            self._make_appconfig(name, props=props, description= description)
            self._ch_appconfig(name, props=newProps, description= newDescription)
        output = out.getvalue().strip()

        # Check correct output message
        correctOut = "The {} application configuration was created successfully for the {} instance\n".format(
            name, self.instance
        )
        correctOut += "The {} application configuration was updated successfully for the {} instance".format(
            name, self.instance
        )
        self.assertEqual(output, correctOut)

        configs = self._get_configs(name)

        propsDict = self.create_props_dict(props=props)
        propsDict = self.create_props_dict(props=newProps, config_props = propsDict)

        # Check that new property and description is correct
        self.assertEqual(configs.properties, propsDict)
        self.assertEqual(configs.description, newDescription)

        self._remove_appconfig(name, noprompt=True)

    ###########################################
    # getappconfig
    ###########################################

    # Test - If config exists, but no properties, print out error message
    def test_get_1(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        with captured_output() as (out, err):
            self._make_appconfig(name)
            self._get_appconfig(name)
        output = out.getvalue().strip()
        correctOut = "The {} application configuration was created successfully for the {} instance\n".format(
            name, self.instance
        )
        correctOut += "The {} application configuration has no properties defined".format(
            name
        )

        self.assertEqual(output, correctOut)

        self._remove_appconfig(name, noprompt=True)

    # Test - getAppconfig outputs corrects property data
    def test_get_2(self):
        name = "TEST__" + uuid.uuid4().hex.upper()[0 : self.stringLength]
        props = self.generateRandom()
        with captured_output() as (out, err):
            self._make_appconfig(name,props=props)
            self._get_appconfig(name)
        output = out.getvalue().strip().splitlines()
        successMessage = output.pop(0)
        correctOut = "The {} application configuration was created successfully for the {} instance".format(
            name, self.instance
        )
        self.assertEqual(successMessage, correctOut)

        self.assertEqual(set(output), set(props))

        self._remove_appconfig(name, noprompt=True)
