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
from contextlib import contextmanager
from io import StringIO
import streamsx.scripts.streamtool as streamtool

@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def cpd_setup():
    env = os.environ
    return (("CP4D_URL" in env and "STREAMS_INSTANCE_ID" in env) or \
         ('STREAMS_BUILD_URL' in env and 'STREAMS_REST_URL')) \
         and \
         "STREAMS_USERNAME" in os.environ and \
         "STREAMS_PASSWORD" in os.environ

# Tests rmtoolkit script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(cpd_setup(), "requires Streams REST API setup")
class Testlstoolkit(unittest.TestCase):

    def _run_lstoolkit(self, args):
        args.insert(0, "--disable-ssl-verify")
        args.insert(1, "lstoolkit")

        with captured_output() as (out, err):
            rc, msg = streamtool.run_cmd(args=args)
        stdout = out.getvalue().splitlines()
        self._check_header(stdout)
        return rc, stdout, msg

    def _check_header(self, out):
        expected = ["Name", "Version", "RequiredProductVersion"]
        headers = out[0].split()
        self.assertEqual(expected, headers)

    def _check_tk(self, out, name):
        for info in out[1:]:
            tk = info.split()
            if name == tk[0]:
                return True

    def test_all(self):
        rc, out, msg = self._run_lstoolkit(['--all'])
        self.assertEqual(0, rc)
        self.assertTrue(len(out) > 1)

        # Just check a couple of known toolkits are in the list
        self.assertTrue(self._check_tk(out, 'spl'))
        self.assertTrue(self._check_tk(out, 'com.ibm.streamsx.topology'))

    def test_id(self):
        bs = BuildService.of_endpoint(verify=False)
        for tk in bs.get_toolkits(name='com.ibm.streamsx.topology'):
            rc, out, msg = self._run_lstoolkit(['--id', tk.id])
            self.assertEqual(0, rc)
            self.assertTrue(len(out) == 2)
            self.assertTrue(self._check_tk(out, tk.name))

    def test_name(self):
        rc, out, msg = self._run_lstoolkit(['--name', 'spl'])
        self.assertEqual(0, rc)
        self.assertTrue(len(out) == 2)

        # Just check a couple of known toolkits are in the list
        self.assertTrue(self._check_tk(out, 'spl'))
        self.assertFalse(self._check_tk(out, 'com.ibm.streamsx.topology'))

    def test_name_no_match(self):
        rc, out, msg = self._run_lstoolkit(['--name', 'sf2o3rwefhw23e'])
        self.assertEqual(0, rc)
        self.assertTrue(len(out) == 1)

    def test_regex(self):
        rc, out, msg = self._run_lstoolkit(['--regex', 'com.ibm.streamsx.*'])
        self.assertEqual(0, rc)
        self.assertTrue(len(out) >= 2)

        # Just check a couple of known toolkits are in the list
        self.assertFalse(self._check_tk(out, 'spl'))
        self.assertTrue(self._check_tk(out, 'com.ibm.streamsx.topology'))

    def test_regex_nomatch(self):
        rc, out, msg = self._run_lstoolkit(['--regex', 'w2r3fswfq.*'])
        self.assertEqual(0, rc)
        self.assertTrue(len(out) == 1)
