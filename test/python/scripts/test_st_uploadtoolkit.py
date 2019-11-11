# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import unittest
import sys
import os
import random
import shutil
import string
import tempfile
import subprocess

from streamsx.build import BuildService
import streamsx.scripts.streamtool as streamtool

def cpd_setup():
    env = os.environ
    return (("CP4D_URL" in env and "STREAMS_INSTANCE_ID" in env) or \
         ('STREAMS_BUILD_URL' in env and 'STREAMS_REST_URL')) \
         and \
         "STREAMS_USERNAME" in os.environ and \
         "STREAMS_PASSWORD" in os.environ and \
         "STREAMS_INSTALL" in os.environ

# Tests rmtoolkit script.
# Requires environment setup for a ICP4D Streams instance.
@unittest.skipUnless(cpd_setup(), "requires Streams REST API setup")
class Testupdatetoolkit(unittest.TestCase):

    def setUp(self):
        self.tkdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tkdir)

    def _mktoolkit(self):
        name = ''.join(random.choices(string.ascii_uppercase, k=24))
        ver = '1.2.' + str(random.randint(0, 1000))
        product = '5.0.' + str(random.randint(0, 1000))
        with open(os.path.join(self.tkdir, 'm.spl'), 'w') as f:
            f.write('void f() {}\n')
        smt = []
        smt.append(os.path.join(os.environ['STREAMS_INSTALL'], 'bin', 'spl-make-toolkit'))
        smt.append('--directory')
        smt.append(self.tkdir)
        smt.append('--name')
        smt.append(name)
        smt.append('--version')
        smt.append(ver)
        smt.append('--product-version')
        smt.append(product)
        print(smt)
        subprocess.run(smt)
        self.assertTrue(os.path.isfile(os.path.join(self.tkdir, 'toolkit.xml')))
        return name, ver, product

    def test_upload(self):
        name, ver, product = self._mktoolkit()
        args = []
        args.append("--disable-ssl-verify")
        args.append("uploadtoolkit")
        args.append("--path")
        args.append(self.tkdir)
        rc, msg = streamtool.run_cmd(args)
        self.assertEqual(0, rc)

        bs = BuildService.of_endpoint(verify=False)
        tks = bs.get_toolkits(name=name)
        self.assertEqual(1, len(tks))
        tk = tks[0]

        self.assertEqual(name, tk.name)
        self.assertEqual(ver, tk.version)
        self.assertEqual(product, tk.requiredProductVersion)

        tk.delete()
