# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
from streamsx.topology import context
from streamsx.topology.context import ConfigParams
from streamsx import rest
import streamsx.ec as ec


import test_vers

class view_name_source(object):
    """A class which wraps a StreamsConnection object and returns the view name
    """
    def __init__(self, sc):
        self.sc = sc

    def __call__(self):
        instance = self.sc.get_instance(id=ec.instance_id())
        job = instance.get_job(id=ec.job_id())
        for view in job.get_views():
            yield view.name

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestUnicode(unittest.TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

    def test_strings(self):
        """ Test strings that are unicode.
            Includes a stream name to verify it
            does not cause an error, but under the covers
            the actual name will be a mangled version of it
            since SPL identifiers are only ASCII.
        """
        topo = Topology()
        ud = []
        ud.append('⡍⠔⠙⠖ ⡊ ⠙⠕⠝⠰⠞ ⠍⠑⠁⠝ ⠞⠕ ⠎⠁⠹ ⠹⠁⠞ ⡊ ⠅⠝⠪⠂ ⠕⠋ ⠍⠹')
        ud.append('2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm')
        ud.append('многоязычных')
        ud.append("Arsenal hammered 5-1 by Bayern again")
        s = topo.source(ud, name='façade')
        sas = s.as_string()
        sd = s.map(lambda s : {'val': s + "_test_it!"})
        tester = Tester(topo)
        tester.contents(s, ud)
        tester.contents(sas, ud)
        dud = []
        for v in ud:
            dud.append({'val': v + "_test_it!"})
        tester.contents(sd, dud)

        tester.test(self.test_ctxtype, self.test_config)
        print(tester.result)

    def test_view_name(self):
        """
        Test view names that are unicode.
        """
        if self.test_ctxtype == context.ContextTypes.STANDALONE:
            return self.skipTest("Skipping unicode view tests for standalone.")
        view_names = ["®®®®", "™¬⊕⇔"]
        topo = Topology()
        view_name_stream = topo.source(view_name_source(self.sc))

        view0 = topo.source(["hello"]).view(name=view_names[0])
        view1 = topo.source(["hello"]).view(name=view_names[1])

        tester = Tester(topo)
        tester.contents(view_name_stream, view_names, ordered=False)

        # For running Bluemix tests, the username & password need a default value of None
        username = getattr(self, "username", None)
        password = getattr(self, "password", None)
        tester.test(self.test_ctxtype, self.test_config, username=username, password=password)

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestDistributedUnicode(TestUnicode):
    def setUp(self):
        Tester.setup_distributed(self)

        # Get username and password
        username = self.username
        password = self.password

        self.sc = rest.StreamsConnection(username=username, password=password)

        # Disable SSL verification
        self.sc.session.verify = False
        self.test_config[ConfigParams.STREAMS_CONNECTION] = self.sc

@unittest.skipIf(not test_vers.tester_supported() , "Tester not supported")
class TestBluemixUnicode(TestUnicode):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        vcap = self.test_config.get('topology.service.vcap')
        sn = self.test_config.get('topology.service.name')
        self.sc = rest.StreamingAnalyticsConnection(vcap, sn)
