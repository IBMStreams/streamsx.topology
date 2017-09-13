# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import os
import sys
import streamsx.topology.context

def standalone(test, topo):
    if 'STREAMS_INSTALL' not in os.environ:
        test.skipTest("No STREAMS_INSTALL")
    if not os.environ['STREAMS_INSTALL']:
        test.skipTest("STREAMS_INSTALL empty")
    rc = streamsx.topology.context.submit("STANDALONE", topo)
    test.assertEqual(0, rc['return_code'])

