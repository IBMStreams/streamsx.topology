# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
import sys
import streamsx.topology.context

def standalone(test, topo):
    rc = streamsx.topology.context.submit("STANDALONE", topo)
    test.assertEqual(0, rc['return_code'])

