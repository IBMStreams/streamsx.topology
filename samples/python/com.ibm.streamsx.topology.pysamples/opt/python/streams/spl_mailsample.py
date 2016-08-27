# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import sys
import smtplib

# Import the SPL decorators
from streamsx.spl import spl

server = smtplib.SMTP('localhost')

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def splNamespace():
    return "com.ibm.streamsx.topology.pysamples.mail"

# Decorate this function as a sink operator
# This means the operator will have a single
# input port and no output ports. The SPL tuple
# is passed in as described in spl_samples.py.
# The function must return None, typically by
# not having any return statement.
@spl.sink
def simplesendmail(from_addr, to_addrs, msg):
    "Send a simple email."
    server.sendmail(from_addr, to_addrs, msg)


# Example of a function that is explicitly ignored by the SPL extractor (spl-extract.py)
@spl.ignore
def thisIsNotAnOperator(a):
    return a+2

# Example of a function that is implicitly ignored by the SPL extractor (spl-extract.py)
def neitherIsThis(b):
    return b+7
