# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import sys
import smtplib

# Import the SPL decorators
from streamsx.spl import spl


# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def spl_namespace():
    return "com.ibm.streamsx.topology.pysamples.mail"

# Decorate this class as a sink operator
# This means the operator will have a single
# input port and no output ports. The SPL tuple
# is passed in as described in spl_samples.py.
# The function must return None, typically by
# not having any return statement.
@spl.for_each(style='position')
class simplesendmail(object):
    "Send a simple email."
    def __init__(self):
        self.server = smtplib.SMTP('localhost')

    def __call__(self, from_addr, to_addrs, msg):
        self.server.sendmail(from_addr, to_addrs, msg)

# Example of a function that is explicitly ignored
# by the SPL extractor (spl-python-extract.py)
@spl.ignore
def thisIsNotAnOperator(a):
    return a+2

# Example of a function that is implicitly ignored
# by the SPL extractor (spl-python-extract.py)
def neitherIsThis(b):
    return b+7
