# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

from __future__ import print_function
from future.builtins import *

import sys
import os
import argparse
import pkg_resources
import streamsx._streams._version
import streamsx.topology.context

def main(args=None):
    """ Output information about `streamsx` and the environment.
   
        Useful for support to get key information for use of `streamsx`
        and Python in IBM Streams.
    """
    _parse_args(args)
    streamsx._streams._version._mismatch_check('streamsx.topology.context')
    srp = pkg_resources.working_set.find(pkg_resources.Requirement.parse('streamsx'))
    if srp is not None:
        srv = srp.parsed_version
        location = srp.location
        spkg = 'package'
    else:
        srv = streamsx._streams._version.__version__
        location = os.path.dirname(streamsx._streams._version.__file__)
        location = os.path.dirname(location)
        location = os.path.dirname(location)
        tk_path = (os.path.join('com.ibm.streamsx.topology', 'opt', 'python', 'packages'))
        spkg = 'toolkit' if location.endswith(tk_path) else 'unknown'

    print('streamsx==' + str(srv) + ' (' + spkg + ')')
    print('  location: ' + str(location))
    print('Python version:' + str(sys.version))
    print('PYTHONHOME=' + str(os.environ.get('PYTHONHOME', 'unset')))
    print('PYTHONPATH=' + str(os.environ.get('PYTHONPATH', 'unset')))
    print('PYTHONWARNINGS=' + str(os.environ.get('PYTHONWARNINGS', 'unset')))
    print('STREAMS_INSTALL=' + str(os.environ.get('STREAMS_INSTALL', 'unset')))
    print('JAVA_HOME=' + str(os.environ.get('JAVA_HOME', 'unset')))
    return 0

def _parse_args(args):
    """ Argument parsing
    """
    cmd_parser = argparse.ArgumentParser(description='Prints support information about streamsx package and environment.')
    return cmd_parser.parse_args(args)

if __name__ == '__main__':
    sys.exit(main())
