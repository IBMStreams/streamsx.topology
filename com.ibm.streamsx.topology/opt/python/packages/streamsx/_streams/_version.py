# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

## Common PEP396 version for modules shipped with streamsx packages
__version__='1.11.7b'

import pkg_resources
import sys
import warnings


# Attempt to detect cases where the python code does not
# match the package install. Typically due to having
# PYTHONPATH point into an older topology toolkit as
# is set by default by streamsprofile.sh
def _mismatch_check(module_name):
    srp = pkg_resources.working_set.find(pkg_resources.Requirement.parse('streamsx'))
    if srp is None or not srp.has_version():
        spv = pkg_resources.parse_version(__version__)
    else:
        spv = srp.parsed_version

    if module_name not in sys.modules:
        return

    sm = sys.modules[module_name]

    file_name = sm.__file__ if hasattr(sm, '__file__') else 'unknown location'

    # Pre 1.11 versions did not maintain a version
    # number so they must be older than this code
    # added in 1.11
    if not hasattr(sm, '__version__'):
        warnings.warn(_warning_msg(spv, module_name, file_name))
        return

    opv = pkg_resources.parse_version(sm.__version__)
    if opv < spv:
        warnings.warn(_warning_msg(spv, module_name, file_name, opv))
        return

def _warning_msg(spv, module_name, file_name, version=None):
    version = ": " + str(version) if version else ""
    return "Version mismatch: streamsx " + str(spv) + " is installed but " + module_name + " from " + file_name +  " is at an older version" + version + "."
