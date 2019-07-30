# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import sys
import sysconfig
import os
import argparse
import urllib3
import streamsx.rest
from streamsx.spl.op import main_composite
from streamsx.spl.toolkit import add_toolkit
from streamsx.topology.context import submit, ConfigParams
import xml.etree.ElementTree as ET
from glob import glob
import re
from packaging import version


_FACADE=['--prefer-facade-tuples', '-k']

def main(args=None):
    """ Mimics SPL SPL compiler sc against the ICP4D build service.
    """
    cmd_args = _parse_args(args)
    topo = _create_topo(cmd_args)

    # Get dependencies for app, if any at all
    dependencies = _parse_dependencies()
    # if dependencies and if -t arg, find & add local toolkits
    if dependencies and cmd_args.spl_path:
        tool_kits = cmd_args.spl_path.split(':')
        # Check if any dependencies are in the passed in toolkits, if so add them
        _add_local_toolkits(tool_kits, dependencies, topo)

    _add_toolkits(cmd_args, topo)
    _submit_build(cmd_args, topo)
    return 0

def _parse_dependencies():
    # Parse info.xml for the dependencies of the app you want to build sab file for
    deps = {} # name - version
    if os.path.isfile('info.xml'):
        root = ET.parse('info.xml').getroot()
        info = '{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}'
        common = '{http://www.ibm.com/xmlns/prod/streams/spl/common}'
        dependency_elements = root.find(info + 'dependencies').findall(info + 'toolkit')
        for dependency_element in dependency_elements:
            name = dependency_element.find(common + 'name').text
            version = dependency_element.find(common + 'version').text
            deps[name] = version
    return deps

def _add_local_toolkits(toolkits, dependencies, topo):
    """ A helper function that given the local toolkit paths, and the dependencies (ie toolkits that the apps depends on),
    checks and adds the local toolkit if it is one of the dependencies

    Arguments:
        toolkits {List} -- A list of local toolkit directories, each could be a toolkit directory (has info.xml directly inside),
                or a directory consisting of toolkits (no info.xml directly inside)
        dependencies {Dictionary} -- A dictionary where the key is the names of the dependencies, and the value is its version.
        topo {topology Object} -- [description]
    """
    local_toolkits = [] # toolkit objects
    #  For each path, check if its a SPL toolkit (has info.xml directly inside) or a directory consisting of toolkits (no info.xml directly inside)
    for x in toolkits:
        if _is_local_toolkit(x):
            tk = _get_toolkit(x)
            local_toolkits.append(tk)
        else:
            # directory consisting of toolkits, get list and check which are local_toolkits, add them
            sub_directories = glob(x + "*/")
            for y in sub_directories:
                if _is_local_toolkit(y):
                    tk = _get_toolkit(y)
                    local_toolkits.append(tk)

    # Once we have all local toolkits, check if they correspond to any dependencies that we have, if so, add them
    for toolkit in local_toolkits:
        if toolkit.name in dependencies:
            _check_satisfies_version(toolkit, dependencies[toolkit.name], topo)

def _get_toolkit(path):
    # Get the name & version of the toolkit that is in the directory PATH
    root = ET.parse(path + "/info.xml").getroot()
    identity = root.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}identity')
    name = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}name')
    version = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}version')

    toolkit_name = name.text
    toolkit_version = version.text
    return _LocalToolkit(toolkit_name, toolkit_version, path)

def _is_local_toolkit(dir):
    # Checks if dir is a toolkit (contains toolkit.xml or info.xml)
    for fn in ['toolkit.xml', 'info.xml']:
        if os.path.isfile(os.path.join(dir, fn)):
            return True
    return False

class _LocalToolkit:
    def __init__(self, name, version, path):
        self.name = name
        self.version = version
        self.path = path

def _check_satisfies_version(toolkit, dependency_range, topo):
    """ Checks if toolkit's version satisfies the dependency_range, if yes, can use it to build main app (ie add it)

    Arguments:
        toolkit {_LocalToolkit} -- A _LocalToolkit object, that we need to check if its version is equal to or is contained in dependency_range
        dependency_range {String} -- A string of the form '1.2.3' or [3.0.0,4.0.0)' that represents a version or range of versions that is acceptable
        topo {[type]} -- [description]
    """
    # Remove '.' from version, and convert it to version
    toolkit_ver = version.parse(toolkit.version)

    # Check if dependency_range is a single # or a range (single # won't contain brackets or parenthesis)
    temp = ['(', ')', '[', ']']
    if not any(x in dependency_range for x in temp):
        required_version = version.parse(dependency_range)
        if required_version == toolkit_ver:
            add_toolkit(topo, toolkit.path)
        else:
            print("Dependency {} with version {} is not the required version {}".format(toolkit.name, toolkit.version, dependency_range), file=sys.stderr)
        return

    # Dependency_range is confirmed to be a range, check if left & right bound is inclusive or exclusive
    left_inclusive = None
    right_inclusive = None

    satisfies_left = False
    satisfies_right = False

    # Check left bound
    if dependency_range[0] == '[':
        left_inclusive = True
    else:
        left_inclusive = False

    # Check right bound
    if dependency_range[-1] == ']':
        right_inclusive = True
    else:
        right_inclusive = False

    # Remove parenthesis and brackets from range and split by ',' to get lower and upper bounds, then convert to version
    # Version also handles the case '[4.5.6,5.2)' where 5.2 > 4.5.6
    bounds = re.sub('[()\[\]]', '', dependency_range).split(',')
    left_bound = version.parse(bounds[0])
    right_bound = version.parse(bounds[1])

    # Check that toolkit_ver satisfies its left and right bounds
    # if left is '[' check that version satisfies it
    if left_inclusive:
        if toolkit_ver >= left_bound:
            satisfies_left = True
    else: # left is '('
        if toolkit_ver > left_bound:
            satisfies_left = True
    # if right is ']' check that version satisfies it
    if right_inclusive:
        if toolkit_ver <= right_bound:
            satisfies_right = True
    else: # right is ')'
        if toolkit_ver < right_bound:
            satisfies_right = True

    # If toolkit.version satisfies left and right bound, then its a valid toolkit dependency, add it
    if satisfies_left and satisfies_right:
        add_toolkit(topo, toolkit.path)
    else:
        print("Dependency {} with version {} was not in range".format(toolkit.name, toolkit.version), file=sys.stderr)

def _submit_build(cmd_args, topo):
     cfg = {}
     # Only supported for remote builds.
     cfg[ConfigParams.FORCE_REMOTE_BUILD] = True
     if cmd_args.disable_ssl_verify:
         cfg[ConfigParams.SSL_VERIFY] = False
         urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
     _sc_options(cmd_args, cfg)
     submit('BUNDLE', topo, cfg)

def _sc_options(cmd_args, cfg):
    args = []
    if cmd_args.prefer_facade_tuples:
        args.append(_FACADE[0])

    if args:
        cfg[ConfigParams.SC_OPTIONS] = args

def _add_toolkits(cmd_args, topo):
    _add_cwd(cmd_args, topo)

def _add_cwd(cmd_args, topo):
    cwd = os.getcwd()
    if _is_likely_toolkit(cwd):
        add_toolkit(topo, cwd)

def _is_likely_toolkit(tkdir):
    """
    Determine if a directory looks like an SPL toolkit.
    """
    if _is_local_toolkit(tkdir):
        return True
    for dirpath, dirnames, filenames in os.walk(tkdir):
        for fn in filenames:
            if fn.endswith('.spl'):
                return True
            if fn.endswith('._cpp.cgt'):
                return True
        if '.namespace' in filenames:
            return True
        if 'native.function' in dirnames:
            return True

def _create_topo(cmd_args):
    topo,invoke = main_composite(kind=cmd_args.main_composite)
    return topo
    
def _parse_args(args):
    """ Argument parsing
    """
    cmd_parser = argparse.ArgumentParser(description='SPL compiler (sc) alias for build service.')
    cmd_parser.add_argument('--main-composite', '-M', required=True, help='SPL Main composite', metavar='name')

    cmd_parser.add_argument('--spl-path', '-t', help='Set the toolkit lookup paths. Separate multiple paths with :. Each path is a toolkit directory, a directory of toolkit directories, or a toolkitList XML file. This path overrides the STREAMS_SPLPATH environment variable.')
    cmd_parser.add_argument('--optimized-code-generation', '-a', action='store_true', help='Generate optimized code with less runtime error checking.')
    cmd_parser.add_argument('--no-optimized-code-generation', action='store_true', help='Generate non-optimized code with more runtime error checking. Do not use with the --optimized-code-generation option.')
    cmd_parser.add_argument(*_FACADE, action='store_true', help='Generate the facade tuples when it is possible.')
    _buildservice_args(cmd_parser)
    _deprecated_args(cmd_parser)

    return cmd_parser.parse_args(args)

def _buildservice_args(cmd_parser):
    rem = cmd_parser.add_argument_group('build service arguments', 'Arguments specific to use of the build service. Not supported by sc.')

    cmd_parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL verification.')

def _deprecated_args(cmd_parser):
    dep = cmd_parser.add_argument_group('deprecated arguments', 'Arguments that have been deprecated by sc and are ignored')

    dep.add_argument('--static-link', '-s', action='store_true')
    dep.add_argument('--standalone-application', '-T', action='store_true')
    dep.add_argument('--set-relax-fusion-relocatability-restartability', '-O', action='store_true')

    dep.add_argument('--checkpoint-directory', '-K', action='store', metavar='path')
    dep.add_argument('--profiling-sampling', '-S', action='store', metavar='rate')
    

if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
