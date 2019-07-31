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
from streamsx.build import BuildService
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

def _add_local_toolkits(toolkit_paths, dependencies, topo):
    """ A helper function that given the local toolkit paths, and the dependencies (ie toolkits that the apps depends on),
    first checks whether the dependency w/ correct version already exists on the remote buildserver, and if not checks
    whether the dependency w/ correct version exists locally, and if so adds it

    Arguments:
        toolkit_paths {List} -- A list of local toolkit directories, each could be a toolkit directory (has info.xml directly inside),
                or a directory consisting of toolkits (no info.xml directly inside)
        dependencies {Dictionary} -- A dictionary consisting of elements where the key is the name of the dependency, and the value is its version.
        topo {topology Object} -- [description]
    """
    sc = BuildService.of_endpoint(verify=False)

    local_toolkits = _get_all_local_toolkits(toolkit_paths)

    # Iterate through all dependencies (toolkits) needed to build sab file, and check if the dependency already exist on the build server
    for dependency_name, dependency_version in dependencies.items():
        if _check_toolkit_on_buildserver(dependency_name, dependency_version, sc):
            # Dependency w/ correct version is already on buildserver, thus don't need to do anything
            continue
        # Dependency w/ correct version not in buildserver, check locally
        else:
            toolkit = _get_local_toolkit(dependency_name, dependency_version, local_toolkits)
            if toolkit:
                # Dependency w/ correct version exists locally, add it
                add_toolkit(topo, toolkit)

def _get_all_local_toolkits(toolkit_paths):
    """ A helper function that given the local toolkit paths, creates and returns a list of local toolkit objects

    Arguments:
        toolkit_paths {List} -- A list of local toolkit directories, each could be a toolkit directory (has info.xml directly inside),
            or a directory consisting of toolkits (no info.xml directly inside)

    Returns:
        local_toolkits {List} -- list of local toolkit objects
    """

    local_toolkits_paths = [] # toolkit paths
    local_toolkits = [] # toolkit objects

    #  For each path, check if its a SPL toolkit (has info.xml directly inside)
    #  or a directory consisting of toolkits (no info.xml directly inside)
    for x in toolkit_paths:
        if _is_local_toolkit(x):
            local_toolkits_paths.append(x)
        else:
            # directory consisting of toolkits, get list and check which are local_toolkits, add them
            sub_directories = glob(x + "*/")
            for y in sub_directories:
                if _is_local_toolkit(y):
                    local_toolkits_paths.append(y)

    # For each direct path to a toolkit, parse the name & version, and convert it to a _LocalToolkit object
    for tk_path in local_toolkits_paths:
        # Get the name & version of the toolkit that is in the directory tk_path
        root = ET.parse(tk_path + "/info.xml").getroot()
        identity = root.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}identity')
        name = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}name')
        version = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}version')

        toolkit_name = name.text
        toolkit_version = version.text

        tk = _LocalToolkit(toolkit_name, toolkit_version, tk_path)
        local_toolkits.append(tk)

    return local_toolkits

def _get_local_toolkit(dependency_name, dependency_version, local_toolkits):
    """ Helper function that given a required dependency, and a list of local toolkits, finds and returns the local toolkit of the same name,
    if it satisfies the required dependency version, else returns None.

    Arguments:
        dependency_name {String} -- The name of the required dependency
        dependency_version {String} -- The version # or version range of the required dependency
        local_toolkits {List} -- A list of _LocalToolkit objects, giving all local toolkits

    Returns:
        _LocalToolkit -- The matching local toolkit, or None if it doesn't exist
    """
    # Get all local toolkits that have the same name as our dependency
    temp = [x for x in local_toolkits if x.name == dependency_name]
    # For each toolkit that matches the name
    for x in temp:
        if _check_correct_version(x, dependency_version):
            return x

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

def _get_remote_toolkits(sc, name):
    # Find all toolkits on the buildserver matching a toolkit name
    # name and version will be returned.
    toolkits = sc.get_toolkits()
    return [toolkit for toolkit in toolkits if toolkit.name == name]

def _check_correct_version(toolkit, dependency_range):
    """ Checks if toolkit's version satisfies the dependency_range, if yes, return true, else false

    Arguments:
        toolkit {_LocalToolkit} -- A _LocalToolkit object, that we need to check if its version is equal to or is contained in dependency_range
        dependency_range {String} -- A string of the form '1.2.3' or [3.0.0,4.0.0)' that represents a version or range of versions that is acceptable
    """
    # Convert it to version
    toolkit_ver = version.parse(toolkit.version)

    # Check if dependency_range is a single # or a range (single # won't contain brackets or parenthesis)
    temp = ['(', ')', '[', ']']
    if not any(x in dependency_range for x in temp):
        required_version = version.parse(dependency_range)
        if required_version == toolkit_ver:
            return True
        return False

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
        return True
    return False

def _check_toolkit_on_buildserver(dependency_name, dependency_version, sc):
    """ Helper function that given a required dependency, checks if it already exists on the remote buildserver

    Arguments:
        dependency_name {String} -- The name of the required dependency
        dependency_version {String} -- The version # or version range of the required dependency
        sc {[type]} -- [description]

    Returns:
        [Boolean] -- True if it exists remotely, False if it doesn't
    """
    # Check if toolkit/dependency already exists remotely in buildserver w/ correct version
    remote_toolkits = _get_remote_toolkits(sc, dependency_name)
    for rmt_tk in remote_toolkits:
        if _check_correct_version(rmt_tk, dependency_version):
            # Dependency w/ correct version is already in buildserver, don't need to do anything
            return True
    return False


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
