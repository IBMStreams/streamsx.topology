# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import sys
import sysconfig
import os
import argparse
import urllib3
import shutil
import streamsx.rest
import pkg_resources
from streamsx.spl.op import _main_composite
from streamsx.spl.toolkit import add_toolkit
from streamsx.topology.context import submit, ConfigParams
from streamsx.build import BuildService
import xml.etree.ElementTree as ET
from glob import glob
import re
from io import StringIO


_FACADE=['--prefer-facade-tuples', '-k']

def main(args=None):
    """ Mimics SPL compiler sc against the ICP4D build service.
    """
    cmd_args = _parse_args(args)
    topo = _create_topo(cmd_args)

    # Add main composite toolkit
    _add_toolkits(cmd_args, topo)

    # Get dependencies for app, if any at all
    dependencies = _parse_dependencies()
    # if dependencies and if -t arg, find & add local toolkits
    spl_path = cmd_args.spl_path
    if spl_path is None:
        spl_path = os.environ.get('STREAMS_SPLPATH')
    if dependencies and spl_path:
        tool_kits = spl_path.split(':')
        # Check if any dependencies are in the passed in toolkits, if so add them
        _add_local_toolkits(tool_kits, dependencies, topo, verify_arg = False if cmd_args.disable_ssl_verify else None)

    sr = _submit_build(cmd_args, topo)
    return _move_bundle(cmd_args, sr)

def _parse_dependencies():
    # Parse info.xml for the dependencies of the app you want to build sab file for
    deps = {} # name - version
    if os.path.isfile('info.xml'):
        # Open XML file and strip all namespaces from XML tags
        # Ex. <info:dependencies> -> <dependencies>
        XML_data = None
        with open('info.xml', 'r') as file:
            XML_data = file.read()
        it = ET.iterparse(StringIO(XML_data))
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]
        root = it.root

        # Get dependency tag
        dependency_elements = root.findall('dependencies')
        assert len(dependency_elements) == 1, 'Should only contain 1 dependency tag'

        # Get toolkits tags
        dependencies = dependency_elements[0].findall('toolkit')
        for toolkit in dependencies:
            name = toolkit.find('name').text
            version = toolkit.find('version').text
            deps[name] = version
    return deps

def _add_local_toolkits(toolkit_paths, dependencies, topo, verify_arg):
    """ A helper function that given the local toolkit paths, and the dependencies (ie toolkits that the apps depends on),
    finds the latest dependency w/ correct version (could be local or on the buildserver) and checks whether it exists locally, and if so adds it.
    If the latest dependency w/ correct version is located on the buildserver, don't need to do anything

    Arguments:
        toolkit_paths {List} -- A list of local toolkit directories, each could be a toolkit directory (has info.xml directly inside),
                or a directory consisting of toolkits (no info.xml directly inside)
        dependencies {Dictionary} -- A dictionary consisting of elements where the key is the name of the dependency, and the value is its version.
        topo {topology Object} -- [description]
        verify_arg {type} -- Disable SSL verification.
    """
    build_server = BuildService.of_endpoint(verify=verify_arg)

    local_toolkits = _get_all_local_toolkits(toolkit_paths)
    remote_toolkits = build_server.get_toolkits()

    all_toolkits = local_toolkits + remote_toolkits

    # Iterate through all dependencies (toolkits) needed to build sab file, and check if the dependency already exist on the build server
    for dependency_name, dependency_version in dependencies.items():

        # All the toolkits that match dependency_name regardless of version
        matching_toolkits = [toolkit for toolkit in all_toolkits if toolkit.name == dependency_name]

        if not matching_toolkits:
            # No toolkits exists on the remote buildserver or locally that can satsify this dependency, continue and let SPL compiler throw error
            continue

        # Toolkit with the highest verion that still satisfies the dependency_version
        latest_compatible_toolkit = None

        # Go through all toolkits, find the highest/latest version, given by latest_compatible_toolkit
        for tk in matching_toolkits:
            # Check if current toolkit tk satisfies the version requirement
            if _check_correct_version(tk, dependency_version):
                # if we have a toolkit that already satisfies the version requirement, and tk also satisfies its
                # compare tk against the highest version we have seen thus far
                if latest_compatible_toolkit:
                    latest_version_so_far = pkg_resources.parse_version(latest_compatible_toolkit.version)
                    curr_tk_version = pkg_resources.parse_version(tk.version)

                    # if it is a later version, update our toolkit
                    if curr_tk_version > latest_version_so_far:
                        latest_compatible_toolkit = tk
                else:
                    latest_compatible_toolkit = tk

        # Check if latest_compatible_toolkit is local, bc we then need to add it
        if isinstance(latest_compatible_toolkit, _LocalToolkit):
            # Latest toolkit is local, upload it
            add_toolkit(topo, latest_compatible_toolkit.path)
            # print("Latest dependency {} with version {} is not on buildserver, adding it".format(latest_compatible_toolkit.name, latest_compatible_toolkit.version))
        # latest_compatible_toolkit exists on buildserver, thus don't need to do anything

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
        path = os.path.join(x, '') # Adds a trailing '/' if not already present
        if _is_local_toolkit(path):
            local_toolkits_paths.append(path)
        else:
            # directory consisting of toolkits, get list and check which are local_toolkits, add them
            sub_directories = glob(path + "*/") # All values in sub_directories will have a trailing '/'
            for y in sub_directories:
                if _is_local_toolkit(y):
                    local_toolkits_paths.append(y)

    # For each direct path to a toolkit, parse the name & version, and convert it to a _LocalToolkit object
    for tk_path in local_toolkits_paths:
        if os.path.isfile(tk_path + 'info.xml'):
            # Get the name & version of the toolkit that is in the directory tk_path
            root = ET.parse(tk_path + 'info.xml').getroot()
            identity = root.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}identity')
            name = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}name')
            version = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}version')

            toolkit_name = name.text
            toolkit_version = version.text

            tk = _LocalToolkit(toolkit_name, toolkit_version, tk_path)
            local_toolkits.append(tk)

    return local_toolkits

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

def _check_correct_version(toolkit, dependency_range):
    """ Checks if toolkit's version satisfies the dependency_range, if yes return true, else false

    Arguments:
        toolkit {_LocalToolkit} -- A _LocalToolkit object, that we need to check if its version is equal to or is contained in dependency_range
        dependency_range {String} -- A string of the form '1.2.3' or [3.0.0,4.0.0)' that represents a version or range of versions that is acceptable
    """
    # Convert it to version
    toolkit_ver = pkg_resources.parse_version(toolkit.version)

    # Check if dependency_range is a single # or a range (single # won't contain brackets or parenthesis)
    temp = ['(', ')', '[', ']']
    if not any(x in dependency_range for x in temp):
        required_version = pkg_resources.parse_version(dependency_range)
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
    left_bound = pkg_resources.parse_version(bounds[0])
    right_bound = pkg_resources.parse_version(bounds[1])

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

    # If toolkit.version satisfies left and right bound, then its a valid toolkit dependency, return True
    if satisfies_left and satisfies_right:
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
     return submit('BUNDLE', topo, cfg)

def _move_bundle(cmd_args, sr):
    out_dir = cmd_args.output_directory
    if not out_dir:
        out_dir = 'output'
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    if 'jobConfigPath' in sr:
        jco = sr['jobConfigPath']
        shutil.move(jco, os.path.join(out_dir, os.path.basename(jco)))
    if not 'bundlePath' in sr:
        return 1
    sab = sr['bundlePath']
    shutil.move(sab, os.path.join(out_dir, os.path.basename(sab)))
    return 0

def _sc_options(cmd_args, cfg):
    args = []
    if cmd_args.prefer_facade_tuples:
        args.append(_FACADE[0])

    if cmd_args.ld_flags:
        args.append('--ld-flags=' + str(cmd_args.ld_flags))
    if cmd_args.cxx_flags:
        args.append('--cxx-flags=' + str(cmd_args.cxx_flags))
    if cmd_args.cppstd:
        args.append('--c++std=' + str(cmd_args.cppstd))
    if cmd_args.data_directory:
        args.append('--data-directory=' + str(cmd_args.data_directory))
    if cmd_args.compile_time_args: # sc -M my::App hello=a,b,c foo=bar -> compile_time_args = ['hello=a,b,c', 'foo=bar']
        # Check if '=' is NOT present in the compile_time_args, this implies that we have a .splmm file
        if any('=' not in arg for arg in cmd_args.compile_time_args):
            # If we have both SPLMM args and SPL CompileTimeValue args, then main composite is a .splmm file
            # _SPLMM_OPTIONS Should contain both regular SPL compile_time_args and SPLMM args to preserve ordering
            cfg[ConfigParams._SPLMM_OPTIONS] = cmd_args.compile_time_args
            # Just the SPL CompileTimeValue args
            args.extend([x for x in cmd_args.compile_time_args if '=' in x])
        else:
            # No SPLMM args, so compiling a regular .spl file
            args.extend(cmd_args.compile_time_args)
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
            if fn.endswith('.splmm'):
                return True
            if fn.endswith('._cpp.cgt'):
                return True
        if '.namespace' in filenames:
            return True
        if 'native.function' in dirnames:
            return True

def _create_topo(cmd_args):
    # the private function permits main composites w/o a namespace
    topo,invoke = _main_composite(kind=cmd_args.main_composite)
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

    cmd_parser.add_argument('compile_time_args', help='arguments that are passed in at compile time.', nargs='*', metavar='compile-time-args')
    cmd_parser.add_argument('--ld-flags', '-w', help='Pass the specified flags to ld while linking occurs.')
    cmd_parser.add_argument('--cxx-flags', '-x', help='Pass the specified flags to the C++ compiler during the build.')
    cmd_parser.add_argument('--c++std', help='Specify the language level for the underlying C++ compiles.', dest='cppstd')
    cmd_parser.add_argument('--data-directory', help='Specifies the location of the data directory to use.')
    cmd_parser.add_argument('--output-directory', help='Specifies a directory where the application artifacts are placed.')

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
