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


_FACADE=['--prefer-facade-tuples', '-k']

def main(args=None):
    """ Mimics SPL SPL compiler sc against the ICP4D build service.
    """
    cmd_args = _parse_args(args)
    topo = _create_topo(cmd_args)

    # Can assume info.xml is present in cwd
    # Parse info.xml for dependencies
    dependencies = parse_dependencies()

    if cmd_args.spl_path:
        tool_kits = cmd_args.spl_path.split(':')
        # Check if any dependencies are in the passed in toolkits, if so add them
        add_local_toolkits(tool_kits, dependencies, topo)

    _add_toolkits(cmd_args, topo)
    _submit_build(cmd_args, topo)
    return 0

def parse_dependencies():
    deps = []
    root = ET.parse('info.xml').getroot()
    info = '{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}'
    common = '{http://www.ibm.com/xmlns/prod/streams/spl/common}'
    dependency_elements = root.find(info + 'dependencies').findall(info + 'toolkit')
    for dependency_element in dependency_elements:
        name = dependency_element.find(common + 'name').text
        version = dependency_element.find(common + 'version').text
        deps.append(name)
    return deps

def add_local_toolkits(toolkits, dependencies, topo):
    """ A helper function that given the local toolkit paths, and the dependencies (ie toolkits that the apps depends on),
    checks and adds the local toolkit if it is one of the dependencies

    Arguments:
        toolkits {List} -- A list of local toolkit directories, each could be a toolkit directory (has info.xml directly inside),
                or a directory consisting of toolkits (no info.xml directly inside)
        dependencies {List} -- A list consisting of the names of the dependencies of our current app
        topo {topology Object} -- [description]
    """
    local_toolkits = {} # name - path
    #  For each path, need to check if its a SPL toolkit (has info.xml directly inside) or a directory consisting of toolkits (no info.xml directly inside)
    for x in toolkits:
        if _is_toolkit_(x):
            local_toolkits[_get_toolkit_name(x)] = x
        else:
            # directory consisting of toolkits (no info.xml directly inside), get list and check which are local_toolkits, add them
            toolkit_dirs = glob(x + "*/")
            for y in toolkit_dirs:
                if _is_toolkit_(y):
                    local_toolkits[_get_toolkit_name(y)] = y

    # Have all local toolkits from the passed in locations
    for toolkit_name, toolkit_path in local_toolkits.items():
        print(toolkit_name)
        if toolkit_name in dependencies:
            add_toolkit(topo, toolkit_path)

    exit()

def _get_toolkit_name(path):
    root = ET.parse(path + "/info.xml").getroot()
    identity = root.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}identity')
    name = identity.find('{http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo}name')
    toolkit_name = name.text
    return toolkit_name

def _is_toolkit_(tkdir):
    # Checks if tkdir is a toolkit (return True) or if it isn't or is directory containing multiple toolkit directories (returns)
    for fn in ['toolkit.xml', 'info.xml']:
        if os.path.isfile(os.path.join(tkdir, fn)):
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
    if _is_toolkit(cwd):
        add_toolkit(topo, cwd)

def _is_toolkit(tkdir):
    """
    Determine if a directory looks like an SPL toolkit.
    """
    for fn in ['toolkit.xml', 'info.xml']:
        if os.path.isfile(os.path.join(tkdir, fn)):
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
