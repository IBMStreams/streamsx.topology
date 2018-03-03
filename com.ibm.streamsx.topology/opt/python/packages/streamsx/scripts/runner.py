# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

from __future__ import print_function
from future.builtins import *

import collections
import sys
import sysconfig
import inspect
import imp
import glob
import os
import shutil
import argparse
import subprocess
import importlib
import streamsx.topology.context as ctx
from streamsx.topology.topology import Topology
import streamsx.spl.toolkit as tk
import streamsx.spl.op as op
import streamsx.rest
import streamsx.scripts.extract

# Structure for an application
# app - Topology object or str (sab file)
# cfg - dict configuration holding JobConfig at least.
_App = collections.namedtuple('_App', ['app', 'cfg'])

class _SubmitParamArg(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        sp = dict()
        setattr(namespace, 'submission_parameters', sp)
        for nvp in values:
            name,value = nvp.split('=', maxsplit=1)
            sp[name] = value
        

def main():
    cmd_args = _parse_args()
    if cmd_args.topology is not None:
        app = _get_topology_app(cmd_args)
    elif cmd_args.main_composite is not None:
        app = _get_spl_app(cmd_args)
    elif cmd_args.bundle is not None:
        app = _get_bundle(cmd_args)
    _job_config_args(cmd_args, app)
    sr = _submit(cmd_args, app)
    print(sr)
    return sr


def _parse_args():
    """ Argument parsing
    """
    cmd_parser = argparse.ArgumentParser(description='Execute a Streams application using a Streaming Analytics service.')

    ctx_group = cmd_parser.add_mutually_exclusive_group(required=True)
    ctx_group.add_argument('--service-name', help='Submit to Streaming Analytics service')
    ctx_group.add_argument('--create-bundle', action='store_true', help='Create a bundle using a local IBM Streams install. No job submission occurs.')

    app_group = cmd_parser.add_mutually_exclusive_group(required=True)
    app_group.add_argument('--topology', help='Topology to call')
    app_group.add_argument('--main-composite', help='SPL main composite')
    app_group.add_argument('--bundle', help="Streams application bundle (sab file) to submit to service")

    bld_group = cmd_parser.add_argument_group('Build options', 'Application build options')
    bld_group.add_argument('--toolkits', nargs='+', help='SPL toolkit containing the main composite and any other required SPL toolkits.')

    jo_group = cmd_parser.add_argument_group('Job options', 'Job configuration options')

    jo_group.add_argument('--job-name', help='Job name')
    jo_group.add_argument('--preload', action='store_true', help='Preload job onto all resources in the instance')
    jo_group.add_argument('--trace', choices=['error', 'warn', 'info', 'debug', 'trace'], help='Application trace level')

    jo_group.add_argument('--submission-parameters', '-p', nargs='+', action=_SubmitParamArg, help="Submission parameters as name=value pairs")

    cmd_args = cmd_parser.parse_args()
    return cmd_args

def _get_topology_app(cmd_args):
    mn, fn = cmd_args.topology.rsplit('.', 1)
    fm = importlib.import_module(mn)
    tf = getattr(fm, fn)
    app = tf()
    if isinstance(app, Topology):
        app = _App(app, {})
    elif not isinstance(app, tuple):
        raise ValueError(app)
    if not isinstance(app[0], Topology):
        raise ValueError(app)
    if isinstance(app[1], ctx.JobConfig):
        cfg = {}
        cfg[ctx.ConfigParams.JOB_CONFIG] = app[1]
        app = _App(app[0], cfg)
    elif not isinstance(app[1], dict):
        raise ValueError(app)

    return app

def _get_spl_app(cmd_args):
    if '::' in cmd_args.main_composite:
        ns, name = cmd_args.main_composite.rsplit('::', 1)
        ns += '._spl'
    else:
        raise ValueError('--main-composite requires a namespace qualified name: ' + str(cmd_args.main_composite))
    topo = Topology(name=name, namespace=ns)
    if cmd_args.toolkits is not None:
        for tk_path in cmd_args.toolkits:
            tk.add_toolkit(topo, tk_path)
            if cmd_args.create_bundle:
                # Mimic what the build service does by indexing
                # any required toolkits including Python operator extraction
                # but only if we can write to it.
                if os.access(tk_path, os.W_OK):
                    streamsx.scripts.extract.main(['-i', tk_path, '--make-toolkit'])

    op.Invoke(topo, cmd_args.main_composite)
    return _App(topo, {})

def _get_bundle(cmd_args):
    return _App(cmd_args.bundle, {})

def _submit(cmd_args, app):
    if isinstance(app.app, Topology):
        return _submit_topology(cmd_args, app)
    if isinstance(app.app, str):
        return _submit_bundle(cmd_args, app)
    
def _submit_topology(cmd_args, app):
    """Submit a Python topology to the service.
    This includes an SPL main composite wrapped in a Python topology.
    """
    cfg = app.cfg
    if cmd_args.create_bundle:
        ctxtype = ctx.ContextTypes.BUNDLE
        cfg['topology.keepArtifacts'] = True
    elif cmd_args.service_name:
        cfg[ctx.ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ctx.ConfigParams.SERVICE_NAME] = cmd_args.service_name
        ctxtype = ctx.ContextTypes.STREAMING_ANALYTICS_SERVICE
    sr = ctx.submit(ctxtype, app.app, cfg)
    return sr

def _submit_bundle(cmd_args, app):
    """Submit an existing bundle to the service"""
    sac = streamsx.rest.StreamingAnalyticsConnection(service_name=cmd_args.service_name)
    sas = sac.get_streaming_analytics()
    sr = sas.submit_job(bundle=app.app, job_config=app.cfg[ctx.ConfigParams.JOB_CONFIG])
    if 'exception' in sr:
        rc = 1
    elif 'status_code' in sr:
        try:
            rc = 0 if int(sr['status_code'] == 200) else 1
        except:
            rc = 1
    elif 'id' in sr or 'jobId' in sr:
        rc = 0
    sr['return_code'] = rc
    return sr

def _job_config_args(cmd_args, app):
    cfg = app.cfg
    if not ctx.ConfigParams.JOB_CONFIG in cfg:
        ctx.JobConfig().add(cfg)
    jc = cfg[ctx.ConfigParams.JOB_CONFIG]
    if cmd_args.job_name:
        jc.job_name = str(cmd_args.job_name)
    if cmd_args.preload:
        jc.preload = True
    if cmd_args.trace:
        jc.tracing = cmd_args.trace
    if cmd_args.submission_parameters:
        jc.submission_parameters.update(cmd_args.submission_parameters)
    
if __name__ == '__main__':
    sr = main()
    if 'return_code' in sr:
        sys.exit(int(sr['return_code']))
    sys.exit(1)
