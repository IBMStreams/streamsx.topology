# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

from __future__ import print_function
from future.builtins import *

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
    if cmd_args.main_composite is not None:
        app = _get_spl_app(cmd_args)
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
    ctx_group.add_argument('--create-bundle', action='store_true', help='Create a bundle')

    app_group = cmd_parser.add_mutually_exclusive_group(required=True)
    app_group.add_argument('--topology', help='Topology to call')
    app_group.add_argument('--main-composite', help='SPL main composite')

    cmd_parser.add_argument('--toolkits', nargs='+', help='Additional SPL toolkits')
    cmd_parser.add_argument('--job-name', help='Job name')
    cmd_parser.add_argument('--preload', action='store_true', help='Preload job onto all resources in the instance')
    cmd_parser.add_argument('--trace', choices=['error', 'warn', 'info', 'debug', 'trace'], help='Application trace level')

    cmd_parser.add_argument('--submission-parameters', '-p', nargs='+', action=_SubmitParamArg, help="Submission parameters as name=value pairs")

    cmd_args = cmd_parser.parse_args()
    return cmd_args

def _get_topology_app(cmd_args):
    mn, fn = cmd_args.topology.rsplit('.', 1)
    fm = importlib.import_module(mn)
    tf = getattr(fm, fn)
    app = tf()
    if isinstance(app, Topology):
        app = (app, {})
    elif not isinstance(app, tuple):
        raise ValueError(app)
    if not isinstance(app[0], Topology):
        raise ValueError(app)
    if isinstance(app[1], ctx.JobConfig):
        cfg = {}
        cfg[ctx.ConfigParams.JOB_CONFIG] = app[1]
        app = (app[0], cfg)
    elif not isinstance(app[1], dict):
        raise ValueError(app)

    return app

def _get_spl_app(cmd_args):
    ns, name = cmd_args.main_composite.rsplit('::', 1)
    ns += '._spl'
    topo = Topology(name=name, namespace=ns)
    if cmd_args.toolkits is not None:
        for tk_path in cmd_args.toolkits:
            tk.add_toolkit(topo, tk_path)

    op.Invoke(topo, cmd_args.main_composite)
    return (topo, {})

def _submit(cmd_args, app):
    cfg = app[1]
    if cmd_args.create_bundle:
        ctxtype = ctx.ContextTypes.BUNDLE
    elif cmd_args.service_name:
        cfg[ctx.ConfigParams.FORCE_REMOTE_BUILD] = True
        cfg[ctx.ConfigParams.SERVICE_NAME] = cmd_args.service_name
        ctxtype = ctx.ContextTypes.STREAMING_ANALYTICS_SERVICE
    sr = ctx.submit(ctxtype, app[0], cfg)
    return sr

def _job_config_args(cmd_args, app):
    cfg = app[1]
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
    main()
