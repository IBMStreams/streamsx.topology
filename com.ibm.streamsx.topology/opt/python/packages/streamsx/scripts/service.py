# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

from __future__ import print_function
from future.builtins import *

import sys
import sysconfig
import os
import argparse
import streamsx.rest

def _stop(sas, cmd_args):
    """Stop the service if no jobs are running unless force is set"""
    if not cmd_args.force:
        status = sas.get_instance_status()
        jobs = int(status['job_count'])
        if jobs:
            return status
    return sas.stop_instance()

def run_cmd(args=None):
    cmd_args = _parse_args(args)
    sc = streamsx.rest.StreamingAnalyticsConnection(service_name=cmd_args.service_name)
    sas = sc.get_streaming_analytics()
    if cmd_args.subcmd == 'start':
        result = sc.get_streaming_analytics().start_instance()
    elif cmd_args.subcmd == 'stop':
        result = _stop(sas, cmd_args)
    elif cmd_args.subcmd == 'status':
        result = sc.get_streaming_analytics().get_instance_status()

    if not cmd_args.full_response:
        return {k: result[k] for k in ('state', 'status', 'job_count')}
    return result

   
def main(args=None):
    """ Performs an action against a Streaming Analytics service.
    """
    try:
        json = run_cmd(args)
        print(json)
        return 0
    except:
        print(sys.exc_info()[1], file=sys.stderr)
        return 1

def _parse_args(args):
    """ Argument parsing
    """
    cmd_parser = argparse.ArgumentParser(description='Control commands for a Streaming Analytics service.')
    cmd_parser.add_argument('--service-name', help='Streaming Analytics service name')
    cmd_parser.add_argument('--full-response', action='store_true', help='Print the full JSON response.')

    subparsers = cmd_parser.add_subparsers(help='Supported commands', dest='subcmd')

    parser_start = subparsers.add_parser('start', help='Start the service instance')
    parser_status = subparsers.add_parser('status', help='Get the service status.')
    parser_stop = subparsers.add_parser('stop', help='Stop the instance for the service.')
    parser_stop.add_argument('--force', action='store_true', help='Stop the service even if jobs are running.')

    #cmd_parser.add_argument('subcmd', choices=['start', 'stop', 'status'])

    return cmd_parser.parse_args(args)

if __name__ == '__main__':
    sys.exit(main())
