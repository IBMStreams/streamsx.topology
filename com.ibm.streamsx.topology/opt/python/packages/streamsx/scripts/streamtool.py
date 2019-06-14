# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

from __future__ import print_function
from future.builtins import *

import sys
import sysconfig
import os
import argparse
import requests
import warnings
import urllib3
import time

import streamsx.topology.context
from streamsx.rest import Instance

###########################################
# submitjob
###########################################
def _submitjob_parser(subparsers):
    job_submit = subparsers.add_parser('submitjob', help='Submit an application bundle')
    job_submit.add_argument('sabfile', help='Location of sab file.', metavar='sab-pathname')

def _submitjob(instance, cmd_args):
    """Submit a job."""
    instance.submit_job(bundle=cmd_args.sabfile)

###########################################
# canceljob
###########################################
def _canceljob_parser(subparsers):
    job_cancel = subparsers.add_parser('canceljob', help='Cancel a job.')
    job_cancel.add_argument('--jobs', '-j', help='Specifies a list of job IDs.', metavar='job-id')
    job_cancel.add_argument('--force', action='store_true', help='Stop the service even if jobs are running.')

def _canceljob(instance, cmd_args):
    """Cancel a job."""
    print('DDD', cmd_args)
    for job in cmd_args.jobs.split(','):
        _job_cancel(instance, job_id=int(job), force=cmd_args.force)

def _job_cancel(instance, job_id=None, job_name=None, force=False):
    job = instance.get_job(id=str(job_id))
    job.cancel(force)


###########################################
# lsjobs
###########################################
def _lsjobs_parser(subparsers):
    job_ls = subparsers.add_parser('lsjobs', help='List the jobs for a given instance')

def _lsjobs(instance, cmd_args):
    """view jobs"""
    jobs = instance.get_jobs()
    print("Instance: " + instance.id)
    print("Id State Healthy User Date Name Group \n")

    for job in jobs:
        jobtime = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job.submitTime/1000))) # job.submitTime/1000 to convert ms to sec
        jobG = job.jobGroup.split("/")[-1]
        print(job.id + " " + job.status.capitalize() + " " + job.health + " " + job.startedBy + " " + jobtime + " " + job.name + " " + jobG + " ")


def run_cmd(args=None):
    cmd_args = _parse_args(args)

    if cmd_args.disable_ssl_verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    instance = Instance.of_endpoint(
        verify=False if cmd_args.disable_ssl_verify else None)

    # if cmd_args.subcmd == 'submitjob':
    #     result = _submitjob(instance, cmd_args)
    # elif cmd_args.subcmd == 'canceljob':
    #     result = _canceljob(instance, cmd_args)

    switch = {
    "submitjob": _submitjob,
    "canceljob": _canceljob,
    "lsjobs": _lsjobs,
    }

    return switch[cmd_args.subcmd](instance, cmd_args)
    # return result

def main(args=None):
    """ Mimic streamtool using the REST api for ICP4D.
    """
    streamsx._streams._version._mismatch_check('streamsx.topology.context')
    try:
        sr = run_cmd(args)
        sr['return_code'] = 0
    except:
        sr = {'return_code':1, 'error': sys.exc_info()}
    return sr

def _parse_args(args):
    """ Argument parsing
    """
    cmd_parser = argparse.ArgumentParser(description='Control commands for a Streaming Analytics service.')
    cmd_parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL verification.')

    subparsers = cmd_parser.add_subparsers(help='Supported commands', dest='subcmd')

    _submitjob_parser(subparsers)
    _canceljob_parser(subparsers)
    _lsjobs_parser(subparsers)

    return cmd_parser.parse_args(args)


if __name__ == '__main__':
    sr = main()
    rc = sr['return_code']
    del sr['return_code']
    if rc == 0:
        print(sr)
    else:
        print(sr['error'][1], file=sys.stderr)
    sys.exit(rc)
