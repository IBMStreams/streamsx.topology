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
import datetime

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
    job_ls.add_argument('--jobs', '-j', help='Specifies a list of job IDs.', metavar='job-id')
    job_ls.add_argument('--users', '-u')
    job_ls.add_argument('--jobnames')

def _lsjobs(instance, cmd_args):
    """view jobs"""
    jobs = instance.get_jobs()

    # If --users argument (ie given list of user ID's), filter jobs by these user ID's
    if (cmd_args.users):
        users = cmd_args.users.split(",")
        jobs = [job for job in jobs if job.startedBy in users]

    # If --jobs argument (ie given list of job ID's), filter jobs by these ID's
    if (cmd_args.jobs):
        job_ids = cmd_args.jobs.split(",")
        jobs = [job for job in jobs if job.id in job_ids]

    # If --jobsnames argument (ie given list of job names), filter jobs by these user job names
    if (cmd_args.jobnames):
        job_names = cmd_args.jobnames.split(",")
        jobs = [job for job in jobs if job.name in job_names]

    print("Instance: " + instance.id)
    print("Id State Healthy User Date Name Group")
    LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    for job in jobs:
        jobHealth = "yes" if job.health == "healthy" else "no"
        jobTime = datetime.datetime.fromtimestamp(job.submitTime/1000).replace(tzinfo=LOCAL_TIMEZONE).isoformat() # job.submitTime/1000 to convert ms to sec
        jobGroup = job.jobGroup.split("/")[-1]
        # jobGroup = job.jobGroup
        print(job.id + " " + job.status.capitalize() + " " + jobHealth + " " + job.startedBy + " " + jobTime + " " + job.name + " " + jobGroup)


def run_cmd(args=None):
    cmd_args = _parse_args(args)

    if cmd_args.disable_ssl_verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    instance = Instance.of_endpoint(
        verify=False if cmd_args.disable_ssl_verify else None)

    switch = {
    "submitjob": _submitjob,
    "canceljob": _canceljob,
    "lsjobs": _lsjobs,
    }

    return switch[cmd_args.subcmd](instance, cmd_args)

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
