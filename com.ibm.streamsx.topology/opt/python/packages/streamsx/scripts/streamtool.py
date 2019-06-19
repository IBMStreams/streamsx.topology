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
import json

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
    job_ls.add_argument('--users', '-u', help='Specifies to select from this list of user IDs')
    job_ls.add_argument('--jobnames', help='Specifies a list of job names')

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
    print('{: <5} {:<10} {:<10} {:<10} {:<30} {:<40} {:<20}'.format("Id", "State", "Healthy", "User", "Date", "Name", "Group"))
    LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    for job in jobs:
        jobHealth = "yes" if job.health == "healthy" else "no"
        jobTime = datetime.datetime.fromtimestamp(job.submitTime/1000).replace(tzinfo=LOCAL_TIMEZONE).isoformat() # job.submitTime/1000 to convert ms to sec
        jobGroup = job.jobGroup.split("/")[-1]
        # jobGroup = job.jobGroup
        print('{: <5} {:<10} {:<10} {:<10} {:<30} {:<40} {:<20}'.format(job.id, job.status.capitalize(), jobHealth, job.startedBy, jobTime, job.name, jobGroup))


###########################################
# appconfig
###########################################

# ls-appconfig
def _lsappconfig_parser(subparsers):
    appconfig_ls = subparsers.add_parser('lsappconfig', help='Retrieve a list of configurations for making a connection to an external application')
    appconfig_ls.add_argument('--fmt', help='Specifies the presentation format')


def _lsappconfig(instance, cmd_args):
    """view appconfigs"""
    configs = instance.get_application_configurations()
    config_format = '%Tf'

    if (cmd_args.fmt): # either %Tf, %Mf, or %Nf, defaults to %Tf
        config_format = cmd_args.fmt

    if (config_format == "%Tf"): # Format for Tf
        print('{: <20} {:<20} {:<30} {:<30} {:<20}'.format("Id", "Owner", "Created", "Modified", "Description"))

    for config in configs:
        createDate = datetime.datetime.fromtimestamp(config.creationTime/1000).strftime("%m/%d/%Y, %I:%M %p ") + "GMT"
        lastModifiedDate = datetime.datetime.fromtimestamp(config.lastModifiedTime/1000).strftime("%m/%d/%Y, %I:%M %p ") + "GMT"

        if (config_format == "%Mf"): # Format for Mf
            print("=================================================")
            print('{: <15} : {: <20}'.format("Id", config.name))
            print('{: <15} : {: <20}'.format("Owner", config.owner))
            print('{: <15} : {: <20}'.format("Created", createDate))
            print('{: <15} : {: <20}'.format("Modified", lastModifiedDate))
            print('{: <15} : {: <20}'.format("Description",  config.description))

        elif (config_format == "%Nf"):
            print('Id: {: <20} Owner: {:<20} Created: {:<30} Modified: {:<30} Description: {:<20}'.format(config.name, config.owner, createDate, lastModifiedDate, config.description))
        elif (config_format == "%Tf"): # Format for Tf
            print('{: <20} {:<20} {:<30} {:<30} {:<20}'.format(config.name, config.owner, createDate, lastModifiedDate, config.description))

    if (config_format == "%Mf"):
            print("=================================================")

# rm-appconfig
def _rmappconfig_parser(subparsers):
    appconfig_rm = subparsers.add_parser('rmappconfig', help='Removes a configuration that is used for making a connection to an external application')
    appconfig_rm.add_argument('config_name', help='Name of the app config')
    appconfig_rm.add_argument('--noprompt', help='Specifies to suppress confirmation prompts.', action='store_true')



def _rmappconfig(instance, cmd_args):
    """remove an appconfig"""
    config_name = cmd_args.config_name
    configs = instance.get_application_configurations(name = config_name)
    if (not configs):
         print("No application configuration by the name {}".format(config_name))
    app_config = instance.get_application_configurations(name = config_name)[0]
    # No confirmation required, delete
    if (cmd_args.noprompt):
        app_config.delete()
    else:
        response = input("Do you want to remove the application configuration {} from the {} instance? Enter 'y' to continue or 'n' to cancel: ".format(config_name, instance.id))
        if (response == "y"):
            app_config.delete()


# mk-appconfig
def _mkappconfig_parser(subparsers):
    appconfig_mk = subparsers.add_parser('mkappconfig', help='Creates a configuration that enables connection to an external application')
    appconfig_mk.add_argument('config_name', help='Name of the app config')
    appconfig_mk.add_argument('--property', action='append', help='Specifies a property name and value pair to add to or change in the configuration')
    appconfig_mk.add_argument('--propfile', help='Specifies the path to a file that contains a list of application configuration properties for connecting to an external application')
    appconfig_mk.add_argument('--description', help='Specifies a description for the application configuration')

def _mkappconfig(instance, cmd_args):
    """create an appconfig"""
    config_name, config_props, config_description = get_appconfig_details(cmd_args)

    # Check if appconfig already exists by that name, if so update/add corresponding name/value pairs
    update_appconfig(instance, config_name, config_props, config_description)

    # No appconfig exists by that name, create new one
    instance.create_application_configuration(name=config_name, properties=config_props, description=config_description)

# ch-appconfig
def _chappconfig_parser(subparsers):
    appconfig_ch = subparsers.add_parser('chappconfig', help='Change the configuration properties that are used to make a connection to an external application')
    appconfig_ch.add_argument('config_name', help='Name of the app config')
    appconfig_ch.add_argument('--property', action='append', help='Specifies a property name and value pair to add to or change in the configuration')
    appconfig_ch.add_argument('--description', help='Specifies a description for the application configuration')

def _chappconfig(instance, cmd_args):
    config_name, config_props, config_description = get_appconfig_details(cmd_args)

    # Update the config
    update_appconfig(instance, config_name, config_props, config_description)

# utility function for mk-appconfig and ch-appconfig
# gets config name, properties and description if it has one
def get_appconfig_details(cmd_args):
    config_name = cmd_args.config_name
    config_props = {}
    config_description = None

    # Get props_file if given
    if (cmd_args.propfile):
        prop_list = [line.rstrip() for line in open(cmd_args.propfile)]
        config_props = create_appconfig_props(config_props, prop_list)

    # Get props from command line
    # Name's specified via command line (ie --property option) override names specified in the prop file
    if (cmd_args.property):
        prop_list = cmd_args.property
        config_props = create_appconfig_props(config_props, prop_list)

    # Create the config description
    if (cmd_args.description):
        config_description = cmd_args.description

    # If dictionary has no name/value pairs, there is no properties
    if (not config_props):
        config_props = None

    return config_name, config_props, config_description

# utility function for mk-appconfig and ch-appconfig
# Create the config properties
def create_appconfig_props(config_props, prop_list):
    # Iterate through list of properties, check if correct format, convert each to name/value pairs add to config_props dict
    # Ex. good proplist is ['name1=value1', 'name2=value2']
    # Ex. bad proplist is ['name1=valu=e1', 'name2=value2']
    for prop in prop_list:
        name_value_pair = prop.split("=")
        if (len(name_value_pair) is not 2):
            print("incorrect property format")
        config_props[name_value_pair[0]] = name_value_pair[1]
    return config_props

# utility function for mk-appconfig and ch-appconfig
# if appconfig already exists with name config_name, update/add corresponding name/value pairs
def update_appconfig(instance, config_name, config_props, config_description):
    if (instance.get_application_configurations(config_name)):
        appconfig = instance.get_application_configurations(config_name)[0]
        appconfig.update(properties=config_props, description=config_description)

# get-appconfig
def _getappconfig_parser(subparsers):
    appconfig_get = subparsers.add_parser('getappconfig', help='Displays the properties of a configuration that enables connection to an external application')
    appconfig_get.add_argument('config_name', help='Name of the app config')

def _getappconfig(instance, cmd_args):
    """get an appconfig"""
    config_name = cmd_args.config_name
    configs = instance.get_application_configurations(name = config_name)
    # Check if any configs by that name
    if (not configs):
        print("No application configuration by the name {}".format(config_name))
        return
    config = configs[0]
    config_props = config.properties
    for key, value in config_props.items():
        try:
            json_value = json.loads(value)
            json_value = json.dumps(json_value, indent=2)
        except ValueError:
            json_value = value
        print(key + "=" + json_value)


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
    "lsappconfig": _lsappconfig,
    "rmappconfig":_rmappconfig,
    "mkappconfig": _mkappconfig,
    "chappconfig": _chappconfig,
    "getappconfig": _getappconfig,
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
    _lsappconfig_parser(subparsers)
    _rmappconfig_parser(subparsers)
    _mkappconfig_parser(subparsers)
    _chappconfig_parser(subparsers)
    _getappconfig_parser(subparsers)



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
