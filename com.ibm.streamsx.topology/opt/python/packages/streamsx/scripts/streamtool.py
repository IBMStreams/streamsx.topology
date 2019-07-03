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
    job_submit.add_argument('--jobConfig', '-g', help='Specifies the name of an external file that defines a job configuration overlay')
    job_submit.add_argument('--jobname', help='Specifies the name of the job.')
    job_submit.add_argument('--P', '-P', help='Specifies a submission-time parameter and value for the job', action='append')
    _user_arg(job_submit)

def _submitjob(instance, cmd_args):
    """Submit a job."""
    job_config = None

    if cmd_args.jobConfig:
        with open(cmd_args.jobConfig) as fd:
            job_config = streamsx.topology.context.JobConfig.from_overlays(json.load(fd))
    else:
        job_config = streamsx.topology.context.JobConfig()

    if cmd_args.jobname:
        job_config.job_name = cmd_args.jobname

    if cmd_args.P:
        for param in cmd_args.P:
            name_value_pair = param.split("=")
            if len(name_value_pair) != 2:
                raise ValueError("The format of the following submission-time parameter is not valid: {}. The correct syntax is: <name>=<value>".format(param))
            else:
                job_config.submission_parameters[name_value_pair[0]] = name_value_pair[1]

    instance.submit_job(bundle=cmd_args.sabfile, job_config=job_config)



###########################################
# canceljob
###########################################
def _canceljob_parser(subparsers):
    job_cancel = subparsers.add_parser('canceljob', help='Cancel a job.')
    job_cancel.add_argument('--jobs', '-j', help='Specifies a list of job IDs.', metavar='job-id')
    job_cancel.add_argument('--force', action='store_true', help='Stop the service even if jobs are running.', default=False)
    job_cancel.add_argument('--jobnames', help='Specifies a list of job names')
    job_cancel.add_argument('--file', '-f', help='Specifies the file that contains a list of job IDs, one per line')
    job_cancel.add_argument('--collectlogs', help='Specifies to collect the log and trace files for each processing element that is associated with the job', action='store_true')

    _user_arg(job_cancel)

def _canceljob(instance, cmd_args):
    """Cancel a job."""
    job_ids_to_cancel = []
    job_names_to_cancel = []

    # if --jobs, get list of job IDs to cancel
    if cmd_args.jobs:
        job_ids = cmd_args.jobs.split(',')
        job_ids_to_cancel.extend(job_ids)

    # if --jobnames, get list of jobnames to cancel
    if cmd_args.jobnames:
        job_names = cmd_args.jobnames.split(',')
        job_names_to_cancel.extend(job_names)

    # if --file, get list of job IDs to cancel from file
    if cmd_args.file:
        my_file = open(cmd_args.file)
        job_ids = [line.rstrip() for line in my_file if not line.isspace()]
        my_file.close()
        job_ids_to_cancel.extend(job_ids)

    # Check if job w/ job ID exists, and if so cancel it
    if job_ids_to_cancel:
        for x in job_ids:
            try:
                job = instance.get_job(id=str(x))
                _job_cancel(instance, x, cmd_args.collectlogs, cmd_args.force)
            except:
                print("The following job ID was not found {}".format(x))
                print("The following job ID cannot be canncelled: {}. See the previous error message".format(x))

    # Check if job w/ job name exists, and if so cancel it
    if job_names_to_cancel:
        for x in job_names_to_cancel:
            jobs = instance.get_jobs(name=str(x))
            if jobs:
                job = jobs[0]
                _job_cancel(instance, job.id, cmd_args.collectlogs, cmd_args.force)
            else:
                print("The following job name is not found: {}. Specify a job name that is valid and try the request again".format(x))

def _job_cancel(instance, job_id=None, collectlogs=False, force=False):
    job = instance.get_job(id=str(job_id))
    if collectlogs:
        log_path = job.retrieve_log_trace()
        if log_path:
            print("The log files for the {} job ID will be collected in the following files: {}".format(job_id, log_path))
        else:
            raise Exception("Retrieval of job's logs is not supported in this version of IBM Streams")
    val = job.cancel(force)
    # Check if job cancelled succesfully
    if val:
        print("The following job ID was canncelled: {}. The job was in the {} instance.".format(job_id, instance.id))
    else:
        raise Exception("One or more jobs failed to stop")


###########################################
# lsjobs
###########################################
def _lsjobs_parser(subparsers):
    job_ls = subparsers.add_parser('lsjobs', help='List the jobs for a given instance')
    job_ls.add_argument('--jobs', '-j', help='Specifies a list of job IDs.', metavar='job-id')
    job_ls.add_argument('--users', '-u', help='Specifies to select from this list of user IDs')
    job_ls.add_argument('--jobnames', help='Specifies a list of job names')
    job_ls.add_argument('--fmt', help='Specifies the presentation format', default='%Tf')
    job_ls.add_argument('--xheaders', help='Specifies to exclude headings from the report', action='store_true')
    job_ls.add_argument('--long', '-l', help='Reports launch count, full host names, and all of the operator instance names for the PEs.', action='store_true')
    job_ls.add_argument('--showtimestamp', help='Specifies to show a time stamp in the output to indicate when the command was run.st', action='store_true')

    _user_arg(job_ls)

def _lsjobs(instance, cmd_args):
    """view jobs"""
    jobs = instance.get_jobs()
    instance_id = instance.id

    # Omit header if xheaders is present
    if cmd_args.xheaders:
        cmd_args.showtimestamp = False
        instance_id = None

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

    # If --showtimestamp, give current date
    time_stamp = None
    LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    if cmd_args.showtimestamp:
        time_stamp = datetime.datetime.now().replace(microsecond=0).replace(tzinfo=LOCAL_TIMEZONE).isoformat()

    # If default output format
    if cmd_args.fmt == '%Tf':
        if time_stamp:
            print("Date: " + time_stamp)

        if instance_id:
            print("Instance: " + instance_id)
            print('{: <5} {:<10} {:<10} {:<10} {:<30} {:<40} {:<20}'.format("Id", "State", "Healthy", "User", "Date", "Name", "Group"))
        for job in jobs:
            jobHealth = "yes" if job.health == "healthy" else "no"
            jobTime = datetime.datetime.fromtimestamp(job.submitTime/1000).replace(tzinfo=LOCAL_TIMEZONE).isoformat() # job.submitTime/1000 to convert ms to sec
            jobGroup = job.jobGroup.split("/")[-1]

            print('{: <5} {:<10} {:<10} {:<10} {:<30} {:<40} {:<20}'.format(job.id, job.status.capitalize(), jobHealth, job.startedBy, jobTime, job.name, jobGroup))
    # non default output format, use helper function
    else:
        headers = ["Id", "State", "Healthy", "User", "Date", "Name", "Group"]
        data = []
        for job in jobs:
            item = []
            jobHealth = "yes" if job.health == "healthy" else "no"
            jobTime = datetime.datetime.fromtimestamp(job.submitTime/1000).replace(tzinfo=LOCAL_TIMEZONE).isoformat() # job.submitTime/1000 to convert ms to sec
            jobGroup = job.jobGroup.split("/")[-1]
            item.extend((job.id, job.status.capitalize(), jobHealth, job.startedBy, jobTime, job.name, jobGroup))
            data.append(item)

        _print_ls("lsjobs", cmd_args.fmt, headers, data, instance_id=instance_id, Date=time_stamp)


###########################################
# appconfig
###########################################

# ls-appconfig
def _lsappconfig_parser(subparsers):
    appconfig_ls = subparsers.add_parser('lsappconfig', help='Retrieve a list of configurations for making a connection to an external application')
    appconfig_ls.add_argument('--fmt', help='Specifies the presentation format', default='%Tf')
    _user_arg(appconfig_ls)

def _lsappconfig(instance, cmd_args):
    """view appconfigs"""
    configs = instance.get_application_configurations()

    # If default output format
    if cmd_args.fmt == '%Tf':
        print('{: <20} {:<20} {:<30} {:<30} {:<20}'.format("Id", "Owner", "Created", "Modified", "Description"))
        for config in configs:
            createDate = datetime.datetime.fromtimestamp(config.creationTime/1000).strftime("%m/%d/%Y, %I:%M %p ") + "GMT"
            lastModifiedDate = datetime.datetime.fromtimestamp(config.lastModifiedTime/1000).strftime("%m/%d/%Y, %I:%M %p ") + "GMT"
            print('{: <20} {:<20} {:<30} {:<30} {:<20}'.format(config.name, config.owner, createDate, lastModifiedDate, config.description))
    # non default output format, use helper function
    else:
        headers = ["Id", "Owner", "Created", "Modified", "Description"]
        data = []
        for config in configs:
            item = []
            createDate = datetime.datetime.fromtimestamp(config.creationTime/1000).strftime("%m/%d/%Y, %I:%M %p ") + "GMT"
            lastModifiedDate = datetime.datetime.fromtimestamp(config.lastModifiedTime/1000).strftime("%m/%d/%Y, %I:%M %p ") + "GMT"
            item.extend((config.name, config.owner, createDate, lastModifiedDate, config.description))
            data.append(item)

        _print_ls("lsappconfig", cmd_args.fmt, headers, data)

def _print_ls(command_name, fmt, headers, data, instance_id=None, Date=None):
    """A helper function that prints the output for lsjobs or lsappconfig if the --fmt flag specifies either '%Mf' or '%Nf'.
    The default '%Tf' is handled by each individual function

    Arguments:
        command_name {String} -- Either 'lsjobs' or 'lsappconfig'
        fmt {String} -- Describes the format output should be in. Either '%Mf' or '%Nf'
        headers {List} -- List consisting of strings that describe the headers for the given output of each command
        data {List} -- A list consisting of list items that each contain the data for either a single job, or a single appconfig

    Keyword Arguments:
        instance_id {String} -- (only for lsjobs) Needed to show the instance (default: {None})
        Date {String} -- (only for lsjobs) Only present if --showtimestamp flag (default: {None})
    """
    if fmt == "%Mf":
        if command_name is "lsjobs" and instance_id:
            print("=================================================")
            if Date:
                print("Date: " + Date)
            print("Instance: " + instance_id)
            print("=================================================")

        for item in data:
            print("=================================================")
            for header, row in zip(headers, item):
                print('{: <15} : {: <20}'.format(header, row))
        print("=================================================")

    elif fmt == "%Nf":
        for item in data:
            toPrint = ''
            for header, row in zip(headers, item):
                toPrint += '{}: {: <25} '.format(header, row)
            print(toPrint)

# rm-appconfig
def _rmappconfig_parser(subparsers):
    appconfig_rm = subparsers.add_parser('rmappconfig', help='Removes a configuration that is used for making a connection to an external application')
    appconfig_rm.add_argument('config_name', help='Name of the app config')
    appconfig_rm.add_argument('--noprompt', help='Specifies to suppress confirmation prompts.', action='store_true')
    _user_arg(appconfig_rm)

def _rmappconfig(instance, cmd_args):
    """remove an appconfig"""
    config_name = cmd_args.config_name
    configs = instance.get_application_configurations(name = config_name)
    if (not configs):
        raise NameError("The {} application configuration does not exist in the {} instance".format(config_name, instance.id))
    app_config = instance.get_application_configurations(name = config_name)[0]

    # No confirmation required, delete
    if (cmd_args.noprompt):
        app_config.delete()
        print("The {} application configuration was removed successfully for the {} instance".format(config_name, instance.id))
    else:
        response = input("Do you want to remove the application configuration {} from the {} instance? Enter 'y' to continue or 'n' to cancel: ".format(config_name, instance.id))
        if (response == "y"):
            app_config.delete()
            print("The {} application configuration was removed successfully for the {} instance".format(config_name, instance.id))


# mk-appconfig
def _mkappconfig_parser(subparsers):
    appconfig_mk = subparsers.add_parser('mkappconfig', help='Creates a configuration that enables connection to an external application')
    appconfig_mk.add_argument('config_name', help='Name of the app config')
    appconfig_mk.add_argument('--property', action='append', help='Specifies a property name and value pair to add to or change in the configuration')
    appconfig_mk.add_argument('--propfile', help='Specifies the path to a file that contains a list of application configuration properties for connecting to an external application')
    appconfig_mk.add_argument('--description', help='Specifies a description for the application configuration')
    _user_arg(appconfig_mk)

def _mkappconfig(instance, cmd_args):
    """create an appconfig"""
    config_name, config_props, config_description = _get_config_details(cmd_args, mk=True)

    # Check if config exists by that name, if so don't do anything
    if instance.get_application_configurations(config_name):
        raise Exception("The {} application configuration already exists in the following {} instance".format(config_name, instance.id))
    else:
        # No appconfig exists by that name, create new one
        appconfig =  instance.create_application_configuration(name=config_name, properties=config_props, description=config_description)
        if appconfig:
            print("The {} application configuration was created successfully for the {} instance".format(config_name, instance.id))
            return appconfig

# ch-appconfig
def _chappconfig_parser(subparsers):
    appconfig_ch = subparsers.add_parser('chappconfig', help='Change the configuration properties that are used to make a connection to an external application')
    appconfig_ch.add_argument('config_name', help='Name of the app config')
    appconfig_ch.add_argument('--property', action='append', help='Specifies a property name and value pair to add to or change in the configuration')
    appconfig_ch.add_argument('--description', help='Specifies a description for the application configuration')
    _user_arg(appconfig_ch)


def _chappconfig(instance, cmd_args):
    config_name, config_props, config_description = _get_config_details(cmd_args, mk=False)
    # Check if config exists by that name, if so update it
    if instance.get_application_configurations(config_name):
        appconfig = instance.get_application_configurations(config_name)[0]
        newAppconfig = appconfig.update(properties=config_props, description=config_description)
        if (newAppconfig):
            print("The {} application configuration was updated successfully for the {} instance".format(config_name, instance.id))
            return newAppconfig
    else:
        # No appconfig exists by that name
        raise NameError("The {} application configuration does not exist in the {} instance".format(config_name, instance.id))


def _get_config_details(cmd_args, mk):
    """A helper function for mkappconfig and chappconfig that gets the name, properties and description of an appconfig
    Arguments:
        cmd_args {List} -- The args passed in the command line
        mk {Boolean} -- Describes whether or not to check for a propFile arg (only mkappconfig can have a --propfile arg)

    Returns:
        config_name {String} -- The name of the appconfig
        config_props {Dictionary} -- key-value pair properties of the appconfig
        config_description {String} -- description of the appconfig
    """
    config_name = cmd_args.config_name
    config_props = {}
    config_description = None
    # If mkappconfig command (bc only this command allows for a propFile) Get props_file if given
    if mk:
        if cmd_args.propfile:
            my_file = open(cmd_args.propfile)
            prop_list = [line.rstrip() for line in my_file if not line.isspace()]
            my_file.close()
            config_props = _create_appconfig_props(config_props, prop_list)

    # Get props from command line
    # Name's specified via command line (ie --property option) override names specified in the prop file
    if cmd_args.property:
        prop_list = cmd_args.property
        config_props = _create_appconfig_props(config_props, prop_list)

    # Create the config description
    if cmd_args.description:
        config_description = cmd_args.description

    return config_name, config_props, config_description

def _create_appconfig_props(config_props, prop_list):
    """A helper function for mk-appconfig and ch-appconfig that creates the config properties dictionary

    Arguments:
        config_props {Dictionary} -- A dictionary with key-value pairs that gives the properties in an appconfig
        prop_list {List} -- A list with items of the form <Key>=<Value> that describe a given property in an appconfig

    Returns:
        config_props {Dictionary} -- A dictionary with key-value pairs that gives the properties in an appconfig
    """
    # Iterate through list of properties, check if correct format, convert each to name/value pairs add to config_props dict
    # Ex. good proplist is ['name1=value1', 'name2=value2']
    # Ex. bad proplist is ['name1=valu=e1', 'name2=value2']
    for prop in prop_list:
        name_value_pair = prop.split("=")
        if (len(name_value_pair) != 2):
            raise ValueError("The format of the following property specification is not valid: {}. The correct syntax is: <name>=<value>".format(prop))
        config_props[name_value_pair[0]] = name_value_pair[1]
    return config_props

# get-appconfig
def _getappconfig_parser(subparsers):
    appconfig_get = subparsers.add_parser('getappconfig', help='Displays the properties of a configuration that enables connection to an external application')
    appconfig_get.add_argument('config_name', help='Name of the app config')
    _user_arg(appconfig_get)


def _getappconfig(instance, cmd_args):
    """get an appconfig"""
    config_name = cmd_args.config_name
    configs = instance.get_application_configurations(name = config_name)
    # Check if any configs by that name
    if (not configs):
        raise NameError("No application configuration by the name {}".format(config_name))
    config = configs[0]
    config_props = config.properties
    if not config_props:
        raise Exception("The {} application configuration has no properties defined".format(config_name))

    for key, value in config_props.items():
        try:
            json_value = json.loads(value)
            json_value = json.dumps(json_value, indent=2)
        except ValueError:
            json_value = value
        print(key + "=" + json_value)
    return config_props


def run_cmd(args=None):
    cmd_args = _parse_args(args)

    if cmd_args.disable_ssl_verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    instance = Instance.of_endpoint(
        username=cmd_args.User,
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

    extra_info = None
    rc = 0
    try:
        extra_info = switch[cmd_args.subcmd](instance, cmd_args)
    except Exception as e:
        rc = 1
        print(e)
        # sys.exc_info()
    return (rc, extra_info)

def main(args=None):
    """ Mimic streamtool using the REST api for ICP4D.
    """
    streamsx._streams._version._mismatch_check('streamsx.topology.context')

    rc, extra_info = run_cmd(args)
    return rc

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

def _user_arg(parser):
    parser.add_argument('--User', '-U', help='Specifies an IBM Streams user ID that has authority to run the command.', metavar='user')


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

