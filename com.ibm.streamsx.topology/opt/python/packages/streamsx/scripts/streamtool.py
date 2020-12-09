# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import sys
import sysconfig
import os
import argparse
import requests
import warnings
import urllib3
import datetime
import json
import locale
import re

import streamsx.topology.context
from streamsx.rest_primitives import Instance
from streamsx.build import BuildService



###########################################
# submitjob
###########################################
def _submitjob_parser(subparsers):
    job_submit = subparsers.add_parser('submitjob', help='Submit an application bundle')
    job_submit.add_argument('sabfile', help='Location of sab file.', metavar='sab-pathname')
    job_submit.add_argument('--jobConfig', '-g', help='Specifies the name of an external file that defines a job configuration overlay', metavar='file-name')
    job_submit.add_argument('--jobname', help='Specifies the name of the job.', metavar='job-name')
    job_submit.add_argument('--jobgroup', '-J', help='Specifies the job group', metavar='jobgroup-name')
    job_submit.add_argument('--outfile', help='Specifies the path and file name of the output file in which the command writes the list of submitted job IDs', metavar='file-name')
    job_submit.add_argument('--P', '-P', help='Specifies a submission-time parameter and value for the job', action='append', metavar='parameter-name')
    _user_arg(job_submit)

def _submitjob(instance, cmd_args, rc):
    """Submit a job."""
    job_config = None

    if cmd_args.jobConfig:
        with open(cmd_args.jobConfig) as fd:
            job_config = streamsx.topology.context.JobConfig.from_overlays(json.load(fd))
    else:
        job_config = streamsx.topology.context.JobConfig()

    if cmd_args.jobname:
        job_config.job_name = cmd_args.jobname

    if cmd_args.jobgroup:
        # Will throw a 500 internal error on instance.submit_job if jobgroup doesn't already exist
        job_config.job_group = cmd_args.jobgroup

    if cmd_args.P:
        for param in cmd_args.P:
            name_value_pair = param.split("=")
            if len(name_value_pair) != 2:
                raise ValueError("The format of the following submission-time parameter is not valid: {}. The correct syntax is: <name>=<value>".format(param))
            else:
                job_config.submission_parameters[name_value_pair[0]] = name_value_pair[1]

    job = instance.submit_job(bundle=cmd_args.sabfile, job_config=job_config)

    if job:
        print("The following number of applications were submitted to the {} instance: {}.".format(instance.id, 1))
        print("Submitted job IDs: {}".format(job.id), file=sys.stderr)
    else:
        raise Exception("Error in creating Job")

    # If --outfile, write jobID to file
    if cmd_args.outfile:
        with open(cmd_args.outfile, 'w') as my_file:
            my_file.write(str(job.id) + '\n')

    return (rc, job)

###########################################
# canceljob
###########################################
def _canceljob_parser(subparsers):
    job_cancel = subparsers.add_parser('canceljob', help='Cancel a job.')
    job_cancel.add_argument('--force', action='store_true', help='Stop the service even if jobs are running.', default=False)
    job_cancel.add_argument('--collectlogs', help='Specifies to collect the log and trace files for each processing element that is associated with the job', action='store_true')
    job_cancel.add_argument('jobid', help='Specifies a list of job IDs.', nargs='*', metavar='jobid')
    # Only 1 of these arguments --jobs, --jobnames, --file can be specified at any given time when running this command
    g1 = job_cancel.add_argument_group(title='jobs jobnames file group', description='One of these options must be chosen.')
    group = g1.add_mutually_exclusive_group(required=False)
    group.add_argument('--jobs', '-j', help='Specifies a list of job IDs.', metavar='job-id')
    group.add_argument('--jobnames', help='Specifies a list of job names', metavar='job-names')
    group.add_argument('--file', '-f', help='Specifies the file that contains a list of job IDs, one per line', metavar='file-name')

    _user_arg(job_cancel)

def _canceljob(instance, cmd_args, rc):
    """Cancel a job."""
    job_ids_to_cancel = []
    job_names_to_cancel = []

    # Check for mutually exclusive arguments, just in case argparse doesn't catch it
    temp = len(list(filter(None, [cmd_args.jobid, cmd_args.jobs, cmd_args.jobnames, cmd_args.file])))
    if temp > 1:
        raise ValueError("Arguments jobid, --jobs, --jobnames, --file are mutually exclusive")

    # get list of job IDs to cancel
    if cmd_args.jobid:
        for x in cmd_args.jobid:
            job_ids = x.split(',')
            job_ids = [job.strip() for job in job_ids]
            job_ids_to_cancel.extend(job_ids)

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
        with open(cmd_args.file, 'r') as my_file:
            job_ids = [line.rstrip() for line in my_file if not line.isspace()]
            job_ids_to_cancel.extend(job_ids)

    # Filter out any blank/empty strings in job_ids_to_cancel and job_names_to_cancel
    # Ex. 'canceljob job1.id , job2.id' -> job_ids_to_cancel = ['job1.id', '', '', 'job2.id'], should be = ['job1.id', 'job2.id']
    job_ids_to_cancel = list(filter(None, job_ids_to_cancel))
    job_names_to_cancel = list(filter(None, job_names_to_cancel))

    # If no jobs to cancel, raise error
    if not job_ids_to_cancel and not job_names_to_cancel:
        raise Exception("No jobs provided")

    # Check if job w/ job ID exists, and if so cancel it
    for x in job_ids_to_cancel:
        if x.isnumeric():
            try:
                job = instance.get_job(id=str(x))
                _job_cancel(instance, x, cmd_args.collectlogs, cmd_args.force)
            except:
                print("The following job ID was not found {}".format(x), file=sys.stderr)
                print("The following job ID cannot be canceled: {}. See the previous error message".format(x), file=sys.stderr)
                rc = 1
        else:
            raise ValueError("The following job identifier is not valid: {}. Specify a job identifier that is numeric and try the request again.".format(x))

    # Check if job w/ job name exists, and if so cancel it
    for x in job_names_to_cancel:
        # if jobname contains a space, its invalid
        if ' ' in x:
            print("{} is not a valid job name. Either its size is longer than 1024 characters or it includes some invalid characters.".format(x), file=sys.stderr)
            rc = 1
            continue
        jobs = instance.get_jobs(name=str(x))
        if jobs:
            job = jobs[0]
            _job_cancel(instance, job.id, cmd_args.collectlogs, cmd_args.force)
        else:
            print("The following job name is not found: {}. Specify a job name that is valid and try the request again.".format(x), file=sys.stderr)
            rc = 1

    return (rc, None)

def _job_cancel(instance, job_id=None, collectlogs=False, force=False):
    job = instance.get_job(id=str(job_id))
    if collectlogs:
        log_path = job.retrieve_log_trace()
        if log_path:
            print("The log files for the {} job ID will be collected in the following files: {}".format(job_id, log_path))
        else:
            raise Exception("Retrieval of job's logs is not supported in this version of IBM Streams")

    # Cancel job, and check if successful
    val = job.cancel(force)
    if val:
        print("The following job ID was canceled: {}. The job was in the {} instance.".format(job_id, instance.id))
    else:
        raise Exception("One or more jobs failed to stop")


###########################################
# lsjobs
###########################################
def _lsjobs_parser(subparsers):
    job_ls = subparsers.add_parser('lsjobs', help='List the jobs for a given instance')
    job_ls.add_argument('--jobs', '-j', help='Specifies a list of job IDs.', metavar='job-id')
    job_ls.add_argument('--users', '-u', help='Specifies to select from this list of user IDs', metavar='user')
    job_ls.add_argument('--jobnames', help='Specifies a list of job names', metavar='job-names')
    job_ls.add_argument('--fmt', help='Specifies the presentation format', default='%Tf', metavar='format-spec')
    job_ls.add_argument('--xheaders', help='Specifies to exclude headings from the report', action='store_true')
    job_ls.add_argument('--long', '-l', help='Reports launch count, full host names, and all of the operator instance names for the PEs.', action='store_true')
    job_ls.add_argument('--showtimestamp', help='Specifies to show a time stamp in the output to indicate when the command was run.', action='store_true')

    _user_arg(job_ls)

def _lsjobs(instance, cmd_args, rc):
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

    headers = ["Id", "State", "Healthy", "User", "Date", "Name", "Group"]

    # If --long option, add productVersion to the headers and get the corresponding value for each job
    if cmd_args.long:
        headers.append('ProductVersion')

    # pre process the data so output is formatted nicely
    h_length = [len(x) for x in headers] # header_length
    job_data = []
    for job in jobs:
        jobHealth = "yes" if job.health == "healthy" else "no"
        jobTime = datetime.datetime.fromtimestamp(job.submitTime/1000, tz = LOCAL_TIMEZONE).isoformat() # job.submitTime/1000 to convert ms to sec
        jobGroup = job.jobGroup.split("/")[-1]
        data = [job.id, job.status.capitalize(), jobHealth, job.startedBy, jobTime, job.name, jobGroup]

        h_length[0] = max(len(job.id), h_length[0])
        h_length[1] = max(len(job.status), h_length[1])
        h_length[2] = max(len(jobHealth), h_length[2])
        h_length[3] = max(len(job.startedBy), h_length[3])
        h_length[4] = max(len(jobTime), h_length[4])
        h_length[5] = max(len(job.name), h_length[5])
        h_length[6] = max(len(jobGroup), h_length[6])

        # If --long option, add productVersion to the job data
        if cmd_args.long:
            prod_version = job.productVersion
            h_length[7] = max(len(prod_version), h_length[7])
            data.append(prod_version)
        job_data.append(data)

    # Output the data

    # If default output format
    if cmd_args.fmt == '%Tf':
        if time_stamp:
            print("Date: " + time_stamp)

        if instance_id:
            print("Instance: " + instance_id)
            # String that represents the header w/ formatting for nice output
            header_string = '{:>{id_w}}  {:<{state_w}}  {:<{health_w}}  {:<{user_w}}  {:<{date_w}}  {:<{name_w}}  {:<{group_w}}'.format(
                "Id", "State", "Healthy", "User", "Date", "Name", "Group",
                id_w=h_length[0], state_w=h_length[1],
                health_w=h_length[2], user_w=h_length[3],
                date_w=h_length[4], name_w=h_length[5], group_w=h_length[6])

            # If --long option, we also want the header to display ProductVersion
            if cmd_args.long:
                header_string += '  {:<{prod_w}}'.format("ProductVersion", prod_w=h_length[7])
            print(header_string)

        for job in job_data:
            # String that represents a job w/ formatting for nice output
            job_string = '{:>{id_w}}  {:<{state_w}}  {:<{health_w}}  {:<{user_w}}  {:<{date_w}}  {:<{name_w}}  {:<{group_w}}'.format(
                job[0], job[1], job[2], job[3], job[4], job[5], job[6],
                id_w=h_length[0], state_w=h_length[1],
                health_w=h_length[2], user_w=h_length[3],
                date_w=h_length[4], name_w=h_length[5], group_w=h_length[6])

            # If --long option, we also want the job to display ProductVersion
            if cmd_args.long:
                job_string += '  {:<{prod_w}}'.format(job[7], prod_w=h_length[7])
            print(job_string)

    elif cmd_args.fmt == "%Mf":
        border = '=' * 50

        if instance_id:
            print(border)
            if time_stamp:
                print("Date: " + time_stamp)
            print("Instance: " + instance_id)

        w1 = len(max(headers, key=len))
        w2 = max(h_length)
        for job in job_data:
            print(border)
            for header, job_item in zip(headers, job):
                print('{:{w1}}  :  {: <{w2}}'.format(header, job_item, w1=w1, w2=w2))
        print(border)

    elif cmd_args.fmt == "%Nf":
        for job in job_data:
            toPrint = ''
            for header, row in zip(headers, job):
                toPrint += '{}  :  {} '.format(header, row)
            print(toPrint)

    return (rc, None)

###########################################
# appconfig
###########################################

# ls-appconfig
def _lsappconfig_parser(subparsers):
    appconfig_ls = subparsers.add_parser('lsappconfig', help='Retrieve a list of configurations for making a connection to an external application')
    appconfig_ls.add_argument('--fmt', help='Specifies the presentation format', default='%Tf', metavar='format-spec')
    _user_arg(appconfig_ls)

def _lsappconfig(instance, cmd_args, rc):
    """view appconfigs"""
    configs = instance.get_application_configurations()
    locale.setlocale(locale.LC_ALL, '') # Needed to correctly display local date and time format
    LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo # Needed to get timezone

    headers = ["Id", "Owner", "Created", "Modified", "Description"]

    # pre process the data so output is formatted nicely
    h_length = [len(x) for x in headers] # header_length
    config_data = []
    for config in configs:
        createDate = datetime.datetime.fromtimestamp(config.creationTime/1000, tz = LOCAL_TIMEZONE).strftime("%x %X %Z")
        lastModifiedDate = datetime.datetime.fromtimestamp(config.lastModifiedTime/1000, tz = LOCAL_TIMEZONE).strftime("%x %X %Z")

        data = [config.name, config.owner, createDate, lastModifiedDate, config.description]
        config_data.append(data)

        h_length[0] = max(len(config.name), h_length[0])
        h_length[1] = max(len(config.owner), h_length[1])
        h_length[2] = max(len(createDate), h_length[2])
        h_length[3] = max(len(lastModifiedDate), h_length[3])
        h_length[4] = max(len(config.description), h_length[4])

    # Output the data

    # If default output format
    if cmd_args.fmt == '%Tf':
        print('{:>{id_w}}  {:<{owner_w}}  {:<{created_w}}  {:<{mod_w}}  {:<{desc_w}}'.format(
            "Id", "Owner", "Created", "Modified", "Description",
            id_w=h_length[0], owner_w=h_length[1],
            created_w=h_length[2], mod_w=h_length[3],
            desc_w=h_length[4]))

        for config in config_data:
            print('{:>{id_w}}  {:<{owner_w}}  {:<{created_w}}  {:<{mod_w}}  {:<{desc_w}}'.format(
            config[0], config[1], config[2], config[3], config[4],
            id_w=h_length[0], owner_w=h_length[1],
            created_w=h_length[2], mod_w=h_length[3],
            desc_w=h_length[4]))

    elif cmd_args.fmt == "%Mf":
        border = '=' * 50

        w1 = len(max(headers, key=len))
        w2 = max(h_length)
        for config in config_data:
            print(border)
            for header, config_item in zip(headers, config):
                print('{:{w1}}  :  {: <{w2}}'.format(header, config_item, w1=w1, w2=w2))
        print(border)

    elif cmd_args.fmt == "%Nf":
        for config in config_data:
            toPrint = ''
            for header, row in zip(headers, config):
                toPrint += '{}  :  {} '.format(header, row)
            print(toPrint)

    return (rc, None)

# rm-appconfig
def _rmappconfig_parser(subparsers):
    appconfig_rm = subparsers.add_parser('rmappconfig', help='Removes a configuration that is used for making a connection to an external application')
    appconfig_rm.add_argument('config_name', help='Name of the app config', metavar='config-name')
    appconfig_rm.add_argument('--noprompt', help='Specifies to suppress confirmation prompts.', action='store_true')
    _user_arg(appconfig_rm)

def _rmappconfig(instance, cmd_args, rc):
    """remove an appconfig"""
    config_name = cmd_args.config_name
    configs = instance.get_application_configurations(name = config_name)
    if (not configs):
        raise NameError("The {} application configuration does not exist in the {} instance".format(config_name, instance.id))
    app_config = instance.get_application_configurations(name = config_name)[0]

    # No confirmation required, delete
    if cmd_args.noprompt:
        app_config.delete()
        print("The {} application configuration was removed successfully for the {} instance".format(config_name, instance.id))
    else:
        response = input("Do you want to remove the application configuration {} from the {} instance? Enter 'y' to continue or 'n' to cancel: ".format(config_name, instance.id))
        if response == "y":
            app_config.delete()
            print("The {} application configuration was removed successfully for the {} instance".format(config_name, instance.id))

    return (rc, None)

# mk-appconfig
def _mkappconfig_parser(subparsers):
    appconfig_mk = subparsers.add_parser('mkappconfig', help='Creates a configuration that enables connection to an external application')
    appconfig_mk.add_argument('config_name', help='Name of the app config', metavar='config-name')
    appconfig_mk.add_argument('--property', action='append', help='Specifies a property name and value pair to add to or change in the configuration', metavar='name=value')
    appconfig_mk.add_argument('--propfile', help='Specifies the path to a file that contains a list of application configuration properties for connecting to an external application', metavar='property-file')
    appconfig_mk.add_argument('--description', help='Specifies a description for the application configuration', metavar='description')
    _user_arg(appconfig_mk)

def _mkappconfig(instance, cmd_args, rc):
    """create an appconfig"""
    config_name, config_props, config_description = _get_config_details(cmd_args, mk=True)
    appconfig = None

    # Check if config exists by that name, if so don't do anything
    if instance.get_application_configurations(config_name):
        raise Exception("The {} application configuration already exists in the following {} instance".format(config_name, instance.id))
    else:
        # No appconfig exists by that name, create new one
        appconfig =  instance.create_application_configuration(name=config_name, properties=config_props, description=config_description)
        if appconfig:
            print("The {} application configuration was created successfully for the {} instance".format(config_name, instance.id))
        else:
            # Failed to create appconfig
            rc = 1

    return (rc, appconfig)

# ch-appconfig
def _chappconfig_parser(subparsers):
    appconfig_ch = subparsers.add_parser('chappconfig', help='Change the configuration properties that are used to make a connection to an external application')
    appconfig_ch.add_argument('config_name', help='Name of the app config', metavar='config-name')
    appconfig_ch.add_argument('--property', action='append', help='Specifies a property name and value pair to add to or change in the configuration', metavar='name=value')
    appconfig_ch.add_argument('--description', help='Specifies a description for the application configuration', metavar='description')
    _user_arg(appconfig_ch)

def _chappconfig(instance, cmd_args, rc):
    """change an appconfig"""
    config_name, config_props, config_description = _get_config_details(cmd_args, mk=False)
    new_app_config = None
    # Check if config exists by that name, if so update it
    if instance.get_application_configurations(config_name):
        appconfig = instance.get_application_configurations(config_name)[0]
        new_app_config = appconfig.update(properties=config_props, description=config_description)
        if new_app_config:
            print("The {} application configuration was updated successfully for the {} instance".format(config_name, instance.id))
        else:
            rc = 1
    else:
        # No appconfig exists by that name
        raise NameError("The {} application configuration does not exist in the {} instance".format(config_name, instance.id))

    return (rc, new_app_config)


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
    appconfig_get.add_argument('config_name', help='Name of the app config', metavar='config-name')
    _user_arg(appconfig_get)

def _getappconfig(instance, cmd_args, rc):
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
    return (rc, config_props)

###########################################
# rmtoolkit
###########################################
def _rmtoolkit_parser(subparsers):
    toolkit_rm = subparsers.add_parser('rmtoolkit', help='Remove toolkits from a build service')
    g1 = toolkit_rm.add_argument_group(title='Toolkit selection', description='Selects which toolkits will be removed.')
    group = g1.add_mutually_exclusive_group(required=True)
    group.add_argument('--id', '-i', help='Specifies the id of the toolkit to remove', metavar='toolkit-id')
    group.add_argument('--name', '-n', help='Remove all toolkits with this name', metavar='toolkit-name')
    group.add_argument('--regex', '-r', help='Remove all toolkits where the name matches the given regex pattern', metavar='toolkit-regex')
    _user_arg(toolkit_rm)

def _rmtoolkit(instance, cmd_args, rc):
    # Get all toolkits from the build_server
    build_server = BuildService.of_endpoint(verify=False if cmd_args.disable_ssl_verify else None)

    tk_to_delete = []
    return_message = None

    # Find the toolkit matching toolkitid
    if cmd_args.id:
        try:
            matching_toolkit = build_server.get_toolkit(cmd_args.id)
            tk_to_delete.append(matching_toolkit)
        except ValueError:
            pass

    # Find all toolkits with toolkitname
    elif cmd_args.name:
        remote_toolkits = build_server.get_toolkits()
        matching_toolkits = [x for x in remote_toolkits if x.name == cmd_args.name]
        if matching_toolkits:
            tk_to_delete.extend(matching_toolkits)

    # Find all toolkits where the name matches toolkitregex
    elif cmd_args.regex:
        matching_toolkits = build_server.get_toolkits(name=cmd_args.regex)
        if matching_toolkits:
            tk_to_delete.extend(matching_toolkits)

    # If there are any toolkits to delete, delete them
    for tk in tk_to_delete:
        val = tk.delete()
        if not val:
            # If tk fails to delete, set error code and message
            rc = 1
            return_message = '1 or more toolkits failed to delete'

    return (rc, return_message)

###########################################
# lstoolkit
###########################################
def _lstoolkit_parser(subparsers):
    toolkit_ls = subparsers.add_parser('lstoolkit', help='List toolkits on the build service')
    g1 = toolkit_ls.add_argument_group(title='Toolkit selection', description='Selects which toolkits will be listed.')
    group = g1.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', '-a', action='store_true', help='List all toolkits')
    group.add_argument('--id', '-i', help='Specifies the id of the toolkit to list', metavar='toolkit-id')
    group.add_argument('--name', '-n', help='List all toolkits with this name', metavar='toolkit-name')
    group.add_argument('--regex', '-r', help='List all toolkits where the name matches the given regex pattern', metavar='toolkit-regex')
    _user_arg(toolkit_ls)

def _lstoolkit(instance, cmd_args, rc):
    # Get all toolkits from the build_service
    build_server = BuildService.of_endpoint(verify=False if cmd_args.disable_ssl_verify else None)

    return_message = None

    # Find the toolkit matching toolkitid
    if cmd_args.id:
        try:
            matching_toolkit = build_server.get_toolkit(cmd_args.id)
            tk_to_list = [matching_toolkit]
        except ValueError:
            tk_to_list = []

    # Find all toolkits with toolkitname
    elif cmd_args.name:
        remote_toolkits = build_server.get_toolkits()
        tk_to_list = [x for x in remote_toolkits if x.name == cmd_args.name]

    # Find all toolkits where the name matches toolkitregex
    elif cmd_args.regex:
        tk_to_list = build_server.get_toolkits(name=cmd_args.regex)
    elif cmd_args.all:
        tk_to_list = build_server.get_toolkits()

    tk_to_list.sort(key=lambda tk : (tk.name, tk.version))

    headers = ["Name", "Version", 'RequiredProductVersion', 'Toolkit-ID']

    # pre process the data so output is formatted nicely
    h_length = [len(x) for x in headers] # header_length
    for tk in tk_to_list:
        h_length[0] = max(len(tk.name), h_length[0])
        h_length[1] = max(len(tk.version), h_length[1])
        h_length[2] = max(len(tk.requiredProductVersion), h_length[2])
        h_length[3] = max(len(tk.id), h_length[3])

    fmt = '{:<{name_w}}  {:<{ver_w}}  {:<{pv_w}}  {:<{id_w}}'

    print(fmt.format(headers[0], headers[1], headers[2], headers[3],
        name_w=h_length[0], ver_w=h_length[1], pv_w=h_length[2], id_w=h_length[3]))
    for tk in tk_to_list:
         print(fmt.format(tk.name, tk.version, tk.requiredProductVersion, tk.id,
             name_w=h_length[0], ver_w=h_length[1], pv_w=h_length[2], id_w=h_length[3]))

    return (rc, return_message)

###########################################
# uploadtoolkit
###########################################
def _uploadtoolkit_parser(subparsers):
    toolkit_up = subparsers.add_parser('uploadtoolkit', help='Upload a toolkit to a build service')
    toolkit_up.add_argument('--path', '-p', help='Path to the toolkit to be uploaded', required=True)
    _user_arg(toolkit_up)

def _uploadtoolkit(instance, cmd_args, rc):
    bs = BuildService.of_endpoint(verify=False if cmd_args.disable_ssl_verify else None)

    return_message = None

    tk = bs.upload_toolkit(cmd_args.path)
    if tk is not None:
        print('Toolkit with id {} uploaded from {}'.format(tk.id, cmd_args.path))
    else:
        return_message = 'Toolkit not uploaded from {}'.format(cmd_args.path)
        print(return_message, file=sys.stderr )
        rc = 1

    return (rc, return_message)

###########################################
# updateoperators
###########################################
def _updateops_parser(subparsers):
    update_ops = subparsers.add_parser('updateoperators', help='Adjust a job configuration while the job is running')
    g1 = update_ops.add_argument_group(title='Job selection', description='One of these options must be chosen.')
    group = g1.add_mutually_exclusive_group(required=True)
    group.add_argument('jobid', help='Specifies a job ID.', nargs='?', metavar='jobid')
    group.add_argument('--jobname', help='Specifies the name of the job.', metavar='job-name')
    update_ops.add_argument('--jobConfig', '-g', help='Specifies the name of an external file that defines a job configuration overlay', metavar='file-name')
    update_ops.add_argument('--parallelRegionWidth', help='Specifies a parallel region name and its width', metavar='parallelRegionName=width')
    update_ops.add_argument('--force', action='store_true', help='Specifies whether to automatically stop the PEs that need to be stopped', default=False)

    _user_arg(update_ops)

def _updateops(instance, cmd_args, rc):
    return_message = None
    job_config_json = None
    job = None

    # Check that job w/ jobID or jobname exists
    if cmd_args.jobid:
        job = instance.get_job(id=str(cmd_args.jobid))
    elif cmd_args.jobname:
        jobs = instance.get_jobs(name=str(cmd_args.jobname))
        if jobs:
            job = jobs[0]

    # job doesn't exist, throw error
    if not job:
        return (1, "The job was not found")

    if cmd_args.jobConfig:
        with open(cmd_args.jobConfig) as fd:
            job_config_json = json.load(fd)
        # If empty JCO passed in, throw error
        if not job_config_json:
            return (1, 'JCO is empty')
    elif cmd_args.parallelRegionWidth:
        # If no JCO, but parallelRegionWidth arg passed in, create empty JCO and later populate w/ parallelRegionWidth
        job_config_json = {}
    else:
        # No JCO or parallelRegionWidth arg, throw error
        return (1, 'A JCO or parallelRegionWidth is required')


    if cmd_args.parallelRegionWidth:
        # Overrides the targetParallelRegion if already present in the JCO
        # else, populate empty JCO w/ parallelRegionWidth
        arr = cmd_args.parallelRegionWidth.split('=')
        if len(arr) != 2:
            raise ValueError("The format of the following submission-time parameter is not valid: {}. The correct syntax is: <name>=<value>".format(arr))
        name, width = arr[0], arr[1]

        entry = {'targetParallelRegion': {'regionName': name, 'newWidth': int(width)}}

        # If JCO is empty, and parallelRegionWidth arg is present, create JCO with arg
        if not job_config_json:
            job_config_json = {
                'jobConfigOverlays' : [
                    {'configInstructions' : {'adjustmentSection': [entry]}
                    }
                ]
            }
        else:
            # Non-empty JCO, and parallelRegionWidth arg is present, override existing in JCO

            # jobConfigOverlays is an array, where only the first jobConfigOverlay is supported
            JCO = job_config_json['jobConfigOverlays'][0]
            # Check if configInstructions already exists
            if 'configInstructions' in JCO:
                cfg_inst = JCO['configInstructions']
                # Overwrite adjustmentSection, since only 1 parallelRegion can be specified
                cfg_inst['adjustmentSection'] = [entry]
            else:
                JCO['configInstructions'] = {'adjustmentSection': [entry]}

    # If --force present, force PE to stop
    if cmd_args.force:
        JCO = job_config_json['jobConfigOverlays'][0]
        JCO['operationConfig'] = {"forcePeStopped": True}

    job_config = streamsx.topology.context.JobConfig.from_overlays(job_config_json)
    json_result = job.update_operators(job_config)

    # --- 1/13/20 JSON result is incorrect until 1Q20 fix ---
    # if json_result:
    #     file_name = str(job.name) + '_' + str(job.id) + '_config.json'
    #     with open(file_name, 'w') as outfile:
    #         json.dump(json_result, outfile)

    if json_result !=0:
        return (1, 'Update operators failed')
    else:
        print('Update operators was started on the {} instance.'.format(instance.id))
        # print('The operator configuration results were written to the following file: {}'.format(file_name))

    return (rc, return_message)

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
    "rmtoolkit": _rmtoolkit,
    "lstoolkit": _lstoolkit,
    "uploadtoolkit": _uploadtoolkit,
    "updateoperators": _updateops,
    }

    extra_info = None
    rc = 0
    try:
        rc, extra_info = switch[cmd_args.subcmd](instance, cmd_args, rc)
    except Exception as e:
        rc = 1
        print(e, file=sys.stderr)
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
    cmd_parser = argparse.ArgumentParser(description='IBM Streams command line interface using REST.')
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
    _rmtoolkit_parser(subparsers)
    _lstoolkit_parser(subparsers)
    _uploadtoolkit_parser(subparsers)
    _updateops_parser(subparsers)

    cmd_args = cmd_parser.parse_args(args)
    if cmd_args.subcmd is None:
        cmd_parser.print_help()
        exit(0)
    else:
        return cmd_args

def _user_arg(parser):
    parser.add_argument('--User', '-U', help='Specifies an IBM Streams user ID that has authority to run the command.', metavar='user')


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

