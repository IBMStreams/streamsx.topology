# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import os
import sys
import time
import streamsx.rest
import streamsx.topology.context

applicationName = 'SampleApplication::Main'

applicationParameters = {
    'iterationInterval': 1.0,
    'stringParameter': 'Hello, world',
    'integerParameter': 42,
    'floatParameter': 3.14159,
    'booleanParameter': 'true',
    'listParameter': [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144],
    'mapParameter': { 'one': 1, 'two': 2, 'three': 3, 'many': sys.maxsize }
    }

applicationBundle = './output/SampleApplication.Main.sab'

runInterval = 30

# first, make sure the environment variables needed by the Streams REST API are set

print('checking environment variables ...')
for variable in [ 'STREAMS_INSTALL', 'STREAMING_ANALYTICS_SERVICE_NAME', 'VCAP_SERVICES' ]:
    if variable not in os.environ:
        raise ValueError('environment variable ' + variable + ' not set')

# temporarily, compile a sample application locally until a Python wrapper for the REST build API is available

print('compiling sample application ...')
result = os.system('sc -M ' + applicationName)
if not result==0: raise RuntimeError(applicationName + ' failed to compile, exit code ' + str(result>>8) + ', signal ' + str(result&0xFF))

print('connecting to IBM Cloud ...')
connection = streamsx.rest.StreamingAnalyticsConnection()

print('connecting to service ' + connection.service_name + ' ...')
service = connection.get_streaming_analytics()

print('starting service ' + connection.service_name + ' ...')
result = service.start_instance()
if not result['state']=='STARTED': raise RuntimeError(connection.service_name + ' service did not start')
if not result['status']=='running': raise RuntimeError(connection.service_name + ' service is not running')

print('checking service ' + connection.service_name + ' ...')
instances = connection.get_instances()
if not len(instances)==1: raise RuntimeError(connection.service_name + ' service not found')
instance = instances[0]
if not instance.status=='running': raise RuntimeError(connection.service_name + ' service is not running')
if not instance.health=='healthy': raise RuntimeError(connection.service_name + ' service is not healthy')

print('configuring job for submission ...')
jobConfig = streamsx.topology.context.JobConfig(submission_parameters=applicationParameters, target_pe_count=1, tracing='info')

print('submitting job to service ' + connection.service_name + ' ...')
result = service.submit_job(applicationBundle, configuration=jobConfig)
if 'status_code' in result: raise RuntimeError('submit failed, status code and error description:' + str(result))  
if 'name' not in result: raise RuntimeError('submit failed, no job name returned, ' + str(result))

print('letting the job run a while ...')
time.sleep(runInterval)

print('checking job ' + result['name'] + ' ...')
jobs = instance.get_jobs(result['name'])
if len(jobs)==0: raise RuntimeError('job failed, could not find job')
for job in jobs:
    if not result['name']==job.name: raise RuntimeError('job failed, could not find job')
    if not job.status=='running': raise RuntimeError('job failed, job is not running, status is ' + job.status + ', health is ' + job.health)
    if not job.health=='healthy': raise RuntimeError('job failed, job is not healthy, status is ' + job.status + ', health is ' + job.health)

print('storing logs with default filenames ...')
for job in jobs:
    job.retrieve_logs_traces(dir='logs')
    for pe in job.get_pes():
        pe.retrieve_console_log(dir='logs')
        pe.retrieve_application_trace(dir='logs')

print('storing logs with jobname-based filenames ...')
for job in jobs:
    job.retrieve_logs_traces('./logs/'+job.name+'.tgz')
    for pe in job.get_pes():
        pe.retrieve_console_log('./logs/'+job.name+'.PE_'+pe.id+'.log')
        pe.retrieve_application_trace('./logs/'+job.name+'.PE_'+pe.id+'.trace')

print('canceling job ...')
for job in jobs:
    result = job.cancel()
    if not result: raise RuntimeError('cancel failed for job ' + job.name)

print('cleaning up ...')
os.system('rm -rf logs output __pycache__ *.log *.trace *.tar.gz *.tgz')

print('finished')
sys.exit(0)
