# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import os
import sys
import time
import unittest
import logging
import streamsx.rest
import streamsx.topology.context

logger = logging.getLogger('submit_tests')
logger.setLevel(logging.INFO)

applicationName = 'TestApplication::Main'

applicationParameters = {
    'iterationInterval': 1.0,
    'stringParameter': 'Hello, world',
    'integerParameter': 42,
    'floatParameter': 3.14159,
    'booleanParameter': 'true',
    'listParameter': [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144],
    'mapParameter': { 'one': 1, 'two': 2, 'three': 3, 'many': sys.maxsize }
    }

applicationBundle = './output/TestApplication.Main.sab'

jobConfigFile = 'TestApplication.jobConfig.json'

runInterval = 30

def environment_variables_set():

    variables = [ 'STREAMS_INSTALL', 'STREAMING_ANALYTICS_SERVICE_NAME', 'VCAP_SERVICES' ]
    for variable in variables:
        if variable not in os.environ:
            return False

    return True

@unittest.skipIf(not environment_variables_set() , "Streams environment variables not set")
class RestSubmitTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):

        # temporarily compile test application locally until REST API wrapper supports remote build service 
        logger.warning('compile ' + applicationName)
        result = os.system('sc -M ' + applicationName)
        if not result==0: raise unittest.SkipTest(applicationName + ' failed to compile, exit code ' + str(result>>8) + ', signal ' + str(result&0xFF))

        logger.warning('connect to IBM Cloud')
        self.connection = streamsx.rest.StreamingAnalyticsConnection()

        logger.warning('connect to service ' + self.connection.service_name)
        self.service = self.connection.get_streaming_analytics()

        logger.warning('start service ' + self.connection.service_name)
        result = self.service.start_instance()
        if not result['state']=='STARTED': raise unittest.SkipTest(self.connection.service_name + ' service did not start')
        if not result['status']=='running': raise unittest.SkipTest(self.connection.service_name + ' service is not running')

        logger.warning('check service ' + self.connection.service_name)
        instances = self.connection.get_instances()
        if not len(instances)==1: raise unittest.SkipTest(self.connection.service_name + ' service not found')
        self.instance = instances[0]
        if not self.instance.status=='running': raise unittest.SkipTest(self.connection.service_name + ' service is not running')
        if not self.instance.health=='healthy': raise unittest.SkipTest(self.connection.service_name + ' service is not healthy')


    def testA_submit_with_defaults(self):

        logger.warning('submit bundle ' + os.path.basename(applicationBundle))
        result = self.service.submit_job(applicationBundle)
        self.assertFalse('status_code' in result, 'submit_job() failed, status code and error description:' + str(result))
        self.assertTrue('name' in result, 'submit_job() failed, no job name returned, result: ' + str(result))

        logger.warning('let the job run a while')
        time.sleep(runInterval)

        logger.warning('check job ' + result['name'])
        jobs = self.instance.get_jobs(result['name'])
        self.assertTrue(len(jobs)>0, 'submit_job() failed, could not find job')

        for job in jobs:
            self.assertTrue(result['name']==job.name, 'submit_job() failed, could not find job')
            self.assertTrue(job.status=='running', 'submit_job() failed, job is not running, status is ' + job.status + ', health is ' + job.health)
            self.assertTrue(job.health=='healthy', 'submit_job() failed, job is not healthy, status is ' + job.status + ', health is ' + job.health)

        logger.warning('store logs with default filenames')
        for job in jobs:
            filename = job.retrieve_logs_traces()
            self.assertTrue(os.path.isfile(filename), 'retrieve_logs_traces() failed, no file stored as ' + filename)
            for pe in job.get_pes():
                filename = pe.retrieve_console_log()
                self.assertTrue(os.path.isfile(filename), 'retrieve_console_log() failed, no file stored as ' + filename)
                filename = pe.retrieve_application_trace()
                self.assertTrue(os.path.isfile(filename), 'retrieve_application_trace() failed, no file stored as ' + filename)

        for job in jobs:
            logger.warning('cancel job')
            result = job.cancel()
            self.assertTrue(result, 'cancel() failed for job ' + job.name)


    def testB_submit_fully_fused(self):

        logger.warning('submit bundle ' + os.path.basename(applicationBundle))
        result = self.service.submit_job(applicationBundle, configuration=jobConfigFile)
        self.assertFalse('status_code' in result, 'submit_job() failed, status code and error description:' + str(result))
        self.assertTrue('name' in result, 'submit_job() failed, no job name returned, result: ' + str(result))

        logger.warning('let the job run a while')
        time.sleep(runInterval)

        logger.warning('check job ' + result['name'])
        jobs = self.instance.get_jobs(result['name'])
        self.assertTrue(len(jobs)>0, 'submit_job() failed, could not find job')
        for job in jobs:
            self.assertTrue(result['name']==job.name, 'submit_job() failed, could not find job')
            self.assertTrue(job.status=='running', 'submit_job() failed, job is not running, status is ' + job.status + ', health is ' + job.health)
            self.assertTrue(job.health=='healthy', 'submit_job() failed, job is not healthy, status is ' + job.status + ', health is ' + job.health)
            pes = job.get_pes()
            self.assertTrue(len(pes)==1, 'job configuration failed, job has more than one PE')

        logger.warning('store logs in jobname-based paths')
        for job in jobs:
            filename = job.retrieve_logs_traces('./logs/testB.'+job.name+'.tar.gz')
            self.assertTrue(filename=='./logs/testB.'+job.name+'.tar.gz', 'retrieve_logs_traces() failed, stored ' + filename + ' instead of ' + './logs/testB.'+job.name+'.tar.gz')
            for pe in job.get_pes():
                filename = pe.retrieve_console_log('./logs/testB.'+job.name+'.PE_'+pe.id+'.log')
                self.assertTrue(filename=='./logs/testB.'+job.name+'.PE_'+pe.id+'.log', 'retrieve_console_log() failed, stored ' + filename + ' instead of ' + './logs/testB.'+job.name+'.PE_'+pe.id+'.log')
                filename = pe.retrieve_application_trace('./logs/testB.'+job.name+'.PE_'+pe.id+'.trace')
                self.assertTrue(filename=='./logs/testB.'+job.name+'.PE_'+pe.id+'.trace', 'retrieve_appliation_trace() failed, stored ' + filename + ' instead of ' + './logs/testB.'+job.name+'.PE_'+pe.id+'.trace')

        for job in jobs:
            logger.warning('cancel job')
            result = job.cancel()
            self.assertTrue(result, 'cancel() failed for job ' + job.name)


    def testC_submit_with_parameters(self):

        jobConfig = streamsx.topology.context.JobConfig(submission_parameters=applicationParameters, target_pe_count=1, tracing='info')

        logger.warning('submit bundle ' + os.path.basename(applicationBundle))
        result = self.service.submit_job(applicationBundle, configuration=jobConfig)
        self.assertFalse('status_code' in result, 'submit_job() failed, status code and error description:' + str(result))
        self.assertTrue('name' in result, 'submit_job() failed, no job name returned, result: ' + str(result))

        logger.warning('let the job run a while')
        time.sleep(runInterval)

        logger.warning('check job ' + result['name'])
        jobs = self.instance.get_jobs(result['name'])
        self.assertTrue(len(jobs)>0, 'submit_job() failed, could not find job')
        for job in jobs:
            self.assertTrue(result['name']==job.name, 'submit_job() failed, could not find job')
            self.assertTrue(job.status=='running', 'submit_job() failed, job is not running, status is ' + job.status + ', health is ' + job.health)
            self.assertTrue(job.health=='healthy', 'submit_job() failed, job is not healthy, status is ' + job.status + ', health is ' + job.health)

        logger.warning('store logs in subdirectory with default filenames')
        for job in jobs:
            filename = job.retrieve_logs_traces(dir='logs')
            self.assertTrue(os.path.isfile(filename), 'retrieve_logs_traces() failed, no file stored as ' + filename)
            for pe in job.get_pes():
                filename = pe.retrieve_console_log(dir='logs')
                self.assertTrue(os.path.isfile(filename), 'retrieve_console_log() failed, no file stored as ' + filename)
                filename = pe.retrieve_application_trace(dir='logs')
                self.assertTrue(os.path.isfile(filename), 'retrieve_appliation_trace() failed, no file stored as ' + filename)

        logger.warning('store logs in subdirectory with jobname-based filenames')
        for job in jobs:
            filename = job.retrieve_logs_traces('testC.'+job.name+'.tgz',dir='logs')
            self.assertTrue(filename=='logs/testC.'+job.name+'.tgz', 'retrieve_logs_traces() failed, stored ' + filename + ' instead of ' + 'logs/testC.'+job.name+'.tgz')
            for pe in job.get_pes():
                filename = pe.retrieve_console_log('testC.'+job.name+'.PE_'+pe.id+'.log', dir='logs')
                self.assertTrue(filename=='logs/testC.'+job.name+'.PE_'+pe.id+'.log', 'retrieve_console_log() failed, stored ' + filename + ' instead of ' + 'logs/testC.'+job.name+'.PE_'+pe.id+'.log')
                filename = pe.retrieve_application_trace('testC.'+job.name+'.PE_'+pe.id+'.trace', dir='logs')
                self.assertTrue(filename=='logs/testC.'+job.name+'.PE_'+pe.id+'.trace', 'retrieve_appliation_trace() failed, stored ' + filename + ' instead of ' + 'logs/testC.'+job.name+'.PE_'+pe.id+'.trace')

        for job in jobs:
            logger.warning('cancel job')
            result = job.cancel()
            self.assertTrue(result, 'cancel() failed for job ' + job.name)


    @classmethod
    def tearDownClass(self):

        #logger.warning('stop service ' + self.connection.service_name)
        #result = self.service.stop_instance()
        #self.assertTrue(result['state']=='STOPPED', 'service did not stop')

        logger.warning('cleanup')
        result = os.system('rm -rf logs output __pycache__ *.log *.trace *.tar.gz *.tgz')

        pass



