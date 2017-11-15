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

applicationBundle = './output/TestApplication.Main.sab'

applicationParameters = {
    'iterationInterval': 1.0,
    'stringParameter': 'Hello, world',
    'integerParameter': 42,
    'floatParameter': 3.14159,
    'booleanParameter': 'true',
    'listParameter': [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144],
    'mapParameter': { 'one': 1, 'two': 2, 'three': 3, 'many': sys.maxsize }
    }

runInterval = 30

def environment_variables_set():

    variables = [ 'STREAMS_INSTALL', 'STREAMING_ANALYTICS_SERVICE_NAME', 'VCAP_SERVICES' ]
    result = True

    for variable in variables:
        if variable not in os.environ:
            logger.error(variable + ' environment variable not set')
            result = False

    return result

@unittest.skipIf(not environment_variables_set() , "Streams environment variables not set")
class RestSubmitTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):

        # temporarily compile test application locally until REST API wrapper supports remote build service 
        logger.warning('compile ' + applicationName)
        result = os.system('sc -M ' + applicationName)
        if not result==0: raise unittest.SkipTest(applicationName + ' failed to compile, exit code ' + str(result>>8) + ', signal ' + str(result&0xFF))

        if not os.path.isdir('logs'):
            os.mkdir('logs')

        name = os.environ['STREAMING_ANALYTICS_SERVICE_NAME']

        logger.warning('connect to IBM Cloud')
        self.connection = streamsx.rest.StreamingAnalyticsConnection()

        logger.warning('connect to service ' + name)
        self.service = self.connection.get_streaming_analytics()

        logger.warning('start service ' + name)
        result = self.service.start_instance()
        if not result['state']=='STARTED': raise unittest.SkipTest(name + ' service did not start')
        if not result['status']=='running': raise unittest.SkipTest(name + ' service is not running')

        logger.warning('check service ' + name)
        instances = self.connection.get_instances()
        if not len(instances)==1: raise unittest.SkipTest(name + ' service not found')
        self.instance = instances[0]
        if not self.instance.status=='running': raise unittest.SkipTest(name + ' service is not running')
        if not self.instance.health=='healthy': raise unittest.SkipTest(name + ' service is not healthy')


    def test_A_submit_with_defaults(self):

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

        for job in jobs:
            logger.warning('store logs for job ' + job.name)
            filename = job.get_application_logs()
            self.assertTrue(os.path.isfile(filename), 'get_application_logs() failed, no file stored as ' + filename)
            filename = job.store_logs()
            self.assertTrue(os.path.isfile(filename), 'store_logs() failed, no file stored as ' + filename)
            for pe in job.get_pes():
                logger.warning('store logs for PE ' + pe.id + ' of job ' + job.name)
                filename = pe.store_console_log()
                self.assertTrue(os.path.isfile(filename), 'store_console_log() failed, no file stored as ' + filename)
                filename = pe.store_application_trace()
                self.assertTrue(os.path.isfile(filename), 'store_application_trace() failed, no file stored as ' + filename)

        for job in jobs:
            logger.warning('cancel job ' + job.name)
            result = job.cancel()
            self.assertTrue(result, 'cancel() failed for job ' + job.name)


    def test_B_submit_fully_fused(self):

        jobConfig = streamsx.topology.context.JobConfig(target_pe_count=1, tracing='info')

        jobConfigOverlay = {}
        jobConfig._add_overlays(jobConfigOverlay)

        logger.warning('submit bundle ' + os.path.basename(applicationBundle))
        result = self.service.submit_job(applicationBundle, configuration=jobConfigOverlay)
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

        for job in jobs:
            logger.warning('store logs for job ' + job.name)
            filename = job.get_application_logs()
            self.assertTrue(os.path.isfile(filename), 'get_application_logs() failed, no file stored as ' + filename)
            filename = job.store_logs('mylogs.'+job.name+'.tar.gz')
            self.assertTrue(filename=='mylogs.'+job.name+'.tar.gz', 'store_logs() failed, stored ' + filename + ' instead of ' + 'mylogs.'+job.name+'.tar.gz')
            for pe in job.get_pes():
                logger.warning('store logs for PE ' + pe.id + ' of job ' + job.name)
                filename = pe.store_console_log('mylogs.'+job.name+'.PE_'+pe.id+'.log')
                self.assertTrue(filename=='mylogs.'+job.name+'.PE_'+pe.id+'.log', 'store_console_log() failed, stored ' + filename + ' instead of ' + 'mylogs.'+job.name+'.PE_'+pe.id+'.log')
                filename = pe.store_application_trace('mylogs.'+job.name+'.PE_'+pe.id+'.trace')
                self.assertTrue(filename=='mylogs.'+job.name+'.PE_'+pe.id+'.trace', 'store_appliation_trace() failed, stored ' + filename + ' instead of ' + 'mylogs.'+job.name+'.PE_'+pe.id+'.trace')

        for job in jobs:
            logger.warning('cancel job ' + job.name)
            result = job.cancel()
            self.assertTrue(result, 'cancel() failed for job ' + job.name)


    def test_C_submit_with_parameters(self):

        jobConfig = streamsx.topology.context.JobConfig(submission_parameters=applicationParameters, tracing='info')

        jobConfigOverlay = {}
        jobConfig._add_overlays(jobConfigOverlay)

        logger.warning('submit bundle ' + os.path.basename(applicationBundle))
        result = self.service.submit_job(applicationBundle, configuration=jobConfigOverlay)
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

        for job in jobs:
            logger.warning('store logs for job ' + job.name + ' in subdirectory')
            filename = job.get_application_logs()
            self.assertTrue(os.path.isfile(filename), 'get_application_logs() failed, no file stored as ' + filename)
            filename = job.store_logs('./logs/'+job.name+'.tgz')
            self.assertTrue(filename=='./logs/'+job.name+'.tgz', 'store_logs() failed, stored ' + filename + ' instead of ' + './logs/'+job.name+'.tgz')
            for pe in job.get_pes():
                logger.warning('store logs for PE ' + pe.id + ' of job ' + job.name + ' in subdirectory')
                filename = pe.store_console_log('./logs/'+job.name+'.PE_'+pe.id+'.log')
                self.assertTrue(filename=='./logs/'+job.name+'.PE_'+pe.id+'.log', 'store_console_log() failed, stored ' + filename + ' instead of ' + './logs/'+job.name+'.PE_'+pe.id+'.log')
                filename = pe.store_application_trace('./logs/'+job.name+'.PE_'+pe.id+'.trace')
                self.assertTrue(filename=='./logs/'+job.name+'.PE_'+pe.id+'.trace', 'store_appliation_trace() failed, stored ' + filename + ' instead of ' + './logs/'+job.name+'.PE_'+pe.id+'.trace')

        for job in jobs:
            logger.warning('cancel job ' + job.name)
            result = job.cancel()
            self.assertTrue(result, 'cancel() failed for job ' + job.name)


    @classmethod
    def tearDownClass(self):

        name = os.environ['STREAMING_ANALYTICS_SERVICE_NAME']

        logger.warning('stop service ' + name)
        result = self.service.stop_instance()
        self.assertTrue(result['state']=='STOPPED', 'service did not stop')





