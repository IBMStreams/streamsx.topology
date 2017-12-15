from streamsx.rest_primitives import *
import os
import random
import tempfile

def check_instance(tc, instance):
    """Basic test of calls against an instance, assumed there is
    at least one job running.
    """
    _fetch_from_instance(tc, instance)
    instance.refresh()
    _fetch_from_instance(tc, instance)

def _fetch_from_instance(tc, instance):    
    _check_non_empty_list(tc, instance.get_pes(), PE)
    _check_non_empty_list(tc, instance.get_jobs(), Job)
    _check_non_empty_list(tc, instance.get_operators(), Operator)
    _check_non_empty_list(tc, instance.get_views(), View)
    _check_non_empty_list(tc, instance.get_operator_connections(), OperatorConnection)
    _check_non_empty_list(tc, instance.get_resource_allocations(), ResourceAllocation)
    _check_non_empty_list(tc, instance.get_active_services(), ActiveService)

    _check_list(tc, instance.get_exported_streams(), ExportedStream)
    _check_list(tc, instance.get_hosts(), Host)
    _check_list(tc, instance.get_imported_streams(), ImportedStream)
    _check_list(tc, instance.get_pe_connections(), PEConnection)

    tc.assertIsInstance(instance.get_domain(), Domain)

def check_job(tc, job):
    """Basic test of calls against an Job """
    _fetch_from_job(tc, job)
    job.refresh()
    _fetch_from_job(tc, job)

def _fetch_from_job(tc, job):
    _check_non_empty_list(tc, job.get_pes(), PE)
    _check_non_empty_list(tc, job.get_operators(), Operator)
    _check_non_empty_list(tc, job.get_views(), View)
    _check_non_empty_list(tc, job.get_operator_connections(), OperatorConnection)

    # See issue 952
    if tc.test_ctxtype != 'STREAMING_ANALYTICS_SERVICE' or tc.is_v2:
        _check_non_empty_list(tc, job.get_resource_allocations(), ResourceAllocation)

    # Presently, application logs can only be fetched from the Stream Analytics Service

    else:
        logs = job.retrieve_log_trace()
        tc.assertTrue(os.path.isfile(logs))
        fn = os.path.basename(logs)
        tc.assertTrue(fn.startswith('job_' + job.id + '_'))
        tc.assertTrue(fn.endswith('.tar.gz'))
        tc.assertEqual(os.getcwd(), os.path.dirname(logs))
        os.remove(logs)

        fn = 'myjoblogs_' + str(random.randrange(999999)) + '.tgz'
        logs = job.retrieve_log_trace(fn)
        tc.assertTrue(os.path.isfile(logs))
        tc.assertEqual(fn, os.path.basename(logs))
        tc.assertEqual(os.getcwd(), os.path.dirname(logs))
        os.remove(logs)

        td = tempfile.mkdtemp()

        logs = job.retrieve_log_trace(dir=td)
        tc.assertTrue(os.path.isfile(logs))
        fn = os.path.basename(logs)
        tc.assertTrue(fn.startswith('job_' + job.id + '_'))
        tc.assertTrue(fn.endswith('.tar.gz'))
        tc.assertEqual(td, os.path.dirname(logs))
        os.remove(logs)

        fn = 'myjoblogs_' + str(random.randrange(999999)) + '.tgz'
        logs = job.retrieve_log_trace(filename=fn,dir=td)
        tc.assertTrue(os.path.isfile(logs))
        tc.assertEqual(fn, os.path.basename(logs))
        tc.assertEqual(td, os.path.dirname(logs))
        os.remove(logs)

    _check_list(tc, job.get_hosts(), Host)
    _check_list(tc, job.get_pe_connections(), PEConnection)

    tc.assertIsInstance(job.get_instance(), Instance)
    tc.assertIsInstance(job.get_domain(), Domain)

def check_domain(tc, domain):
    """Basic test of calls against an Domain """
    _fetch_from_domain(tc, domain)
    domain.refresh()
    _fetch_from_domain(tc, domain)

def _fetch_from_domain(tc, domain):
    _check_non_empty_list(tc, domain.get_instances(), Instance)
    _check_non_empty_list(tc, domain.get_active_services(), ActiveService)
    
    # See issue 952
    if tc.test_ctxtype != 'STREAMING_ANALYTICS_SERVICE':
        _check_non_empty_list(tc, domain.get_resource_allocations(), ResourceAllocation)
    _check_non_empty_list(tc, domain.get_resources(), Resource)

    _check_list(tc, domain.get_hosts(), Host)

def _check_non_empty_list(tc, items, expect_class):
    tc.assertTrue(items)
    _check_list(tc, items, expect_class)

def _check_list(tc, items, expect_class):
    for item in items:
        tc.assertIsInstance(item, expect_class)
