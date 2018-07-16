from streamsx.rest_primitives import *
import os
import random
import shutil
import tempfile

def check_instance(tc, instance):
    """Basic test of calls against an instance, assumed there is
    at least one job running.
    """
    _fetch_from_instance(tc, instance)
    instance.refresh()
    _fetch_from_instance(tc, instance)
    _check_resource_allocations(tc, instance)

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

def _check_operators(tc, ops):
    for op in ops:
         tc.assertIsInstance(op.operatorKind, str)
         tc.assertIsInstance(op.name, str)
         pe = op.get_pe()
         tc.assertIsInstance(pe, PE)

         _check_metrics(tc, op)

         outs = op.get_output_ports()
         tc.assertIsInstance(outs, list)
         for out in outs:
             tc.assertIsInstance(out, OperatorOutputPort)
             _check_metrics(tc, out)

         ins = op.get_input_ports()
         tc.assertIsInstance(ins, list)
         for in_ in ins:
             tc.assertIsInstance(in_, OperatorInputPort)
             _check_metrics(tc, in_)
        
         host_op = op.get_host()
         host_pe = pe.get_host()
         # container based instances return None for get_host
         if host_op is not None:
             tc.assertIsInstance(host_op, Host)
         else:
             tc.assertIsNone(host_pe)
         if host_pe is not None:
             tc.assertIsInstance(host_pe, Host)
             tc.assertEqual(host_op.ipAddress, host_pe.ipAddress)
         else:
             tc.assertIsNone(host_op)

def check_job(tc, job):
    """Basic test of calls against an Job """
    _fetch_from_job(tc, job)
    job.refresh()
    _fetch_from_job(tc, job)

def _check_metrics(tc, obj):
    metrics = obj.get_metrics()
    tc.assertIsInstance(metrics, list)
    for m in metrics:
        tc.assertIsInstance(m, Metric)
        tc.assertIsInstance(m.name, str)
        tc.assertIsInstance(m.value, int)

def _check_resource_allocations(tc, obj):
    for ra in obj.get_resource_allocations():
        _check_resource_allocation(tc, ra)

def _check_resource_allocation(tc, ra):
    tc.assertIsInstance(ra, ResourceAllocation)
    tc.assertIsInstance(ra.applicationResource, bool)
    tc.assertIsInstance(ra.schedulerStatus, str)
    tc.assertIsInstance(ra.status, str)

    r = ra.get_resource()
    tc.assertIsInstance(r.id, str)
    tc.assertIsInstance(r.displayName, str)
    tc.assertIsInstance(r.ipAddress, str)
    tc.assertIsInstance(r.status, str)
    tc.assertIsInstance(r.tags, list)
    _check_metrics(tc, r)

    for pe in ra.get_pes():
        tc.assertIsInstance(pe, PE)
    for job in ra.get_jobs():
        tc.assertIsInstance(job, Job)
        
def _fetch_from_job(tc, job):
    _check_non_empty_list(tc, job.get_pes(), PE)
    ops = job.get_operators()
    _check_non_empty_list(tc, ops, Operator)
    _check_operators(tc, ops)
    _check_resource_allocations(tc, job)

    pes = job.get_pes()
    for pe in pes:
        tc.assertIsInstance(pe, PE)
        _check_metrics(tc, pe)
     
    _check_non_empty_list(tc, job.get_views(), View)
    _check_non_empty_list(tc, job.get_operator_connections(), OperatorConnection)

    # See issue 952
    if tc.test_ctxtype != 'STREAMING_ANALYTICS_SERVICE' or tc.is_v2:
        _check_non_empty_list(tc, job.get_resource_allocations(), ResourceAllocation)

    # Presently, application logs can only be fetched from the Stream Analytics Service
    if tc.test_ctxtype == 'STREAMING_ANALYTICS_SERVICE':
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

        # PE
        pe = job.get_pes()[0]
        _check_resource_allocation(tc, pe.get_resource_allocation())

        trace = pe.retrieve_trace()
        tc.assertTrue(os.path.isfile(trace))
        fn = os.path.basename(trace)
        tc.assertTrue(fn.startswith('pe_' + pe.id + '_'))
        tc.assertTrue(fn.endswith('.trace'))
        tc.assertEqual(os.getcwd(), os.path.dirname(trace))
        os.remove(trace)

        fn = 'mypetrace_' + str(random.randrange(999999)) + '.txt'
        trace = pe.retrieve_trace(fn)
        tc.assertTrue(os.path.isfile(trace))
        tc.assertEqual(fn, os.path.basename(trace))
        tc.assertEqual(os.getcwd(), os.path.dirname(trace))
        os.remove(trace)

        trace = pe.retrieve_trace(dir=td)
        tc.assertTrue(os.path.isfile(trace))
        fn = os.path.basename(trace)
        tc.assertTrue(fn.startswith('pe_' + pe.id + '_'))
        tc.assertTrue(fn.endswith('.trace'))
        tc.assertEqual(td, os.path.dirname(trace))
        os.remove(trace)

        fn = 'mypetrace_' + str(random.randrange(999999)) + '.txt'
        trace = pe.retrieve_trace(filename=fn,dir=td)
        tc.assertTrue(os.path.isfile(trace))
        tc.assertEqual(fn, os.path.basename(trace))
        tc.assertEqual(td, os.path.dirname(trace))
        os.remove(trace)

        # PE console log

        console = pe.retrieve_console_log()
        tc.assertTrue(os.path.isfile(console))
        fn = os.path.basename(console)
        tc.assertTrue(fn.startswith('pe_' + pe.id + '_'))
        tc.assertTrue(fn.endswith('.stdouterr'))
        tc.assertEqual(os.getcwd(), os.path.dirname(console))
        os.remove(console)

        fn = 'mypeconsole' + str(random.randrange(999999)) + '.txt'
        console = pe.retrieve_console_log(fn)
        tc.assertTrue(os.path.isfile(console))
        tc.assertEqual(fn, os.path.basename(console))
        tc.assertEqual(os.getcwd(), os.path.dirname(console))
        os.remove(console)

        console = pe.retrieve_console_log(dir=td)
        tc.assertTrue(os.path.isfile(console))
        fn = os.path.basename(console)
        tc.assertTrue(fn.startswith('pe_' + pe.id + '_'))
        tc.assertTrue(fn.endswith('.stdouterr'))
        tc.assertEqual(td, os.path.dirname(console))
        os.remove(console)

        fn = 'mypeconsole' + str(random.randrange(999999)) + '.txt'
        console = pe.retrieve_console_log(filename=fn,dir=td)
        tc.assertTrue(os.path.isfile(console))
        tc.assertEqual(fn, os.path.basename(console))
        tc.assertEqual(td, os.path.dirname(console))
        os.remove(console)

        shutil.rmtree(td)

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
        _check_resource_allocations(tc, domain)
    _check_non_empty_list(tc, domain.get_resources(), Resource)

    _check_list(tc, domain.get_hosts(), Host)

def _check_non_empty_list(tc, items, expect_class):
    tc.assertTrue(items)
    _check_list(tc, items, expect_class)

def _check_list(tc, items, expect_class):
    for item in items:
        tc.assertIsInstance(item, expect_class)
