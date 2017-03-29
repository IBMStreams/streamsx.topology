from streamsx.rest_primitives import *

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

def check_job(tc, job):
    """Basic test of calls against an Job """
    _fetch_from_job(tc, job)
    job.refresh()
    _fetch_from_job(tc, job)

def _fetch_from_job(tc, job):
    _check_non_empty_list(tc, job.get_hosts(), Host)
    _check_non_empty_list(tc, job.get_pes(), PE)
    _check_non_empty_list(tc, job.get_operators(), Operator)

def _check_non_empty_list(tc, items, expect_class):
    tc.assertTrue(items)
    print("LIST", items)
    for item in items:
        tc.assertTrue(isinstance(item, expect_class))
