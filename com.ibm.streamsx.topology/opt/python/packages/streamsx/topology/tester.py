# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import streamsx.ec as ec
import streamsx.topology.context as stc
import os
import unittest
import logging
import collections
import threading
from streamsx.rest import StreamsConnection
from streamsx.rest import StreamingAnalyticsConnection
from streamsx.topology.context import ConfigParams
import time

_logger = logging.getLogger('streamsx.topology.test')

class Tester(object):
    """Testing support for a Topology.

    Allows testing of a Topology by creating conditions against the contents
    of its streams.

    Conditions may be added to a topology at any time before submission.

    If a topology is submitted directly to a context then the graph
    is not modified. This allows testing code to be inserted while
    the topology is being built, but not acted upon unless the topology
    is submitted in test mode.

    If a topology is submitted through the test method then the topology
    may be modified to include operations to ensure the conditions are met.

    Args:
        topology: Topology to be tested.
    """
    def __init__(self, topology):
        self.topology = topology
        topology.tester = self
        self._conditions = {}
        self.local_check = None

    @staticmethod
    def setup_standalone(test):
        """
        Setup a unittest.TestCase to run tests using IBM Streams standalone mode.

        Requires a local IBM Streams install define by the STREAMS_INSTALL
        environment variable. If STREAMS_INSTALL is not set then the
        test is skipped.

        Two attributes are set in the test case:
         * test_ctxtype - Context type the test will be run in.
         * test_config- Test configuration.

        Args:
            test(unittest.TestCase): Test case to be set up to run tests using Tester

        Returns: None
        """
        if not 'STREAMS_INSTALL' in os.environ:
            raise unittest.SkipTest("Skipped due to no local IBM Streams install")
        test.test_ctxtype = stc.ContextTypes.STANDALONE
        test.test_config = {}

    @staticmethod
    def setup_distributed(test):
        """
        Setup a unittest.TestCase to run tests using IBM Streams distributed mode.

        Requires a local IBM Streams install define by the STREAMS_INSTALL
        environment variable. If STREAMS_INSTALL is not set then the
        test is skipped.

        The Steams instance to use is defined by the environment variables:
         * STREAMS_ZKCONNECT - Zookeeper connection string
         * STREAMS_DOMAIN_ID - Domain identifier
         * STREAMS_INSTANCE_ID - Instance identifier

        Two attributes are set in the test case:
         * test_ctxtype - Context type the test will be run in.
         * test_config - Test configuration.

        Args:
            test(unittest.TestCase): Test case to be set up to run tests using Tester

        Returns: None
        """
        if not 'STREAMS_INSTALL' in os.environ:
            raise unittest.SkipTest("Skipped due to no local IBM Streams install")

        if not 'STREAMS_INSTANCE_ID' in os.environ:
            raise unittest.SkipTest("Skipped due to STREAMS_INSTANCE_ID environment variable not set")
        if not 'STREAMS_DOMAIN_ID' in os.environ:
            raise unittest.SkipTest("Skipped due to STREAMS_DOMAIN_ID environment variable not set")

        test.username = os.getenv("STREAMS_USERNAME", "streamsadmin")
        test.password = os.getenv("STREAMS_PASSWORD", "passw0rd")

        test.test_ctxtype = stc.ContextTypes.DISTRIBUTED
        test.test_config = {}

    @staticmethod
    def setup_streaming_analytics(test, service_name=None, force_remote_build=False):
        """
        Setup a unittest.TestCase to run tests using Streaming Analytics service on IBM Bluemix cloud platform.

        The service to use is defined by:
         * VCAP_SERVICES environment variable containing `streaming_analytics` entries.
         * service_name which defaults to the value of STREAMING_ANALYTICS_SERVICE_NAME environment variable.

        If VCAP_SERVICES is not set or a service name is not defined then the test is skipped.

        Two attributes are set in the test case:
         * test_ctxtype - Context type the test will be run in.
         * test_config - Test configuration.

        Args:
            test(unittest.TestCase): Test case to be set up to run tests using Tester
            service_name(str): Name of Streaming Analytics service to use. Must exist as an
                entry in the VCAP services. Defaults to value of STREAMING_ANALYTICS_SERVICE_NAME environment variable.

        Returns: None
        """
        if not 'VCAP_SERVICES' in os.environ:
            raise unittest.SkipTest("Skipped due to VCAP_SERVICES environment variable not set")

        test.test_ctxtype = stc.ContextTypes.STREAMING_ANALYTICS_SERVICE
        if service_name is None:
            service_name = os.environ.get('STREAMING_ANALYTICS_SERVICE_NAME', None)
        if service_name is None:
            raise unittest.SkipTest("Skipped due to no service name supplied")
        test.test_config = {'topology.service.name': service_name}
        if force_remote_build:
            test.test_config['topology.forceRemoteBuild'] = True

    def add_condition(self, stream, condition):
        self._conditions[condition.name] = (stream, condition)
        return stream

    def tuple_count(self, stream, count, exact=True):
        """Test that that a stream contains an number of tuples.

        If exact is True then condition becomes valid when count
        tuples are seen on stream during the test. Subsequently if additional
        tuples are seen on stream then the condition fails and can never
        become valid.

        If exact is False then the condition becomes valid once count
        tuples are seen on stream and remains valid regardless of
        any additional tuples.

        Args:
            stream(Stream): Stream to be tested.
            count: Number of tuples expected.
            exact: True if the stream must contain the exactly count of tuples
            to be value, False if the stream must contain at least count tuples.

        Returns: stream

        """
        _logger.debug("Adding tuple count (%d) condition to stream %s.", count, stream)
        if exact:
            name = "ExactCount" + str(len(self._conditions))
            cond = _TupleExactCount(count, name)
        else:
            name = "AtLeastCount" + str(len(self._conditions))
            cond = _TupleAtLeastCount(count, name)
        return self.add_condition(stream, cond)

    def contents(self, stream, expected, ordered=True):
        """Test that a stream contains the expected tuples.

        Args:
            stream(Stream): Stream to be tested.
            expected(list): Sequence of expected tuples.
            ordered(bool): True if the ordering of received tuples must match expected.

        Returns:

        """
        name = "StreamContents" + str(len(self._conditions))
        if ordered:
            cond = _StreamContents(expected, name)
        else:
            cond = _UnorderedStreamContents(expected, name)
        return self.add_condition(stream, cond)

    def tuple_check(self, stream, checker):
        """Check each tuple on a stream.

        For each tuple ``t`` on ``stream`` ``checker(t)`` is called.

        If the return evaluates to ``False`` then the condition fails.
        Once the condition fails it can never become valid.
        Otherwise the condition becomes or remains valid. The first
        tuple on the stream makes the condition valid if the checker
        callable evaluates to true.

        The condition can be combined with :py:meth:`tuple_count` with
        ``exact=False`` to test a stream map or filter with random input data.

        Args:
            stream(Stream): Stream to be tested.
            checker(callable): Callable that must evaluate to True for each tuple.

        """
        name = "TupleCheck" + str(len(self._conditions))
        cond = _TupleCheck(checker, name)
        return self.add_condition(stream, cond)

    def local_check(self, callable):
        self.local_check = callable

    def test(self, ctxtype, config=None, assert_on_fail=True, username=None, password=None):
        """Test the topology.

        Submits the topology for testing and verifies the test conditions are met.

        Args:
            ctxtype(str): Context type for submission.
            config: Configuration for submission.
            assert_on_fail(bool): True to raise an assertion if the test fails, False to return the passed status.
            username(str): username for distributed tests
            password(str): password for distributed tests

        Returns:
            bool: True if test passed, False if test failed.

        """

        # Add the conditions into the graph as sink operators
        _logger.debug("Adding conditions to topology %s.", self.topology.name)
        for ct in self._conditions.values():
            condition = ct[1]
            stream = ct[0]
            stream.for_each(condition, name=condition.name)

        if config is None:
            config = {}
        _logger.debug("Starting test topology %s context %s.", self.topology.name, ctxtype)

        if stc.ContextTypes.STANDALONE == ctxtype:
            passed = self._standalone_test(config)
        elif stc.ContextTypes.DISTRIBUTED == ctxtype:
            passed = self._distributed_test(config, username, password)
        elif stc.ContextTypes.STREAMING_ANALYTICS_SERVICE == ctxtype or stc.ContextTypes.ANALYTICS_SERVICE == ctxtype:
            passed = self._streaming_analytics_test(ctxtype, config)
        else:
            raise NotImplementedError("Tester context type not implemented:", ctxtype)

        if assert_on_fail:
            assert passed, "Test failed for topology: " + self.topology.name
        if passed:
            _logger.info("Test topology %s passed for context:%s", self.topology.name, ctxtype)
        else:
            _logger.error("Test topology %s failed for context:%s", self.topology.name, ctxtype)
        return passed

    def _standalone_test(self, config):
        """ Test using STANDALONE.
        Success is solely indicated by the process completing and returning zero.
        """
        sr = stc.submit(stc.ContextTypes.STANDALONE, self.topology, config)
        self.submission_result = sr
        self.result = {'passed': sr['return_code'], 'submission_result': sr}
        return sr['return_code'] == 0

    def _distributed_test(self, config, username, password):
        self.streams_connection = config.get(ConfigParams.STREAMS_CONNECTION)
        if self.streams_connection is None:
            # Supply a default StreamsConnection object with SSL verification disabled, because the default
            # streams server is not shipped with a valid SSL certificate
            self.streams_connection = StreamsConnection(username, password)
            self.streams_connection.session.verify = False
            config[ConfigParams.STREAMS_CONNECTION] = self.streams_connection
        sjr = stc.submit(stc.ContextTypes.DISTRIBUTED, self.topology, config)
        self.submission_result = sjr
        if sjr['return_code'] != 0:
            _logger.error("Failed to submit job to distributed instance.")
            return False
        return self._distributed_wait_for_result()

    def _streaming_analytics_test(self, ctxtype, config):
        sjr = stc.submit(ctxtype, self.topology, config)
        self.submission_result = sjr
        self.streams_connection = config.get(ConfigParams.STREAMS_CONNECTION)
        if self.streams_connection is None:
            vcap_services = config.get(ConfigParams.VCAP_SERVICES)
            service_name = config.get(ConfigParams.SERVICE_NAME)
            self.streams_connection = StreamingAnalyticsConnection(vcap_services, service_name)
        if sjr['return_code'] != 0:
            _logger.error("Failed to submit job to Streaming Analytics instance")
            return False
        return self._distributed_wait_for_result()

    def _distributed_wait_for_result(self):
        self._start_local_check()
        cc = _ConditionChecker(self, self.streams_connection, self.submission_result)
        self.result = cc._complete()
        self.result['submission_result'] = self.submission_result
        if self.local_check is not None:
            self._local_thread.join()
        cc._canceljob(self.result)
        if self.local_check_exception is not None:
            raise self.local_check_exception
        return self.result['passed']

    def _start_local_check(self):
        self.local_check_exception = None
        if self.local_check is None:
            return
        self._local_thread = threading.Thread(target=self._call_local_check)
        self._local_thread.start()

    def _call_local_check(self):
        try:
            self.local_check_value = self.local_check()
        except Exception as e:
            self.local_check_value = None
            self.local_check_exception = e

class Condition(object):
    _METRIC_PREFIX = "streamsx.condition:"

    @staticmethod
    def _mn(mt, name):
        return Condition._METRIC_PREFIX + mt + ":" + name

    def __init__(self, name=None):
        self.name = name
        self._valid = False
        self._fail = False
    @property
    def valid(self):
        return self._valid
    @valid.setter
    def valid(self, v):
        if self._fail:
           return
        if self._valid != v:
            if v:
                self._metric_valid.value = 1
            else:
                self._metric_valid.value = 0
            self._valid = v
        self._metric_seq += 1

    def fail(self):
        self._metric_fail.value = 1
        self.valid = False
        self._fail = True
        if (ec.is_standalone()):
            raise AssertionError("Condition failed:" + str(self))

    def __getstate__(self):
        # Remove metrics from saved state.
        state = self.__dict__.copy()
        for key in state:
            if key.startswith('_metric'):
              del state[key]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __enter__(self):
        self._metric_valid = self._create_metric("valid", kind='Gauge')
        self._metric_seq = self._create_metric("seq")
        self._metric_fail = self._create_metric("fail", kind='Gauge')
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if (ec.is_standalone()):
            if not self._fail and not self.valid:
                raise AssertionError("Condition failed:" + str(self))

    def _create_metric(self, mt, kind=None):
        return ec.CustomMetric(self, name=Condition._mn(mt, self.name), kind=kind)


class _TupleExactCount(Condition):
    def __init__(self, target, name=None):
        super(_TupleExactCount, self).__init__(name)
        self.target = target
        self.count = 0
        if target == 0:
            self.valid = True

    def __call__(self, tuple):
        self.count += 1
        self.valid = self.target == self.count
        if self.count > self.target:
            self.fail()

    def __str__(self):
        return "Exact tuple count: expected:" + str(self.target) + " received:" + str(self.count)

class _TupleAtLeastCount(Condition):
    def __init__(self, target, name=None):
        super(_TupleAtLeastCount, self).__init__(name)
        self.target = target
        self.count = 0
        if target == 0:
            self.valid = True

    def __call__(self, tuple):
        self.count += 1
        self.valid = self.count >= self.target

    def __str__(self):
        return "At least tuple count: expected:" + str(self.target) + " received:" + str(self.count)

class _StreamContents(Condition):
    def __init__(self, expected, name=None):
        super(_StreamContents, self).__init__(name)
        self.expected = expected
        self.received = []

    def __call__(self, tuple):
        self.received.append(tuple)
        if len(self.received) > len(self.expected):
            self.fail()
            return

        if self._check_for_failure():
            return

        self.valid = len(self.received) == len(self.expected)

    def _check_for_failure(self):
        """Check for failure.
        """
        if self.expected[len(self.received) - 1] != self.received[-1]:
            self.fail()
            return True
        return False

    def __str__(self):
        return "Stream contents: expected:" + str(self.expected) + " received:" + str(self.received)

class _UnorderedStreamContents(_StreamContents):
    def _check_for_failure(self):
        """Unordered check for failure.

        Can only check when the expected number of tuples have been received.
        """
        if len(self.expected) == len(self.received):
            if collections.Counter(self.expected) != collections.Counter(self.received):
                self.fail()
                return True
        return False

class _TupleCheck(Condition):
    def __init__(self, checker, name=None):
        super(_TupleCheck, self).__init__(name)
        self.checker = checker

    def __call__(self, tuple):
        if not self.checker(tuple):
            self.fail()
        else:
            # Will not override if already failed
            self.valid = True

    def __str__(self):
        return "Tuple checker:" + str(self.checker)

#######################################
# Internal functions
#######################################

def _result_to_dict(passed, t):
    result = {}
    result['passed'] = passed
    result['valid'] = t[0]
    result['fail'] = t[1]
    result['progress'] = t[2]
    result['conditions'] = t[3]
    return result

class _ConditionChecker(object):
    def __init__(self, tester, sc, sjr):
        self.tester = tester
        self._sc = sc
        self._sjr = sjr
        self._instance_id = sjr['instanceId']
        self._job_id = sjr['jobId']
        self._sequences = {}
        for cn in tester._conditions:
            self._sequences[cn] = -1
        self.delay = 0.5
        self.timeout = 10.0
        self.waits = 0
        self.additional_checks = 2

        self.job = self._find_job()

    def _complete(self):
        while (self.waits * self.delay) < self.timeout:
            check = self. __check_once()
            if check[1]:
                return self._end(False, check)
            if check[0]:
                if self.additional_checks == 0:
                    return self._end(True, check)
                self.additional_checks -= 1
                continue
            if check[2]:
                self.waits = 0
            else:
                self.waits += 1
            time.sleep(self.delay)
        return self._end(False, check)

    def _end(self, passed, check):
        result = _result_to_dict(passed, check)
        return result

    def _canceljob(self, result):
        if self.job is not None:
            self.job.cancel(force=not result['passed'])

    def __check_once(self):
        cms = self._get_job_metrics()
        valid = True
        progress = True
        fail = False
        condition_states = {}
        for cn in self._sequences:
            condition_states[cn] = 'NotValid'
            seq_mn = Condition._mn('seq', cn)
            # If the metrics are missing then the operator
            # is probably still starting up, cannot be valid.
            if not seq_mn in cms:
                valid = False
                continue
            seq_m = cms[seq_mn]
            if seq_m.value == self._sequences[cn]:
                progress = False
            else:
                self._sequences[cn] = seq_m.value

            fail_mn = Condition._mn('fail', cn)
            if not fail_mn in cms:
                valid = False
                continue

            fail_m = cms[fail_mn]
            if fail_m.value != 0:
                fail = True
                condition_states[cn] = 'Fail'
                continue

            valid_mn =  Condition._mn('valid', cn)

            if not valid_mn in cms:
                valid = False
                continue
            valid_m = cms[valid_mn]

            if valid_m.value == 0:
                valid = False
            else:
                condition_states[cn] = 'Valid'

        return (valid, fail, progress, condition_states)

    def _find_job(self):
        instance = self._sc.get_instance(id=self._instance_id)
        return instance.get_job(id=self._job_id)

    def _get_job_metrics(self):
        """Fetch all the condition metrics for a job.
        We refetch the metrics each time to ensure that we don't miss
        any being added, e.g. if an operator is slow to start.
        """
        cms = {}
        for op in self.job.get_operators():
            metrics = op.get_metrics(name=Condition._METRIC_PREFIX + '*')
            for m in metrics:
                cms[m.name] = m
        return cms
