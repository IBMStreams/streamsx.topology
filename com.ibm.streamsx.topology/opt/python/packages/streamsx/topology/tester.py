import streamsx.ec as ec
import streamsx.topology.context

class Tester(object):
    def __init__(self, topology):
       self.topology = topology
       topology.tester = self
       self._conditions = {}

    def add_condition(self, stream, condition):
        self._conditions[condition.name] = (stream, condition)

    def tuple_count(self, stream, count):
        name = "ExactCount" + str(len(self._conditions));
        cond = TupleExactCount(count, name)
        self.add_condition(stream, cond)

    def stream_contents(self, stream, expected):
        name = "StreamContents" + str(len(self._conditions));
        cond = StreamContents(expected, name)
        self.add_condition(stream, cond)

    def submit(self, config=None):

        # Add the conditions into the graph
        for ct in self._conditions.values():
            condition = ct[1]
            stream = ct[0]
            stream.for_each(condition, name=condition.name)

        if config is None:
            config = {}

        streamsx.topology.context.submit("STANDALONE", self.topology, config)

        #streamsx.topology.context.submit("DISTRIBUTED", self.topology, config)
        #_resource_url = "https://streamsqse.localdomain:8443/streams/rest/resources"
        #sc = StreamsConnection(username="streamsadmin", password="passw0rd", resource_url=_resource_url)

        #cc = _ConditionChecker(self, sc)
        #cc._complete()

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
           return None
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


class TupleExactCount(Condition):
    def __init__(self, target, name=None):
        super(TupleExactCount, self).__init__(name)
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


class StreamContents(Condition):
    def __init__(self, expected, name=None):
        super(StreamContents, self).__init__(name)
        self.expected = expected
        self.received = []

    def __call__(self, tuple):
        self.received.append(tuple)
        if len(self.received) > len(self.expected):
            self.fail()
            return None

        if self.expected[len(self.received) - 1] != self.received[-1]:
            self.fail()
            return None

        self.valid = len(self.received) == len(self.expected)

    def __str__(self):
        return "Stream contents: expected:" + str(self.expected) + " received:" + str(self.received)


#######################################
# Internal functions
#######################################

from streamsx.rest import StreamsConnection
import time

class _ConditionChecker(object):
    def __init__(self, tester, sc):
        self.sc = sc
        self.tester = tester
        self._pms = {}
        for cn in tester._conditions:
            self._pms[cn] = -1
        self.delay = 0.5
        self.timeout = 10.0
        self.waits = 0
        self.additional_checks = 2

    def _complete(self):
        while (self.waits * self.delay) < self.timeout:
            check = self. __check_once()
            if check[1]:
                return False
            if check[0]:
                if self.additional_checks == 0:
                    return True
                self.additional_checks -= 1
                continue
            if check[2]:
                self.waits = 0
            else:
                self.waits += 1
            time.sleep(self.delay)

    def __check_once(self):
        cms = self._get_job_metrics()
        valid = True
        progress = True
        fail = False
        for cn in self._pms:
            seq_mn = Condition._mn('seq', cn)
            if not seq_mn in cms:
                valid = False
                continue
            seq_m = cms[seq_mn]
            if seq_m.value == self._pms[cn]:
                progress = False
            else:
                self._pms[cn] = seq_m.value

            fail_mn = Condition._mn('fail', cn)
            if not fail_mn in cms:
                valid = False
                continue
            fail_m = cms[fail_mn]
            if fail_m.value != 0:
                fail = True

            valid_mn =  Condition._mn('valid', cn)

            if not valid_mn in cms:
                valid = False
                continue
            valid_m = cms[valid_mn]

            if valid_m.value == 0:
                valid = False

        return (valid, fail, progress)


    def _get_job_metrics(self):
        cms = {}
        for instance in self.sc.get_instances():
            for j in instance.get_jobs():
                for op in j.get_operators():
                    metrics = op.get_metrics(name=Condition._METRIC_PREFIX + '*')
                    if not metrics:
                        continue
                    for m in metrics:
                        cms[m.name] = m
        return cms
