from enum import Enum

class ConsistentRegionConfig(object):
    class Trigger(Enum):
        OPERATOR_DRIVEN = 1
        PERIODIC = 2

    # def __init__(self, trigger, period):
    #     self.trigger = trigger
    #     self.period = period
    #     self.drain = 180
    #     self.reset = 180
    #     self.attempts = 5

    def __init__(self, trigger=None, period=None, old=None, drain=None, reset=None, attempts=None):
        assert (trigger is not None and old is None) or (trigger is None and old is not None)
        
        if trigger is not None:
            # Initial construction
            self.trigger = trigger
            self.period = period
            self.drain = drain if drain is not None else 180
            self.reset = reset if reset is not None else 180
            self.attempts = attepts if attempts is not None else 5
        else:
            self.trigger = old.trigger
            self.period = old.period
            self.drain = old.drain if drain is None else drain
            self.reset = old.reset if reset is None else reset
            self.attempts = old.attempts if attempts is None else attempts

    def operator_driven():
        return ConsistentRegionConfig(trigger=ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN)
    

    def periodic(period):
        return ConsistentRegionConfig(trigger=ConsistentRegionConfig.Trigger.PERIODIC, period=period)

    def drainTimeout(self, drainTimeout):
        return ConsistentRegionConfig(old=self, drain=drainTimeout)

    def resetTimeout(self, resetTimeout):
        return ConsistentRegionConfig(old=self, reset=resetTimeout)

    def maxConsecutiveAttempts(self, attempts):
        return ConsistentRegionConfig(old=self, attempts=attempts)

