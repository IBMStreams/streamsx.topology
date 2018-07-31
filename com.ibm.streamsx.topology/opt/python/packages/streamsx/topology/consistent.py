from enum import Enum
from datetime import timedelta

# TODO documentation
class ConsistentRegionConfig(object):
    class Trigger(Enum):
        OPERATOR_DRIVEN = 1
        PERIODIC = 2

    def __init__(self, trigger=None, period=None, drainTimeout=None, resetTimeout=None, maxConsecutiveAttempts=None):

        # period cannot be specified for OPERATOR_DRIVEN
        # (This can only happen if someone calls this constructor
        # directly instead of using the periodic and operator_driven
        # methods.
        if trigger == ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN and period is not None:
            raise ValueError("period does not apply to an operator driven consistent region")

        if trigger == ConsistentRegionConfig.Trigger.PERIODIC:
            if period is None:
                raise ValueError("period must be specified for a consistent region with periodic trigger.")
            elif isinstance(period, timedelta):
                if period.total_seconds() <= 0.0:
                    raise ValueError("period must be greater than zero.")
            elif float(period) <= 0.0:
                raise ValueError("period must be greater than zero.")


        # drainTimeout and resetTimeout must be timedelta values, or must be castable to
        # float, and both must be greater than 0.
        if drainTimeout is None:
            pass
        elif isinstance(drainTimeout, timedelta):
            if drainTimeout.total_seconds() <= 0.0:
                raise ValueError("drain timeout value must be greater than zero.")
        elif float(drainTimeout) <= 0.0:
            raise ValueError("drain timeout value must be greater than zero.")


        if resetTimeout is None:
            pass
        elif isinstance(resetTimeout, timedelta):
            if resetTimeout.total_seconds() <= 0.0:
                raise ValueError("reset timeout value must be greater than zero.")
        elif float(resetTimeout) <= 0.0:
            raise ValueError("reset timeout value must be greater than zero.")


        # maxConsecutiveAttempts must be 1-0x7FFFFFFF.  It also must be an int.
        if maxConsecutiveAttempts is None:
            pass
        elif int(maxConsecutiveAttempts) < 1 or int(maxConsecutiveAttempts) > 0x7FFFFFFF:
            raise ValueError("maxConsecutiveAttempts must be between 1 and " + str(0x7FFFFFFF) + ", inclusive.")
        elif not float(maxConsecutiveAttempts).is_integer():
            raise ValueError("maxConsecutiveAttempts must be an integer value.")

        self.trigger = trigger
        self.period = period
        self.drainTimeout = drainTimeout if drainTimeout is not None else 180
        self.resetTimeout = resetTimeout if resetTimeout is not None else 180
        self.maxConsecutiveAttempts = maxConsecutiveAttempts if maxConsecutiveAttempts is not None else 5

    def operator_driven(drainTimeout=None, resetTimeout=None, maxConsecutiveAttempts=None):
        return ConsistentRegionConfig(trigger=ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN, drainTimeout=drainTimeout, resetTimeout=resetTimeout, maxConsecutiveAttempts=maxConsecutiveAttempts)

    def periodic(period, drainTimeout=None, resetTimeout=None, maxConsecutiveAttempts=None):
        return ConsistentRegionConfig(trigger=ConsistentRegionConfig.Trigger.PERIODIC, period=period, drainTimeout=drainTimeout, resetTimeout=resetTimeout, maxConsecutiveAttempts=maxConsecutiveAttempts)
