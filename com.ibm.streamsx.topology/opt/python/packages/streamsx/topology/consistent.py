from enum import Enum
from datetime import timedelta

"""
Consistent region configuration

***************
Module contents
***************

"""

class ConsistentRegionConfig(object):
    """
    A :py:class:`ConsistentRegionConfig` defines a consistent region.
    
    The recommended way to create a :py:class:`ConsistentRegionConfig` is
    to call either :py:meth:`.operator_driven` or :py:meth:`.periodic`.
    
    Args:
        trigger(ConsistentRegionConfig.Trigger): Determines how the 
            drain/checkpoint cycle of the consistent region is triggered.
        period: The trigger period.  If the trigger is :py:const:`~ConsistentRegionConfig.Trigger.PERIODIC`, this must 
            be specified, otherwise it may not be specfied.  This may be 
            either a :py:class:`datetime.timedelta` value or the number of 
            seconds as a `float`.
        drainTimeout: Indicates the maximum time in seconds that the drain 
            and checkpoint of the region is allotted to finish processing. 
            If the process takes longer than the specified time, a failure 
            is reported and the region is reset to the point of the 
            previously successfully established consistent state. The value
            must be specified as either a 
            :py:class:`datetime.timedelta` value or the number of seconds 
            as a `float`.  If not specified, the default value is 180 
            seconds.
        resetTimeout: Indicates the maximum time in seconds that the reset
            of the region is allotted to finish processing. If the process
            takes longer than the specified time, a failure is reported and
            another reset of the region is attempted.  The value must be
            specified  as either a :py:class:`datetime.timedelta` value or 
            the number of seconds as a `float`.  If not specified, the 
            default value is 180 seconds.
        maxConsecutiveAttempts(int): Indicates the maximum number of 
            consecutive attempts to reset a consistent region. After a 
            failure, if the maximum number of attempts is reached, the 
            region stops processing new tuples. After the maximum number 
            of consecutive attempts is reached, a region can be reset only 
            with manual intervention or with a program with a call to a 
            method in the consistent region controller.  This must be an 
            integer value between 1 and 2147483647, inclusive.  If not 
            specified, the default value is 5.

    Example:
    ::
        # set source to be a the start of an operator driven consistent region
        # with a drain timeout of five seconds and a reset timeout of twenty seconds.
        source.set_consistent(ConsistentRegionConfig.operatorDriven(drainTimeout=5, resetTimeout=20)

    .. seealso:: :py:meth:`~streamsx.topology.topology.Stream.set_consistent`

    """

    class Trigger(Enum):
        """
        Defines how the drain-checkpoint cycle of a consistent region is triggered.
        """
        OPERATOR_DRIVEN = 1
        """
        Region is triggered by the start operator.
        """
        PERIODIC = 2
        """
        Region is triggered periodically.
        """

    def __init__(self, trigger=None, period=None, drainTimeout=None, resetTimeout=None, maxConsecutiveAttempts=None):

        # period cannot be specified for OPERATOR_DRIVEN
        # (This can only happen if someone creates an instance 
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
        """Define an operator-driven consistent region configuration.  
        The source operator triggers drain and checkpoint cycles for the region.

        Args:
          drainTimeout: The drain timeout, as either a :py:class:`datetime.timedelta` value or the number of seconds as a `float`.  If not specified, the default value is 180 seconds.
          resetTimeout: The reset timeout, as either a :py:class:`datetime.timedelta` value or the number of seconds as a `float`.  If not specified, the default value is 180 seconds.
          maxConsecutiveAttempts(int): The maximum number of consecutive attempts to reset the region.  This must be an integer value between 1 and 2147483647, inclusive.  If not specified, the default value is 5.

        Returns:
          ConsistentRegionConfig: the configuration.
        """

        return ConsistentRegionConfig(trigger=ConsistentRegionConfig.Trigger.OPERATOR_DRIVEN, drainTimeout=drainTimeout, resetTimeout=resetTimeout, maxConsecutiveAttempts=maxConsecutiveAttempts)

    def periodic(period, drainTimeout=None, resetTimeout=None, maxConsecutiveAttempts=None):
        """Create a periodic consistent region configuration.
        The IBM Streams runtime will trigger a drain and checkpoint
        the region periodically at the time interval specified by `period`.
        
        Args:
          period: The trigger period.  This may be either a :py:class:`datetime.timedelta` value or the number of seconds as a `float`.
          drainTimeout: The drain timeout, as either a :py:class:`datetime.timedelta` value or the number of seconds as a `float`.  If not specified, the default value is 180 seconds.
          resetTimeout: The reset timeout, as either a :py:class:`datetime.timedelta` value or the number of seconds as a `float`.  If not specified, the default value is 180 seconds.
          maxConsecutiveAttempts(int): The maximum number of consecutive attempts to reset the region.  This must be an integer value between 1 and 2147483647, inclusive.  If not specified, the default value is 5.

        Returns:
          ConsistentRegionConfig: the configuration.
        """

        return ConsistentRegionConfig(trigger=ConsistentRegionConfig.Trigger.PERIODIC, period=period, drainTimeout=drainTimeout, resetTimeout=resetTimeout, maxConsecutiveAttempts=maxConsecutiveAttempts)
