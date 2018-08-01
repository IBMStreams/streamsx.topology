from typing import Union

class ConsistentRegionConfig(object):
    class Trigger(Enum):
        OPERATOR_DRIVEN = 1
        PERIODIC = 2

    def operator_driven(drainTimeout:Union[datetime.timedelta, float, None]=None, resetTimeout:Union[datetime.timedelta, float, None]=None, maxConsecutiveAttempts:Union[int, None]=None) -> 'ConsistentRegionConfig': ...

    def periodic(period:Union[datetime.timedelta, float], drainTimeout:Union[datetime.timedelta, float, None]=None, resetTimeout:Union[datetime.timedelta, float, None]=None, maxConsecutiveAttempts:Union[int, None]=None) -> 'ConsistentRegionConfig': ...
