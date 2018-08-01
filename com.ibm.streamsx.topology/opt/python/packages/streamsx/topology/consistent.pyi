from typing import Union
from enum import Enum
from datetime import timedelta

class ConsistentRegionConfig(object):
    class Trigger(Enum):
        OPERATOR_DRIVEN = 1
        PERIODIC = 2

    def operator_driven(drainTimeout:Union[timedelta, float, None]=None, resetTimeout:Union[timedelta, float, None]=None, maxConsecutiveAttempts:Union[int, None]=None) -> 'ConsistentRegionConfig': ...

    def periodic(period:Union[timedelta, float], drainTimeout:Union[timedelta, float, None]=None, resetTimeout:Union[timedelta, float, None]=None, maxConsecutiveAttempts:Union[int, None]=None) -> 'ConsistentRegionConfig': ...
