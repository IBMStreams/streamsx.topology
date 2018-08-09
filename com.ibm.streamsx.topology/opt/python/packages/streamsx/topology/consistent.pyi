from typing import Union
from enum import Enum
from datetime import timedelta

class ConsistentRegionConfig(object):
    class Trigger(Enum):
        OPERATOR_DRIVEN = 1
        PERIODIC = 2

    def operator_driven(drainTimeout:Union[timedelta, float]=180, resetTimeout:Union[timedelta, float]=180, maxConsecutiveAttempts:int=5) -> 'ConsistentRegionConfig': ...

    def periodic(period:Union[timedelta, float], drainTimeout:Union[timedelta, float]=180, resetTimeout:Union[timedelta, float]=180, maxConsecutiveAttempts:int=5) -> 'ConsistentRegionConfig': ...
