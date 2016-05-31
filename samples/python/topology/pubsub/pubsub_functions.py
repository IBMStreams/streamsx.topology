import itertools
import time

def sequence():
    return itertools.count()

def delay(v):
    time.sleep(0.1)
    return True
