import random
from time import sleep

class randomWalk(object):
    def __init__(self):
        self.num = 0.0
     
    def __call__(self):
        while True:
            self.num += (random.random()-0.5)/10.0
            yield self.num
            sleep(0.05)
            
class jsonRandomWalk(object):
    def __init__(self):
        self.num = 0.0
     
    def __call__(self):
        while True:
            self.num += (random.random()-0.5)/10.0
            yield {'val': float(self.num)}
            sleep(0.05)
            
class movingAverage(object):
    def __init__(self, lookback):
        self.lookback = lookback
        self._tuples = []
        
    def __call__(self, tup):
        self._tuples.insert(0, tup)
        if len(self._tuples) >= self.lookback:
            self._tuples.pop()
        return {'val' : self.calc_average()}
        
    def calc_average(self):
        _sum = 0
        for i in self._tuples:
            _sum += i['val']
        return _sum / len(self._tuples)