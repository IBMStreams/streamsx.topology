import time

from streamsx.spl import spl

def spl_namespace():
    return "com.ibm.streamsx.topology.pytest.mt"

@spl.source()
class MTSource(object):
    def __init__(self, N):
        self.a = 0
        self.b = 0
        self.count = 0
        self.N = int(N)

    def __iter__(self):
        return self

    def __next__(self):
        self.a += 1
        time.sleep(0.0001)
        self.b += 1
        if self.a == self.b:
            if self.count == self.N:
                raise StopIteration()
            v = self.count
            self.count += 1
            return v,
        return None

    def next(self):
        return self.__next__()

@spl.filter()
class MTFilter(object):
    def __init__(self):
        self.a = 0
        self.b = 0

    def __call__(self, *t):
        self.a += 1
        time.sleep(0.001)
        self.b += 1
        return self.a == self.b

@spl.map()
class MTMap(object):
    def __init__(self):
        self.a = 0
        self.b = 0

    def __call__(self, *t):
        self.a += 1
        time.sleep(0.001)
        self.b += 1
        return t if self.a == self.b else None

@spl.for_each()
class MTForEach(object):
    def __init__(self):
        self.a = 0
        self.b = 0

    def __call__(self, *t):
        self.a += 1
        time.sleep(0.001)
        self.b += 1
        if self.a != self.b:
            raise ValueError('MTForEach:' + str(self.a) + " != " + str(self.b))

