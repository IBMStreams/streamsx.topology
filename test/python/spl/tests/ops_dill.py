# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
from streamsx.spl import spl

@spl.filter()
class F1(object):
    def __init__(self, v):
        self.c = 0
        self.v = v

    def __call__(self, *tuple_):
        self.c += 1
        return tuple_[0] < self.v

@spl.filter()
class F2(object):
    def __init__(self, v):
        self.c = 42
        self.v = v

    def __call__(self, *tuple_):
        self.c += 1
        return tuple_[0] < self.v

    def __getstate__(self):
        return {'c': self.c + 99, 'v': self.v}

        
@spl.map()
class M1(object):
    def __init__(self, v):
        self.c = 0
        self.v = v

    def __call__(self, *tuple_):
        self.c += 1
        return tuple_[0] + self.v,
        

@spl.map()
class M2(object):
    def __init__(self, v):
        self.c = 0
        self.v = v

    def __call__(self, *tuple_):
        self.c += 1
        return tuple_[0] + self.v,
        
    def __getstate__(self):
        return {'c': self.c + 43, 'v': self.v}


@spl.for_each()
class FE1(object):
    def __init__(self, v):
        self.c = 0
        self.v = v

    def __call__(self, *tuple_):
        self.c += self.v + tuple_[0]
        

@spl.map()
class FE2(object):
    def __init__(self, v):
        self.c = 0
        self.v = v

    def __call__(self, *tuple_):
        self.c += self.v + tuple_[0]
        
    def __getstate__(self):
        return {'c': self.c + 74, 'v': self.v}
