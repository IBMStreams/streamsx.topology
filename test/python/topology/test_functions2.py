# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
from __future__ import print_function
from test_functions2_dep import filter2

def hello_world() :
    return ["Hello", "World!"]

def filter(t):
   return filter2(t) 

global _hwcount_filter
_hwcount_filter = 0
def check_hello_world_filter(t):
   print("TUPLE", t)
   global _hwcount_filter
   if _hwcount_filter == 0:
      if t != "Hello":
          raise AssertionError()
      _hwcount_filter += 1
      return None
   raise AssertionError()
