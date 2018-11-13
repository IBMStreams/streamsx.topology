# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

from streamsx.spl import spl

# Only loaded during extraction so spl.extracting()
# should always be set.

if not spl.extracting():
    raise ValueError("spl.extacting is not true: " + str(spl.extracting()))
