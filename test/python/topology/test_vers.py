import csv
import os
import sys

def get_version():
    if 'STREAMS_INSTALL' not in os.environ:
        return '4.2.x.x'

    pvf = os.environ['STREAMS_INSTALL'] + '/.product'
    vers={}
    with open(pvf, "r") as cf:
        reader = csv.reader(cf, delimiter='=', quoting=csv.QUOTE_NONE)
        for row in reader:
            vers[row[0]] = row[1] 
    return vers['Version']

def tester_supported():
    v = get_version()
    return not v.startswith('4.0.') and not v.startswith('4.1.')
