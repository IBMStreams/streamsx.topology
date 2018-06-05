import csv
import os
import sys
import numbers

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

# Requires components of VRMF version numbers to be integers,
# except for special case of '4.2.x.x' from get_version().
def has_min_version(required_version):
    rvrmf = required_version.split('.')
    pvrmf = get_version().split('.')
    for i in range(len(rvrmf)):
        if i >= len(pvrmf):
            return 0
        if pvrmf[i] == 'x':
            pvrmf[i] = 0
        pi = int(pvrmf[i])
        ri = int(rvrmf[i])
        if pi < ri:
            return 0
        if pi > ri:
            return 1
    return 1

def tester_supported():
    v = get_version()
    return not v.startswith('4.0.') and not v.startswith('4.1.')

def optional_type_supported():
    return has_min_version('4.3');
