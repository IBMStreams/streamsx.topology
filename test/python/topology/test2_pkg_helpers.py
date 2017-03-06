# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

d = {}

try:
    import test_package.test_subpackage.test_module
    d['hp'] = True
except ImportError:
    d['hp'] = False

def missing_package(tuple):
    if d['hp']:
        # Don't want to be able to import the package
        return "Package was imported!"
    else:
        return tuple + "MP"

def imported_package(tuple):
    if d['hp']:
        # Ensure we can call the package
        if test_package.test_subpackage.test_module.filter(tuple):
            return tuple + "IP"
        else:
            return None
    else:
        return "Package was not imported"
