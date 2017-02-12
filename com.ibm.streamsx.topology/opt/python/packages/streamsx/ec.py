# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

"""
Overview
--------
Access to the IBM Streams execution context.

A Streams application runs distributed or standalone.

Distributed
-----------
Distributed is used when an application is submitted
to the Streaming Analytics service on IBM Bluemix
or a IBM Streams distributed instance.

With distributed a running application is a `job` that
contains one or more processing elements (PEs). A PE
corresponds to a Linux operating system process.
The PEs in a job may be distributed across the
hosts in the Streams instance.

Standalone
----------
Standalone is a mode where the complete application is run
as a single PE (process) outside of a Streams instance.

Standalone is typically used for ad-hoc testing of an
application.

Execution Context
-----------------
This module (`streamsx.exec`) provides access to the execution
context when Python code is running in a Streams application.

Access is only supported when running:
 * Python 3.5
 * Streams 4.2 or later

This module may be used by Python functions or classes used
in a `Topology` or decorated SPL operators.

Certain functionality is only available when a Python class is
being invoked in a Streams application.

"""

import threading

try:
    import _streamsx_ec
    _supported = True
except:
    _supported = False

def _check():
    if not _supported:
        raise NotImplementedError("Access to the execution context requires Python 3.5 and Streams 4.2 or later")

def job_id():
    """
    Return the job identifier.
    """
    _check()
    return _streamsx_ec.job_id()

def pe_id():
    """
    Return the PE identifier.
    """
    _check()
    return _streamsx_ec.pe_id()

def channel(obj):
    """
    Return the parallel region channel number `obj` is executing in.
    
    Args:
        obj: Instance of a class executing as an SPL Python operator.
    """
    return _streamsx_ec.channel(_get_opc(obj))


####################
# internal functions
####################

# Thread local of capsules during
# operator class initialization
_capsules = threading.local()

# Sets the operator capsule as a thread
# local to allow access from an operator's
# class __init__ method.
def _set_opc(opc):
    _capsules._opc = opc

# Clear the operator capsule from the
# thread local
def _clear_opc():
    _capsules._opc = None

# Save the opc in the operator class
# (getting it from the thread local)
def _save_opc(obj):
    _capsules.obj = obj
    if hasattr(_capsules, '_opc'):
       opc = _capsules._opc
       if opc is not None:
           obj._streamsx_ec_op = opc

def _get_opc(obj):
    _check()
    try:
        opc = obj._streamsx_ec_op
        return opc
    except AttributeError:
        try:
            opc = _capsules._opc
            if opc is not None:
                return opc
        except AttributeError:
             pass
        raise NameError("")
