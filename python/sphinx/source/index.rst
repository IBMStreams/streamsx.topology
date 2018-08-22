.. streamsx documentation master file, created by
   sphinx-quickstart on Thu Feb  9 15:48:14 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

##########################
IBM Streams Python support
##########################

Python APIs for use with IBM® Streaming Analytics service on
IBM Cloud and on-premises IBM Streams.

**********************************
Python Application API for Streams
**********************************

Module that allows the definition and execution of streaming
applications implemented in Python.
Applications use Python code to process tuples and tuples are Python objects.

SPL operators may also be invoked from Python applications to allow
use of existing IBM Streams toolkits.

See :py:mod:`~streamsx.topology`

.. autosummary::
   :toctree: .
   :template: streamsx_module.rst

   streamsx.topology
   streamsx.topology.topology
   streamsx.topology.context
   streamsx.topology.schema
   streamsx.topology.tester
   streamsx.topology.tester_runtime
   streamsx.ec
   streamsx.spl.op
   streamsx.spl.types
   streamsx.spl.toolkit

******************************
SPL primitive Python operators
******************************

SPL primitive Python operators provide the ability
to perform tuple processing using Python in an SPL application.

A Python function or class is simply turned into an SPL primitive operator
through provided decorators.

SPL (Streams Processing Language) is a domain specific language for streaming
analytics supported by Streams.

.. autosummary::
   :toctree: .
   :template: streamsx_module.rst

   streamsx.spl.spl

***********************
Streams Python REST API
***********************

Module that allows interaction with an running Streams instance or
service through HTTPS REST APIs.

========

.. autosummary::
   :toctree: .
   :template: streamsx_module.rst

   streamsx.rest
   streamsx.rest_primitives

*******
Scripts
*******

The `streamsx` package provides a number of command line scripts.

.. toctree::

   scripts/runner.rst
   scripts/service.rst
   scripts/extract.rst

************
Environments
************

.. toctree::

   service.rst
   pysetup.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

