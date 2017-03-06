# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
"""
Python APIs for use with Streaming Analytics service on
IBM® Bluemix cloud platform and on-premises IBM Streams.

Python Application API for Streams
----------------------------------
Module that allows the definition and execution of streaming
applications implemented in Python.
Applications use Python code to process tuples and tuples are Python objects.

See :py:mod:`streamsx.topology`

Python functions as SPL operators
---------------------------------
A Python function or class can be simply turned into an SPL primitive operator
to allow tuple processing using Python in an SPL application.

SPL (Streams Processing Language) is a domain specific language for streaming
analytics supported by Streams.

See :py:mod:`streamsx.spl`

Streams Python REST API
-----------------------
Module that allows interaction with an running Streams instance or
service through HTTPS REST APIs.

See :py:mod:`streamsx.rest`

"""

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
