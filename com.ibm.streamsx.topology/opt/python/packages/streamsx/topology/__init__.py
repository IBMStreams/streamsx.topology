# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

"""
Python API to allow creation of streaming applications for
Streaming Analytics service on IBM® Bluemix cloud platform
and on-premises IBM Streams.

Overview
########

IBM® Streams is an advanced analytic platform that allows user-developed applications to quickly ingest,
analyze and correlate information as it arrives from thousands of real-time sources.
Streams can handle very high data throughput rates, millions of events or messages per second.
With this API Python developers can build streaming applications that can be executed using IBM Streams,
including the processing being distributed across multiple computing resources (hosts or machines) for scalability.
"""

import logging


class _NullHandler(logging.Handler):
    def emit(self, record):
        pass

_debug = logging.getLogger('streamsx.topology.internal')
_debug.addHandler(_NullHandler())
_debug.setLevel(logging.CRITICAL)