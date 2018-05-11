########################
IBM Streams Python setup
########################

***************************
Streaming Analytics service
***************************

The service instance has Anaconda 4.1 installed with Python 3.5 as the
runtime environment and has ``PYTHONHOME`` application environment variable
pre-configured.

Any streaming applications using Python must use Python 3.5 when
submitted to the service instance.

***********************
IBM Streams on-premises
***********************

For a distributed cluster running Streams Python 2.7 or Python 3.5 may
be used.

Anaconda may be used as the Python runtime, Anaconda has the advantage of
being pre-built and including a number of standard packages. Ananconda
installs may be downloaded at: https://www.continuum.io/downloads .

.. note::
    An Anaconda distribution for Python 2.7 or 3.5 must be used.

If building Python from source then it must be built to support embedding
of the runtime with shared libraries (``--enabled-shared`` option to `configure`).

Distributed
===========

For distributed the Streams application environment variable
``PYTHONHOME`` must be set to the Python install path.

This is set using `streamtool` as::

    streamtool setproperty --application-ev PYTHONHOME=path_to_python_install

The application environment variable may also be set using the Streams
console. The `Instance Management` view has an
`Application Environment Variables` section. Expanding the details
for that section allows modification of the set of environment
variables available to Streams applications.

The Python install path must be accessible on every application resource
that will execute Python code within a Streams application.

Standalone
==========

The environment ``PYTHONHOME`` must be set to the Python install path.
