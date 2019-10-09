########################
IBM Streams Python setup
########################

***************
Developer setup
***************

Developers install the `streamsx` package Python Package Index (PyPI) to
use this functionality::

    pip install streamsx

If already installed upgrade to the latest version is recommended::

   pip install --upgrade streamsx

A local install of IBM Streams is **not** required when:

    * Using the Streams and Streaming Analytics REST bindings :py:mod:`streamsx.rest`.
    * Devloping and submitting streaming applications using :py:mod:`streamsx.topology.topology` to Cloud Pak for Data or Streaming Analytics service on IBM Cloud.

        * The environment variable ``JAVA_HOME`` must reference a Java 1.8 JRE or JDK/SDK.

A local install of IBM Streams is required when:

    * Developing and submitting streaming applications using :py:mod:`streamsx.topology.topology` to IBM Streams 4.2, 4.3 distributed or standalone contexts.

        * If set the environment variable ``JAVA_HOME`` must reference a Java 1.8 JRE or JDK/SDK, otherwise the Java install from ``$STREAMS_INSTALL/java`` is used.
    * Creating SPL toolkits with Python primitive operators using :py:mod:`streamsx.spl.spl` decorators for use with 4.2, 4.3 distributed or standalone applications.

.. warning::
   When using the `streamsx` package ensure that the environment variable
   ``PYTHONPATH`` does **not** include a path ending with
   ``com.ibm.streamsx.topology/opt/python/packages``.
   The IBM Streams environment configuration script ``streamsprofile.sh``
   modifies or sets ``PYTHONPATH`` to include the Python support
   from the SPL topology toolkit shipped with the product. This was to
   support Python before the `streamsx` package was available. The
   recommendation is to unset ``PYTHONPATH`` or modify it not to
   include the path to the topology toolkit.

.. note::
   The `streamsx` package is self-contained and does not depend on any
   SPL topology toolkit (``com.ibm.streamsx.topology``) installed
   under ``$STREAMS_INSTALL/toolkits`` or on the SPL compiler's (``sc``)
   toolkit path. This is true at SPL compilation time and runtime.

***************************
Streaming Analytics service
***************************

The service instance has Anaconda 4.1 installed with Python 3.5 as the
runtime environment and has ``PYTHONHOME`` Streams application environment variable
pre-configured.

Any streaming applications using Python must use Python 3.5 when
submitted to the service instance.

**********************
IBM Cloud Pak for Data
**********************

An IBM Streams service instance within Cloud Pak for Data has Anaconda installed with Python 3.6 as the
runtime environment and has ``PYTHONHOME`` Streams application environment variable pre-configured.

Any streaming applications using Python must use Python 3.6 when
submitted to the service instance.

Streaming applications can be submitted through Jupyter notebooks running in
Cloud Pak for Data projects. The `streamsx` package is preinstalled and applications are sumitted to the :py:const:`~streamsx.topology.context.ContextTypes.DISTRIBUTED` context.

********************
IBM Streams 4.2, 4.3
********************

For a distributed cluster running Streams Python 3.7, 3.6 or 3.5
may be used.

Anaconda or Miniconda distributions may be used as the Python runtime, these have the advantage of being pre-built and including a number of standard packages.
Ananconda installs may be downloaded at: https://www.continuum.io/downloads .

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

.. note::
   The Python version used to declare and submit the application must compatible with the setting of ``PYTHONHOME`` in the instance. For example, if ``PYTHONHOME`` Streams application instance variable points to a Python 3.6 install, then Python 3.5 or 3.6 can be used to declare and submit the application.

Standalone
==========

The environment ``PYTHONHOME`` must be set to the Python install path.

***********************************
Bundle Python version compatibility
***********************************

As of 1.13 Streams application bundles (sab files) invoking Python are binary
compatible with a range of Python releases when using Python 3.

The minimum verson supported is the version of Python used during bundle
creation.

The maximum version supported is the highest version of Python with a
proposed release schedule.

For example if a sab is built with Python 3.6 then it can be submitted
to a Streams instance using 3.6 or higher, up to & including 3.9 which is
the highest Python release with a proposed release schedule as of 1.13.

.. note::
   Compatability across Python releases is dependent on Python's
   `Stable Application Binary Inteface <https://docs.python.org/3/c-api/stable.html>`_.
