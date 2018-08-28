###############
streamsx-runner
###############

********
Overview
********

Submits a Streams application to the Streaming Analytics service.

The application to be submitted can be:

 * A Python application defined through :py:class:`~streamsx.topology.topology.Topology` using the ``--topology`` flag.
 * An SPL application (main composite) using the ``--main-composite`` flag.
 * A Streams application bundle (``sab`` file) using the ``--bundle`` flag.

*****
Usage
*****

::

    streamsx-runner [-h] (--service-name SERVICE_NAME | --create-bundle)
                 (--topology TOPOLOGY | --main-composite MAIN_COMPOSITE | --bundle BUNDLE)
                 [--toolkits TOOLKITS [TOOLKITS ...]] [--job-name JOB_NAME]
                 [--preload] [--trace {error,warn,info,debug,trace}]
                 [--submission-parameters SUBMISSION_PARAMETERS [SUBMISSION_PARAMETERS ...]]
    
    Execute a Streams application using a Streaming Analytics service.
    
    optional arguments:
      -h, --help            show this help message and exit
      --service-name SERVICE_NAME
                            Submit to Streaming Analytics service
      --create-bundle       Create a bundle using a local IBM Streams install. No
                            job submission occurs.
      --topology TOPOLOGY   Topology to call
      --main-composite MAIN_COMPOSITE
                            SPL main composite (namespace::composite_name)
      --bundle BUNDLE       Streams application bundle (sab file) to submit to
                            service
    
    Build options:
      Application build options
    
      --toolkits TOOLKITS [TOOLKITS ...]
                            SPL toolkit containing the main composite and any
                            other required SPL toolkits.
    
    Job options:
      Job configuration options
    
      --job-name JOB_NAME   Job name
      --preload             Preload job onto all resources in the instance
      --trace {error,warn,info,debug,trace}
                            Application trace level
      --submission-parameters SUBMISSION_PARAMETERS [SUBMISSION_PARAMETERS ...], -p SUBMISSION_PARAMETERS [SUBMISSION_PARAMETERS ...]
                            Submission parameters as name=value pairs

*****************************************
Submitting to Streaming Analytics service
*****************************************

An application is submitted to a Streaming Analytics service using
``--service-name SERVICE_NAME``. The named service must exist in the
VCAP services definition pointed to by the ``VCAP_SERVICES`` environment
variable.

The application is submitted as source (except ``--bundle``)  and compiled into
a Streams application bundle (``sab`` file) using the build service before
being submitted as a running job to the service instance.

.. seealso:: :ref:`sas-access`

.. _runner-python-apps:

Python applications
===================

To submit a Python application a Python function must be defined
that returns the application (and optionally its configuration)
to be submitted. The fully qualified name of this function is
specified using the ``--topology`` flag.

For example, an application can be submitted as::

    streamsx-runner --service-name Streaming-Analytics-xd \
        --topology com.example.apps.sensor_ingester

The function returns one of:

    * a :py:class:`~streamsx.topology.topology.Topology` instance defining the application
    * a ``tuple`` containing two values, in order:
        * a :py:class:`~streamsx.topology.topology.Topology` instance defining the application
        * job configuration, one of:
            * :py:class:`~streamsx.topology.context.JobConfig` instance
            * ``dict`` corresponding to the configuration object passed into :py:func:`~streamsx.topology.context.submit`

For example the above function might be defined as::

    def _create_sensor_ingester_app():
       topo = Topology('SensorIngesterApp')
       
       # Application declaration omitted
       ...

       return topo

    def sensor_ingester():
        return (_create_sensor_ingester_app(), JobConfig(job_name='SensorIngester'))


Thus when this application is submitted using the `sensor_ingester` function
it is always submitted with the same job name `SensorIngester`.

The function must be accessible from the current Python path
(typically through environment variable ``PYTHONPATH``).

.. _runner-spl-apps:

SPL applications
================

The main composite that defines the application is specified using the ``-main-composite`` flag specifing the fully namespace qualified name.

Any required local SPL toolkits, *including the one containing the main composite*, must be indivdually specified by location to the ``--toolkits`` flag. Any SPL toolkit that is present on the IBM Cloud service need not be included.

For example, an application that uses the Slack toolkit might be submitted as::

    streamsx-runner --service-name Streaming-Analytics-xd \
        --main-composite com.example.alert::SlackAlerter \
        --toolkits $HOME/app/alerters $HOME/toolkits/com.ibm.streamsx.slack

where ``$HOME/app/alerters`` is the location of the SPL application toolkit containing the ``com.example.alert::SlackAlerter`` main composite.

.. warning::
    The main composite name must be namespace qualified.
    Use of the default namespace for a main composite is not
    recommended as it increases the chance of a name clash with
    another SPL toolkit.

.. _runner-sab:

Streams application bundles
===========================

A Streams application bundle is submitted to a service instance using ``--bundle``.  The argument to ``--bundle`` is a locally accessible file that will be uploaded to the service.

The bundle must have been created on using an IBM Streams install whose architecture and OS version matches the service instance. Currently this is ``x86_64`` and RedHat/CentOS 6 or 7 depending on the service instance.

The ``--toolkits`` flag must not be specified when submitting a bundle.

Job options
===========

Job options, such as ``--job-name``, configure the running job.

For ``--topology`` job options set as arguments to ``streamsx-runner`` override any configuration returned from the function defining the application.

************************************
Creating Streams application bundles
************************************

``--create-bundle`` uses a local IBM Streams install to attempt to mimic the build that would occur with ``-topology`` or ``--main-composite``. Differences between the local environment and the IBM Cloud Streaming Analytics build environment may cause build failures in one and not the other.

This can be used as a mechanism to perform a local test build before using the service, or as a valid mechanism to create bundles for later upload with ``--bundle``.

For example simply changing the ``--service-name name`` to ``--create-bundle`` perfoms a local build of the same application::

    # Submit to an Streaming Analytics service
    streamsx-runner --service-name Streaming-Analytics-xd \
        --main-composite com.example.alert::SlackAlerter \
        --toolkits $HOME/app/alerters $HOME/toolkits/com.ibm.streamsx.slack

    # Build the same application locally
    streamsx-runner --create-bundle \
        --main-composite com.example.alert::SlackAlerter \
        --toolkits $HOME/app/alerters $HOME/toolkits/com.ibm.streamsx.slack

