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

*****
Usage
*****

::

    streamsx-runner [-h] (--service-name SERVICE_NAME | --create-bundle)
                       (--topology TOPOLOGY | --main-composite MAIN_COMPOSITE)
                       [--toolkits TOOLKITS [TOOLKITS ...]]

    Execute a Streams application using a Streaming Analytics service.

    optional arguments:
      -h, --help            show this help message and exit
      --service-name SERVICE_NAME
                            Submit to Streaming Analytics service
      --create-bundle       Create a bundle
      --topology TOPOLOGY   Topology to call
      --main-composite MAIN_COMPOSITE
                            SPL Main composite
      --toolkits TOOLKITS [TOOLKITS ...]
                            Additional SPL toolkits


*****************************************
Submitting to Streaming Analytics service
*****************************************

An application is submitted to a Streaming Analytics service using
``--service-name SERVICE_NAME``. The named service must exist in the
VCAP services defintion pointed to by the ``VCAP_SERVICES`` environment
variable.

The application is submitted as source (Python or SPL) and compiled into
a Streams application bundle (sab file) using the build service before
being submitted as a running job to the service instance.

*******************
Python applications
*******************

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
(typically through enviornment variable ``PYTHONPATH``).

****************
SPL applications
****************

The main composite that defines the application is specified using the ``-main-composite`` flag specifing the fully namespace qualified name.

Any required local SPL toolkits, *including the one containing the main composite*, must be indivdually specified by location to the ``--toolkits`` flag. Any SPL toolkit that is present on the IBM Cloud service need not be included.

For example, an application that uses the Slack toolkit might be submitted as::

    streamsx-runner --service-name Streaming-Analytics-xd \
        --main-composite com.example.alert::SlackAlerter \
        --toolkits $HOME/app/alerters $HOME/toolkits/com.ibm.streamsx.slack

where ``$HOME/app/alerts`` is the location of the SPL application toolkit containing the ``com.example.alert::SlackAlerter`` main composite.
