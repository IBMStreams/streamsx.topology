################
streamsx-service
################

********
Overview
********

Control commands for a Streaming Analytics service.

*****
Usage
*****

::

    service.py [-h] [--service-name SERVICE_NAME] [--full-response]
                  {start,status,stop} ...

    Control commands for a Streaming Analytics service.

    positional arguments:
      {start,status,stop}   Supported commands
        start               Start the service instance
        status              Get the service status.
        stop                Stop the instance for the service.

    optional arguments:
      -h, --help            show this help message and exit
      --service-name SERVICE_NAME
                            Streaming Analytics service name
      --full-response       Print the full JSON response.

    service.py stop [-h] [--force]

    optional arguments:
      -h, --help  show this help message and exit
      --force     Stop the service even if jobs are running.


*****************************************
Controlling a Streaming Analytics service
*****************************************

The Streaming Analytics service to control is defined using
``--service-name SERVICE_NAME``. If not provided then the
service name is defined by the environment variable
``STREAMING_ANALYTICS_SERVICE_NAME``.

The named service must exist in the VCAP services definition
pointed to by the ``VCAP_SERVICES`` environment variable.

The response from making the control request is printed to
standard out in JSON format. By default a minimal response
is printed including the status of the service and the job count.
The complete response from the service REST API is printed if
the option ``--full-response`` is given.

