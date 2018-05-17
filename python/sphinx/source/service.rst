.. _sas-main:

###############################
IBM Streaming Analytics service
###############################


********
Overview
********

IBM® Streaming Analytics for IBM Cloud is powered by IBM® Streams, an advanced analytic platform that you can use to ingest, analyze, and correlate information as it arrives from different types of data sources in real time. When you create an instance of the Streaming Analytics service, you get your own instance of IBM® Streams running in IBM® Cloud, ready to run your IBM® Streams applications.

.. seealso::
    `Overview at ibm.com <https://www.ibm.com/cloud/streaming-analytics>`_

    `IBM Cloud catalog <https://console.bluemix.net/catalog/services/streaming-analytics>`_

    `Streaming Analytics service documentation <https://console.bluemix.net/docs/services/StreamingAnalytics/index.html>`_

***************
Package support
***************

This `streamsx` package supports :

    * Developing streaming applications in Python that can be submitted to a Streaming Analytics service. See :py:mod:`streamsx.topology.topology`, :py:const:`~streamsx.topology.context.ContextTypes.STREAMING_ANALYTICS_SERVICE`.
    * Submitting streaming applications written in Python or SPL to a Streaming Anlaytics service. See :ref:`runner-python-apps`, :ref:`runner-spl-apps`.
    * Submitting a pre-compiled Streams application bundle (``sab`` file) Python or SPL to a Streaming Anlaytics service. See :ref:`runner-sab`.
    * Python bindings to the IBM Streams REST API and the Streaming Analytics REST API. See :py:mod:`streamsx.rest`

.. _sas-access:

*******************
Accessing a service
*******************

In order to use a Streaming Analytics service you must have access
to credentials for the service.  There are two mechanisms used by
this package, VCAP services and direct use of Streaming Analytics credentials.

.. _sas-vcap:

VCAP services
=============

This is the format used by Cloud Foundry for bindable services.
The service key for Streaming Analytics service is ``streaming-analytics``,
the value of that key in the VCAP services is a list of accessible services,
each service represented by a separate object.

Each streaming analytics object must have these keys:

    * ``name`` identifying the name of the service.
    * ``credentials`` identifying the connection credentials for the service.

Example VCAP services containing two Streaming Analytics services `sa-test` and `sa-prod` (with the specific connection details elided)::

    {
    "streaming-analytics": [
    { 
      "name": "sa-test",
      "credentials":
      {
         "apikey": "...",
         "iam_apikey_description": "Auto generated apikey during resource-key operation for Instance - ...",
         "iam_apikey_name": "auto-generated-apikey-...",
         "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
         "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity ...",
          "v2_rest_url": "https://streams-app-service.ng.bluemix.net/v2/streaming_analytics/..."
      }
    },
    {
      "name": "sa-prod",
      "credentials":
      {
         "apikey": "...",
         "iam_apikey_description": "Auto generated apikey during resource-key operation for Instance - ...",
         "iam_apikey_name": "auto-generated-apikey-...",
         "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
         "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity ...",
          "v2_rest_url": "https://streams-app-service.ng.bluemix.net/v2/streaming_analytics/..."
      }
    }
    ]
    }

.. note::
   The specific keys in the credentials may differ depending on the service plan.

.. seealso::
   https://docs.run.pivotal.io/devguide/deploy-apps/environment-variable.html#VCAP-SERVICES

Cloud Foundry applications
--------------------------

When a Streaming Analytics service is bound to a Cloud Foundry Python
application the environment variable ``VCAP_SERVICES`` is
automatically defined and contains a string representation of the
JSON VCAP services information.

Client applications
-------------------

Client applications are ones that run outside of the IBM Cloud, for
example on a local laptop, or applications that are not bound to a service.

Client applications running must define a valid VCAP services in its JSON format as either:

    * In the environment variable ``VCAP_SERVICES`` containing a string representation of the JSON VCAP services information.
    * In a file containing a string representation of the JSON VCAP services information and have the file's absolute path in either:

        * the environment variable ``VCAP_SERVICES``
        * the configuration property :py:const:`~streamsx.topology.context.ConfigParams.VCAP_SERVICES` when submitting an application using :py:func:`~streamsx.topology.context.submit` with context type :py:const:`~streamsx.topology.context.ContextTypes.STREAMING_ANALYTICS_SERVICE`. This overrides the environment variable `VCAP_SERVICES`.

The contents of the file must be manually created, the credentials for the ``credentials`` key are obtained from the Streaming Analytics manage console. Select the `Service Credentials` page and then copy the required credentials. You may need to first create credentials. You can an copy the credentials by taking the `View credentials` action and then clicking the `copy to clipboard` icon on the right hand side.

.. warning::
   The credential information in VCAP services is in plain text. Ensure that the any file containing the information or setting the environment variable has suitable permissions set. For example only readable by the intended user.

.. _sas-service-name:

Selecting the service
---------------------

The Streaming Analyitcs service to use is specifed by its name, the required service much exist in the VCAP service information using the ``name`` key.

The name of the service to use is set by:

    * the environment variable ``STREAMING_ANALYTICS_SERVICE_NAME``.
    * the configuration property :py:const:`~streamsx.topology.context.ConfigParams.SERVICE_NAME` when submitting an application using :py:func:`~streamsx.topology.context.submit` with context type :py:const:`~streamsx.topology.context.ContextTypes.STREAMING_ANALYTICS_SERVICE`. This overrides the environment variable `STREAMING_ANALYTICS_SERVICE_NAME`.
    * the ``--service-name`` option to ``streamsx-runner``.

.. _sas-service-def:

Service definition
==================

The Streaming Analytics service to use may be specified solely using its credentials. The credentials are specified:

    * with the configuration property :py:const:`~streamsx.topology.context.ConfigParams.SERVICE_DEFINITION` when submitting an application using :py:func:`~streamsx.topology.context.submit` with context type :py:const:`~streamsx.topology.context.ContextTypes.STREAMING_ANALYTICS_SERVICE`.
    * when using :py:meth:`streamsx.rest.StreamingAnalyticsConnection.of_definition` to create a REST connection.


Credentials obtained from the Streaming Analytics manage console. Select the `Service Credentials` page and then copy the required credentials. You may need to first create credentials. You can an copy the credentials by taking the `View credentials` action and then clicking the `copy to clipboard` icon on the right hand side.
