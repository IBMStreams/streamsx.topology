#############
streamsx-info
#############

********
Overview
********

Information about streamsx package and environment.

Prints to standard out information about the `streamsx` package
and environment variables used to support Python in IBM Streams
and Streaming Analytics service.

A Python warning is issued if a mismatch is detected between
the installed `streamsx` package and its modules. This is typically
due to having a different version of the modules accessible through
the environment variable ``PYTHONPATH``.

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

Output is subject to change in the order and information displayed.
Intended as an ad-hoc tool to help diagnose issues with `streamsx`.

Script may also be run as Python module:

.. code-block:: none

   python -m streamsx.scripts.info

*****
Usage
*****

.. code-block:: none

    usage: streamsx-info [-h]

        Prints support information about streamsx package and environment.

    optional arguments:
        -h, --help  show this help message and exit

