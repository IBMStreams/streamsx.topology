.. _spl-py-extract:

##################
spl-python-extract
##################

********
Overview
********

Extracts SPL Python primitive operators from decorated
Python classes and functions.

Executing this script against an SPL toolkit creates the SPL
primitive operator meta-data required by the SPL compiler (`sc`).

*****
Usage
*****

.. code-block:: none

    spl-python-extract [-h] -i DIRECTORY [--make-toolkit] [-v]

    Extract SPL operators from decorated Python classes and functions.

    optional arguments:
      -h, --help            show this help message and exit
      -i DIRECTORY, --directory DIRECTORY
                            Toolkit directory
      --make-toolkit        Index toolkit using spl-make-toolkit
      -v, --verbose         Print more diagnostics

******************************
SPL Python primitive operators
******************************

SPL operators that call a Python function or callable class are created by
decorators provided by the `streamsx` package.

To create SPL operators from Python functions or classes one or more Python
modules are created in the ``opt/python/streams`` directory
of an SPL toolkit.

``spl-python-extract`` is a Python script that creates SPL operators from
Python functions and classes contained in modules under ``opt/python/streams``.

The resulting operators embed the Python runtime to allow stream
processing using Python.

Details on how to implement SPL Python primitive operators see
:py:mod:`streamsx.spl.spl`.

