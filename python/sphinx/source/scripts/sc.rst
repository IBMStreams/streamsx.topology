###################
streamsx-sc
###################

********
Overview
********

Command line interface for IBM Streams running on IBM Cloud Pak for Data.

``streamsx-sc`` replicates a sub-set of Streams ``sc``
commands focusing on supporting DevOps for streaming applications.

``streamsx-sc`` is supported for Streams instances running
on Cloud Pak for Data. A local install of Streams is **not** required,
simply the installation of the `streamsx` package. All functionality
is implemented through the Cloud Pak for Data and Streams REST apis.

Cloud Pak for Data configuration
================================

The Streams instance and authentication are defined through environment variables:

    * **CP4D_URL** - Cloud Pak for Data deployment URL, e.g. `https://cp4d_server:31843`.
    * **STREAMS_INSTANCE_ID** - Streams service instance name.
    * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name. Overridden by the ``--User`` option.
    * **STREAMS_PASSWORD** - Password for authentication.

*****
Usage
*****

.. code-block:: none

    streamsx-sc sc.py [-h] --main-composite name [--spl-path SPL_PATH]
            [--optimized-code-generation] [--no-optimized-code-generation]
            [--prefer-facade-tuples] [--ld-flags LD_FLAGS]
            [--cxx-flags CXX_FLAGS] [--c++std C++STD]
            [--data-directory DATA_DIRECTORY]
            [--output-directory OUTPUT_DIRECTORY] [--disable-ssl-verify]
            [--static-link] [--standalone-application]
            [--set-relax-fusion-relocatability-restartability]
            [--checkpoint-directory path] [--profiling-sampling rate]
            [compile-time-args [compile-time-args ...]]


Options and arguments

    compile-time-args:
        arguments that are passed in at compile time

    -M,--main-composite:
        SPL Main composite

    -t,--spl-path:
        Set the toolkit lookup paths. Separate multiple paths
        with :. Each path is a toolkit directory, a directory
        of toolkit directories, or a toolkitList XML file.
        This path overrides the STREAMS_SPLPATH environment
        variable.

    -a,--optimized-code-generation:
        Generate optimized code with less runtime error
        checking

    -\--no-optimized-code-generation:
        Generate non-optimized code with more runtime error
        checking. Do not use with the --optimized-code-
        generation option.

    -k,--prefer-facade-tuples:
        Generate the facade tuples when it is possible.

    -w,--ld-flags:
        Pass the specified flags to ld while linking occurs.

    -x,--cxx-flags:
        Pass the specified flags to the C++ compiler during
        the build.

    \--c++std:
        Specify the language level for the underlying C++
        compiles.

    \--data-directory:
        Specifies the location of the data directory to use.

    \--output-directory:
        Specifies a directory where the application artifacts
        are placed.

    \--disable-ssl-verify:
        Disable SSL verification

Deprecated arguments
    Arguments specific to use of the build service. Not supported by sc.

    -s,--static-link

    -T,--standalone-application

    -O,--set-relax-fusion-relocatability-restartability

    -K,--checkpoint-directory

    -S,--profiling-sampling

