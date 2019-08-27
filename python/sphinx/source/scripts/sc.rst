###################
streamsx-sc
###################

********
Overview
********

SPL compiler for IBM Streams running on IBM Cloud Pak for Data.

``streamsx-sc`` replicates a sub-set of Streams 4.3 ``sc`` options.

``streamsx-sc`` is supported for Streams instances running
on Cloud Pak for Data. A local install of Streams is **not** required,
simply the installation of the `streamsx` package. All functionality
is implemented through the Cloud Pak for Data and Streams build service REST apis.

Cloud Pak for Data configuration
================================

The Streams instance and authentication are defined through environment variables:

    * **CP4D_URL** - Cloud Pak for Data deployment URL, e.g. `https://cp4d_server:31843`.
    * **STREAMS_INSTANCE_ID** - Streams service instance name.
    * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
    * **STREAMS_PASSWORD** - Password for authentication.

*****
Usage
*****

.. code-block:: none

    streamsx-sc [-h] --main-composite name [--spl-path SPL_PATH]
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
        Pass named arguments each in the format `name=value` to the compiler.
        The name cannot contain the character ``=`` but otherwise is a free
        form string. It matches the name parameter that is specified in calls
        that are made to the compile-time argument access functions from
        within SPL code. The value can be any string. See `Compile-time arguments <https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/compileargs.html>`_ .

    -M,--main-composite:
        SPL Main composite

    -t,--spl-path:
        Set the toolkit lookup paths. Separate multiple paths
        with ``:``. Each path is a toolkit directory or a directory
        of toolkit directories.
        This path overrides the ``STREAMS_SPLPATH`` environment
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
        Disable SSL verification against the build service

Deprecated arguments
    Arguments supported by `sc` but deprecated. They have no affect on compilation.

    -s,--static-link

    -T,--standalone-application

    -O,--set-relax-fusion-relocatability-restartability

    -K,--checkpoint-directory

    -S,--profiling-sampling


********
Toolkits
********

The application toolkit is defined as the working directory of `streamsx-sc`.

Local toolkits are found through the toolkit path set by `--spl-path` or environment variable ``STREAMS_SPLPATH``. Local toolkits are included in the build code archive sent to the build service if:

    *  the toolkit is defined as a dependent of the application toolkit including recursive dependencies of required local toolkits.
    *  and a toolkit of a higher version within the required dependency range does not exist locally or remotely on the build service.

The toolkit path for the compilation on the build service includes:

    * the application toolkit
    * local tookits included in the build code archive
    * all toolkits uploaded on the Streams build service
    * all product toolkits on the Streams build service

The application toolkit and local toolkits included in the build archive are processed prior to the actual compilation by:

    * having any Python SPL primitive operators extracted using ``spl-python-extract``
    * indexed using ``spl-make-toolkit``

.. versionadded:: 1.13
