###################
streamsx-streamtool
###################

********
Overview
********

Command line interface for IBM Streams running on IBM Cloud Pak for Data.

``streamsx-streamtool`` replicates a sub-set of Streams ``streamtool``
commands focusing on supporting DevOps for streaming applications.

``streamsx-streamtool`` is supported for Streams Cloud Pak for Data (5.x) instances
A local install of Streams is **not** required,
simply the installation of the `streamsx` package. All functionality
is implemented through Cloud Pak for Data and Streams REST apis.

Cloud Pak for Data configuration
================================

The Streams instance and authentication are defined through environment variables, the details depend on if the Streams instance is running in integrated or standalone configuration.

Integrated configuration
------------------------

    * **CP4D_URL** - Cloud Pak for Data deployment URL, e.g. `https://cp4d_server:31843`.
    * **STREAMS_INSTANCE_ID** - Streams service instance name.
    * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name. Overridden by the ``--User`` option.
    * **STREAMS_PASSWORD** - Password for authentication.

Standalone configuration
------------------------

        * **STREAMS_REST_URL** - Streams SWS service (REST API) URL, e.g. when the service is exposed as node port: `https://<NODE-IP>:<NODE-PORT>`
        * **STREAMS_BUILD_URL** - Streams build service (REST API) URL, e.g. when the service is exposed as node port: `https://<NODE-IP>:<NODE-PORT>`. Required for `lstoolkit` and `rmtoolkit`.
        * **STREAMS_USERNAME** - (optional) User name to submit the job as, defaulting to the current operating system user name.
        * **STREAMS_PASSWORD** - Password for authentication.

*****
Usage
*****

.. code-block:: none

    streamsx-streamtool submitjob [-h] [--jobConfig file-name]
            [--jobname job-name] [--jobgroup jobgroup-name]
            [--outfile file-name] [--P parameter-name]
            [--User user]
            sab-pathname

    streamsx-streamtool canceljob [-h] [--force] [--collectlogs]
            [--jobs job-id | --jobnames job-names | --file file-name]
            [--User user]
            [jobid [jobid ...]]

    streamsx-streamtool lsjobs [-h] [--jobs job-id] [--users user]
            [--jobnames job-names] [--fmt format-spec]
            [--xheaders] [--long] [--showtimestamp]
            [--User user]

    streamsx-streamtool lsappconfig [-h] [--fmt format-spec] [--User user]

    streamsx-streamtool mkappconfig [-h] [--property name=value]
            [--propfile property-file]
            [--description description] [--User user]
            config-name

    streamsx-streamtool rmappconfig [-h] [--noprompt] [--User user] config-name

    streamsx-streamtool chappconfig [-h] [--property name=value]
            [--description description] [--User user]
            config-name

    streamsx-streamtool getappconfig [-h] [--User user] config-name

    streamsx-streamtool lstoolkit [-h]
            (--all | --id toolkit-id | --name toolkit-name | --regex toolkit-regex)
            [--User user]

    streamsx-streamtool rmtoolkit [-h]
            (--toolkitid toolkit-id | --toolkitname toolkit-name | --toolkitregex toolkit-regex)
            [--User user]

    streamsx-streamtool uploadtoolkit [-h] --path toolkit-path [--User user]

    streamsx-streamtool updateoperators [-h] [--jobname job-name]
            [--jobConfig file-name]
            [--parallelRegionWidth parallelRegionName=width]
            [--force] [--User user]
            [jobid]

*****************************************
submitjob
*****************************************

The streamtool submitjob command previews or submits one job.

Description:

A submitted job runs an application that is defined by an application bundle.
Application bundles are created by the Stream Processing Language (SPL)
compiler. A job consists of one or more processing elements (PEs). The PEs are
placed on one or more of the application resources for the instance. The
submission fails if the PE placement constraints can't be met. 
 
Jobs remain in the system until they are canceled or the instance is stopped.

.. code-block:: none

    streamsx-streamtool submitjob [-h] [--jobConfig file-name]
            [--jobname job-name] [--jobgroup jobgroup-name]
            [--outfile file-name] [--P parameter-name]
            [--User user]
            sab-pathname

Options and arguments

    sab-pathname
        Specifies the path name for the application bundle file. If you do
        not specify an absolute path, the command seeks the file in the
        directory where you ran the command. Alternatively, you can specify
        the path name for the application description language (ADL) file if
        the application bundle file exists in the same directory.

    -g,--jobConfig:
        Specifies the name of an external file that defines a job
        configuration overlay. You can use a job configuration overlay to set
        the job configuration when the job is submitted or to change the
        configuration of a running job.

    -P,--P:
        Specifies a submission-time parameter and value for the job. You can
        specify this option multiple times in the command.

    -J,--jobgroup:
        Specifies the job group. If you do not specify this option, the
        command uses the following job group: default.

    -\--jobname:
        Specifies the name of the job.

    -\--outfile:
        Specifies the path and file name of the output file in which the
        command writes the list of submitted job IDs. The path can be an
        absolute or relative path. If you do not specify a path, the file is
        created in the directory where you run the command.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.

*****************************************
canceljob
*****************************************

The streamtool canceljob command cancels one or more jobs.

This command stops the processing elements (PEs) for the job and removes
knowledge of the jobs and their PEs from the instance. The log files for the
processing elements are scheduled for removal.

If you specify to collect the PE logs before they are removed, the operation
can time out waiting for the termination of PEs. If such a timeout occurs, the
operation fails and the jobs or PEs are still in the system. The canceljob
command can be run again later to cancel them.

You can use the --force option to ignore a PE termination timeout and force the
job to cancel.

.. code-block:: none

    streamsx-streamtool canceljob [-h] [--force] [--collectlogs]
            [--jobs job-id | --jobnames job-names | --file file-name]
            [--User user]
            [jobid [jobid ...]]

Options and arguments

    jobid
        Specifies a list of job IDs.

    -f,--file:
        Specifies the file that contains a list of job IDs, one per line.

    -j,--jobs:
          Specifies a list of job IDs, which are delimited by commas.

    -\--jobnames:
        Specifies a list of job names, which are delimited by commas.

    -\--collectlogs:
        Specifies to collect the log and trace files for each processing
        element that is associated with the job.

    -\--force:
        Specifies to quickly cancel a job and remove the job from the Streams
        data table.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.


*****************************************
lsjobs
*****************************************

The streamtool lsjobs command lists the jobs in the instance.

The streamtool lsjobs command provides a health summary for each job. The
health summary is an aggregation of the PE health summaries for the job. If all
of the PEs for a job are reported as healthy, the job is reported as healthy.
Otherwise, the job is reported as not healthy. Use the streamtool lspes command
to determine the health of PEs.

The command also reports the status of each job. For more information about job
states, see the IBM Streams product documentation.

The date and time that the job was submitted are presented in local time with
the iso8601 format: yyyy-mm-ddThh:mm:ss+/-hhmm, where the final hhmm values are
the local offset from UTC. For example: 2010-03-16T13:41:53-0500.

When job selection options are specified, selected jobs must meet all of the
selection criteria.
After a cancel request for a job is processed, this command no longer reports
the job or its processing elements (PEs). 

.. code-block:: none

    streamsx-streamtool lsjobs [-h] [--jobs job-id] [--users user]
            [--jobnames job-names] [--fmt format-spec]
            [--xheaders] [--long] [--showtimestamp]
            [--User user]

Options and arguments

    -j,--jobs:
            Specifies a list of job IDs, which are delimited by commas.

    -\--jobnames:
        Specifies a list of job names, which are delimited by commas.

    -u,--users:
        Specifies to select from this list of user IDs, which are delimited
        by commas.

    -\--xheaders:
        Specifies to exclude headings from the report.

    -l,--long:
        Reports launch count, full host names, and all of the operator
        instance names for the PEs.

    -\--fmt:
        Specifies the presentation format. The command supports the following
        values:

            * %Mf: Multiline record format. One line per field. 
            * %Nf: Name prefixed field table format. One line per job. 
            * %Tf: Standard table format, which is the default. One line per job.

    -\--showtimestamp:
        Specifies to show a time stamp in the output to indicate when the
        command was run.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.


*****************************************
lsappconfig
*****************************************

The streamtool lsappconfig command lists the available configurations that
enable connections to an external application.

Retrieve a list of configurations for making a connection to an external
application. 

.. code-block:: none

    streamsx-streamtool lsappconfig [-h] [--fmt format-spec] [--User user]

Options and arguments

    -\--fmt:
        Specifies the presentation format. The command supports the following
        values:

            * %Mf: Multiline record format. One line per field.
            * %Nf: Name prefixed field table format. One line per cfgname.
            * %Tf: Standard table format, which is the default. One line per cfgname.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.


*****************************************
mkappconfig
*****************************************

The streamtool mkappconfig command creates a configuration that enables
connection to an external application.

Operators can retrieve the configuration information to make a connection to an
external application, such as an Internet Of Things application. The properties
include items that the application needs at runtime, like connection
information and credentials.
 
Use this command to register properties or a properties file. Create the
property file using a name=value syntax.

.. code-block:: none

    streamsx-streamtool mkappconfig [-h] [--property name=value]
            [--propfile property-file]
            [--description description] [--User user]
            config-name

Options and arguments

    config-name:
        Name of the app config

    -\--description:
        Specifies a description for the application configuration. The
        description can be 1024 characters in length. If the description
        contains blank characters, it must be enclosed in single or double
        quotation marks. Quotation marks within the description must be
        preceded by a backslash (\).

    -\--property:
        Specifies a property name and value pair to add to or change in the
        configuration. This option can be specified multiple times and has an
        additive effect.
    
    -\--propfile:
        Specifies the path to a file that contains a list of application
        configuration properties for connecting to an external application.
        The properties are listed as name=value pairs, each on a separate
        line. Use this option as a way to include multiple configuration
        properties when you create an application configuration. Options that
        you specify at the command line override values that are specified in
        this property file.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.


*****************************************
rmappconfig
*****************************************

The streamtool rmappconfig command removes a configuration that enables
connection to an external application. 


This command removes a configuration that is used for making a connection to an
external application.

.. code-block:: none

    streamsx-streamtool rmappconfig [-h] [--noprompt] [--User user] config-name

Options and arguments

    config-name:
        Name of the app config

    -\--noprompt:
        Specifies to suppress confirmation prompts.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.

*****************************************
chappconfig
*****************************************

The streamtool chappconfig command updates a configuration that enables
connection to an external application. 


Use this command to change the configuration properties that are used to make a
connection to an external application, such as an Internet Of Things
application. You can change the values of properties or add new properties.

.. code-block:: none

    streamsx-streamtool chappconfig [-h] [--property name=value]
            [--description description] [--User user]
            config-name

Options and arguments

    config-name:
        Name of the app config

    -\--description:
        Specifies a description for the application configuration. The
        description can be 1024 characters in length. If the description
        contains blank characters, it must be enclosed in single or double
        quotation marks. Quotation marks within the description must be
        preceded by a backslash (\).

    -\--property:
        Specifies a property name and value pair to add to or change in the
        configuration. This option can be specified multiple times and has an
        additive effect.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.


*****************************************
getappconfig
*****************************************

The streamtool getappconfig command displays the properties of a configuration
that enables connection to an external application.


This command retrieves the properties and values of a specific configuration
for connecting to an external application.

.. code-block:: none

    streamsx-streamtool getappconfig [-h] [--User user] config-name

Options and arguments

    config-name:
        Name of the app config

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.

*****************************************
lstoolkit
*****************************************

List toolkits from a build service.

.. code-block:: none

    streamsx-streamtool lstoolkit [-h]
            (--all | --id toolkit-id | --name toolkit-name | --regex toolkit-regex)
            [--User user]

Options and arguments

    -a,--all: 
        List all toolkits
 
    -i,--id:
        List a specific toolkit given its toolkit id

    -n,--name:
        List all toolkits with this name

    -r,--regex:
        List all toolkits where the name matches the given regex pattern

*****************************************
rmtoolkit
*****************************************

Remove toolkits from a build service.

.. code-block:: none

    streamsx-streamtool rmtoolkit [-h]
            (--id toolkit-id | --name toolkit-name | --regex toolkit-regex)
            [--User user]

Options and arguments

    -i,--id:
        Specifies the id of the toolkit to delete

    -n,--name:
        Remove all toolkits with this name

    -r,--regex:
        Remove all toolkits where the name matches the given regex pattern

*****************************************
uploadtoolkit
*****************************************

Upload a toolkit to a build service.

.. code-block:: none

    streamsx-streamtool uploadtoolkit [-h] --path toolkit-path [--User user]

Options and arguments

    -p,--path:
        Specifies the path of the indexed toolkit to upload

.. versionadded:: 1.13

*****************************************
updateoperators
*****************************************

Adjust a job configuration while the job is running in order to improve the job performance

.. code-block:: none

    streamsx-streamtool updateoperators [-h] [--jobname job-name]
            [--jobConfig file-name]
            [--parallelRegionWidth parallelRegionName=width]
            [--force] [--User user]
            [jobid]

Options and arguments

    jobid:
        Specifies a job ID

    -\--jobname:
        Specifies the name of the job

    -g,--jobConfig:
        Specifies the name of an external file that defines a job
        configuration overlay. You can use a job configuration overlay to set
        the job configuration when the job is submitted or to change the
        configuration of a running job.

    -\--parallelRegionWidth:
        Specifies a parallel region name and its width.

    -\--force:
        Specifies whether to automatically stop the PEs that need to be
        stopped.

    -U,--User:
        Specifies an IBM Streams user ID that has authority to run the
        command.
