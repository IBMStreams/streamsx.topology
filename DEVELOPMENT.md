## Development of streamsx.topology project

### Workflow

The recommended workflow is forking workflow.
https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow

Fork this repository and develop in feature branches in your fork. When ready, sumbmit a pull request against the target branch.

**Please do not use of short-lived temporary development branches in the main IBMStreams/streamsx.topology repo**
If a need does occur, please ensure the branch is deleted once merged.
The main repo should only contain `master`, release branches (`vX_Y`)and shared development feature branches (`feature/xxx`).

### Setup

Once you have forked the repository and created your local clone you need to download
these additional development software tools.

* Apache Ant 1.9.2 or later: The build uses Ant, the version it has been tested with is 1.9.4 and 1.9.2. - https://ant.apache.org/
* JUnit 4.10: Java unit tests are written using JUnit, tested at version 4.10. - http://junit.org/
* Jacoco 0.7.5: The JUnit tests have code coverage enabled by default, using Jacoco, tested with version 0.7.5. - http://www.eclemma.org/jacoco/
* Python 3.5: Required to provide support for Python. 
* Scala - Optional, Scala support will be built if SCALA_HOME is set. Development was done with 2.11.7 - http://www.scala-lang.org/

The Apache Ant `build.xml` files are setup to assume that the Junit and Jacoco jars are copied into `$HOME/.ant/lib`.
```
> ls $HOME/.ant/lib
jacocoagent.jar  jacocoant.jar  junit-4.10.jar
```

The project also requires a local install of IBM InfoSphere Streams 4.x (>= 4.0.1.0), with the environment variable `STREAMS_INSTALL` set to the root of the install. The recommended setup is to source the `bin/streamsprofile.sh` script in the Streams install.
```
> source /opt/ibm/InfoSphere_Streams/4.0.1.0/bin/streamsprofile.sh
InfoSphere Streams environment variables have been set.
```

For building the documentation for the Python support Sphinx must be installed:

```
pip install sphinx
pip install sphinx_rtd_theme
```

The Python documentation is only invoked through the top-level `release`
target or the default target in `python/build.xml`.
Creation of the Python documentation can be skipped by setting the property
`topology.build.sphinx=no`, e.g.

```
ant -Dtopology.build.sphinx=no release
```

For further information on writing Python docstrings, see [Python Docstring conventions](#python-docstring-conventions).


### Building

The top-level Ant file `streamsx.topology/build.xml` has these main targets:
* `all` (default) : Build the project, including the Java code for the Java Application API, the SPL `com.ibm.streamsx.topology` SPL toolkit and the sample applications. If SCALA_HOME is set then Scala support is also built.
* `clean` : Clean the project
* `test` : Run the JUnit tests, most of the tests are run twice, once in embedded mode (within the JVM) and once in Streams standalone mode.
* `test.report` : Build a test report for the JUnit test runs. This is invoked automatically when the `test` target passes, but in case of a failure, this may be invoked to produce a test report to easily display the failure(s).
* `test.quick` : Run the Junit tests quickly as a sanity check, this runs a subset of the tests, avoiding SPL generation & compilation and code coverage. *This target currently may still invoke some SPL compilation (sc) so may not be a quick as it could be.*

### Implementing toolkit messages
This toolkit supports globalized messages with unique message IDs. The guidelines for implementing a message bundle are described in [Messages and National Language Support for Toolkits](https://github.com/IBMStreams/administration/wiki/Messages-and-National-Language-Support-for-toolkits).

### Distributed testing

By default the Ant `test` target does not run the tests against a Streams instance (distributed), as it requires an instance to be running, which may not always be the case. A sub-set of the tests can also be run against a Streams instance like this:
```
cd test/java
ant unittest.distributed
```
This requires that your environment is setup so that `streamtool submitjob` submit jobs to an instance without requiring any authentication. This is the case for the [Streams Quicksttart VM image](http://www-01.ibm.com/software/data/infosphere/stream-computing/trials.html).

### IBM Cloud Streaming Analytics service testing

Tests are run against the service if these environment variables are set:

* `VCAP_SERVICES` - File containing JSON VCAP services
* `STREAMING_ANALYTICS_SERVICE_NAME` - Name of Streaming Analytics service to use

e.g.

```
export VCAP_SERVICES=$HOME/vcap/my_vcap
export STREAMING_ANALYTICS_SERVICE_NAME=debrunne-streams2
```

The tests are run with this target:

```
cd test/python
ant test.application.api
```

They can be run alone by (need to set environment variable `PYTHONPATH`):

```
cd test/python/topology
python3 -m unittest test_streaming_analytics_service 
```

### Test reports

Running the test targets produces two reports:
* `test/java/report/junit/index.html` - JUnit test report
* `test/java/report/coverage/index.html` - Code soverage report. Full coverage numbers are obtained by running the top-level `test` and `unittest.distributed` targets.

### Python Docstring conventions

Python docstrings use the Google style: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html

How to cross reference Python classes, modules, methods, functions etc: http://www.sphinx-doc.org/en/stable/domains.html#cross-referencing-python-objects

When creating cross reference to entity in another module, display the name in short form(~):
```python
    def function(arg1):
        """Calls :py:func:`~.othermodule.tool`"""
        from othermodule import tool
        tool()
```

When referencing function arguments, use a single backquote(`):
```python
    def function(arg1):
        """function summary
        Args:
            arg1 (str): argument 1
        Returns:
            str: return `arg1`
        """
        return arg1
```

When referencing environment variable, use bold(**):
```python
    """
    **VCAP_SERVICE** is an environment variable
    """
```

When referencing types in Args or Returns, use the type name directly.  Sphinx would convert the reference into hyperlinks, and this ensures the builtin help() will display the arguments and return nicely. 
```python
    def function(arg1):
        """function summary
        Args:
            arg1 (UserType1): argument 1
        Returns:
            UserType2: return something
        """
```

Container types such as lists and dictionaries can use the following syntax:
```python
    def function(arg1, arg2):
        """function summary
        Args:
            arg1 (list(int)): list of integers
            arg2 (dict(str, int)): mapping of str to int
        Returns:
            tuple(float, float): returns two float tuples
        """
```

The `__init__` method should be documented on the `__init__` method itself (not in the class level docstring).  This results in better rendering for the builtin help() function. 

## Proposing changes
Please submit changes through a pull request (PR), typically against `master` or a feature branch.

For a pull request:

 * **Restrict to a single issue**, this speeds up acceptance and makes it easier to merge across release branches if needed.
    * Don't lump several unrelated fixes/features into a single PR
    * Don't fix/change formatting in unrelated code in the same PR
    * Don't fix unrelated documentation/comments in the same PR
    * Propose such unrelated fixes in new PRs, it's git, branches are cheap.
    * If unsure if something is related, just ask!
 * Features and defect fixes are typically associated with an issue.
    * For a defect, create an issue that describes the problem as its summary (not the fix).
 * Describe what changes you made in the PR summary (or indirectly in the associated issue)
    * Help out reviewers with explanations, don't make them have to make assumuptions to review the code.
 * Describe what tests were run.
 
 It's recommended that you use branches for your development, modifying the target branch (e.g. `master` or the feature branch) directly (even locally in the clone) is not recommended, as multiple changes by other developers may be made to the official copy before you have a chance to merge.  
