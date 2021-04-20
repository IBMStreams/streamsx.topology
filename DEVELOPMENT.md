## Development of streamsx.topology project

### Workflow

The recommended workflow is forking workflow.
https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow

Fork this repository and develop in feature branches in your fork. When ready, sumbmit a pull request against the target branch.

**Please do not use of short-lived temporary development branches in the main IBMStreams/streamsx.topology repo**
If a need does occur, please ensure the branch is deleted once merged.
The main repo should only contain `develop`, release branches (`vX_Y`)and shared development feature branches (`feature/xxx`).

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

The project also requires a local install of IBM InfoSphere Streams 4.x (>= 4.2), with the environment variable `STREAMS_INSTALL` set to the root of the install. The recommended setup is to source the `bin/streamsprofile.sh` script in the Streams install, see [Configuring the IBM Streams environment by running streamsprofile.sh](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.install.doc/doc/ibminfospherestreams-configuring-streams-environment.html)

Use a local installation of the Streams runtime: [Install version 4.3 or later of IBM Streams](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.install.doc/doc/installstreams-container.html) or the free [Streams Quick Start Edition](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.qse.doc/doc/installtrial-container.html).

```
> source /opt/ibm/InfoSphere_Streams/4.3.0.0/bin/streamsprofile.sh
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

### Implementing toolkit messages
This toolkit supports globalized messages with unique message IDs. The guidelines for implementing a message bundle are described in [Messages and National Language Support for Toolkits](https://github.com/IBMStreams/administration/wiki/Messages-and-National-Language-Support-for-toolkits).

### Develop

#### Python - streamsx

Find the sources of the streamsx Python package in the toolkit directory `com.ibm.streamsx.topology/opt/python/packages/streamsx`:

[Python package in the toolkit](https://github.com/IBMStreams/streamsx.topology/tree/develop/com.ibm.streamsx.topology/opt/python/packages/streamsx)

#### Java sources

The Java sources are located in the `java` directory:

[Java sources](https://github.com/IBMStreams/streamsx.topology/tree/develop/java)

### Distributed testing

By default the Ant `test` target does not run the tests against a Streams instance (distributed), as it requires an instance to be running, which may not always be the case. A sub-set of the tests can also be run against a Streams instance like this:

```
cd test/java
ant unittest.distributed
```

or

```
cd test/python
ant test.distributed
```

This requires that your environment is setup so that `streamtool submitjob` submit jobs to an instance without requiring any authentication. This is the case for the [Streams Quick Start Edition](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.qse.doc/doc/installtrial-container.html).

### IBM Cloud Streaming Analytics service testing

Tests are run against the service if these environment variables are set:

* `VCAP_SERVICES` - File containing JSON VCAP services
* `STREAMING_ANALYTICS_SERVICE_NAME` - Name of Streaming Analytics service to use

e.g.

```
export VCAP_SERVICES=$HOME/vcap/my_vcap
export STREAMING_ANALYTICS_SERVICE_NAME=user-streams
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
Please submit changes through a pull request (PR), typically against `develop` or a feature branch.

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
 
 It's recommended that you use branches for your development, modifying the target branch (e.g. `develop` or the feature branch) directly (even locally in the clone) is not recommended, as multiple changes by other developers may be made to the official copy before you have a chance to merge.  
 
 ## Making a release
 
 Assuming code has been tested.
 
1. Obtain a clean clone of `IBMStreams/streamsx.topology`
    
    * Ensure clone is clean using `git status` or `git clean -xfd` from top level
    
2. Switch to the correct branch for the release - `git checkout develop` or `git checkout v1_15`

    * Traditionally initial releases for an X.Y sequence are from develop until develop needs to have new development for X.Y+1
    * At that point a branch is created vX_Y based off develop (e.g. v1_15)
    * Bug fix releases of older releases are from the vX_Y branch
    
3. Change these four files to have the **equivalent** correct version:

    * `release-pom.xml` - replace version for Maven Central 1.16-alpha-0, for example 1.16.1
    * `com.ibm.streamsx.topology/info.xml` - Uses SPL convention, e.g. for alpha 1.16.0.alpha, 1.16.0.beta, 1.16.8
    * `com.ibm.streamsx.topology/opt/python/packages/streamsx/_streams/_version.py` - Use Python PEP396 convention, 1.16.0a, 1.16.0b, 1.16.8 - Note the third value is always bumped for a release within the same X.Y sequence.
    * `com.ibm.streamsx.topology/CHANGELOG.md` - replace *latest/develop* by the correct released version
    * Once a GA (non-alpha, non-beta) release is made in an X.Y.Z series then all future releases X.Y.W (W>Z) are GA
    
3a. If creating an new X.Y+1 sequence (e.g. 1.17 replacing 1.16) then:

   * Worth searching all files in the repo for the fixed string `X.Y` (e.g. `fgrep 1.16`) to see if anything else should be changed.
   * Add & commit any modified files.
  
4. Add and commit the two files changed **and push to IBMStreams**

    * `git add com.ibm.streamsx.topology/info.xml com.ibm.streamsx.topology/opt/python/packages/streamsx/_streams/_version.py release-pom.xml`
    * `git commit -m "1.16.8 release"`
    * `git push origin develop` using the release branch
    
5. Build using `ant release` at the top-level

    * This creates the release artifacts for the topology release under a newly created release directory `release-streamsx.topology` at the top level.
    
6. Draft a new release at https://github.com/IBMStreams/streamsx.topology/releases

    * Typically can copy the release notes from the previous release and modify as needed.
    * Mark as pre-release if release is alpha or beta
    * Add a tag **on the correct release branch** that matches the version from `info.xml`
    * Save as needed

7. Attach jars from step 5 to the release

8. Publish the release

9. Make a release of the Python `streamsx` package following:
    * https://github.com/IBMStreams/pypi.streamsx/blob/develop/build/README.md
    * This uses the release artifact uploaded to the release in step 7.
    * Note: Select the correct branch in `pypi.streamsx`
    
10. At readthedocs for streamsx.topology if required you can make the specific doc set for the tag active. The tag is based upon the tag in the streamsx.topology release.
    * https://readthedocs.org/projects/streamsxtopology/versions/

11. Deploy the release on Maven central

   * `ant maven-deploy`


