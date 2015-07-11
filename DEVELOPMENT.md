## Development of streamsx.topology project

### Setup

Once you have forked the repository and created your local clone you need to download
these additional developement software tools.

* Apache Ant 1.9.4: The build uses Ant, the version it has been tested with is 1.9.4. - https://ant.apache.org/
* JUnit 4.10: Java unit tests are written using JUnit, tested at version 4.10. - http://junit.org/
* Jacoco 0.7.5: The JUnit tests have code coverage enabled by default, using Jacoco, tested with version 0.7.5. - http://www.eclemma.org/jacoco/
* Scala - Optional, Scala support will be built if SCALA_HOME is set. Development was done with 2.11.7 - http://www.scala-lang.org/

The Apache Ant `build.xml` files are setup to assume that the Junit and Jacoco jars are copied into `$HOME/.ant/lib`.
```
> ls $HOME/.ant/lib
jacocoagent.jar  jacocoant.jar  junit-4.10.jar
```

The project also requires a local install of IBM InfoSphere Streams 4.x, with the environment variable `STREAMS_INSTALL` set to the root of the install. The recommended setup is to source the `bin/streamsprofile.sh` script in the Streams install.
```
> source /opt/ibm/InfoSphere_Streams/4.0.0.0/bin/streamsprofile.sh
InfoSphere Streams environment variables have been set.
```

### Building

The top-level Ant file `streamsx.topology/build.xml` has these main targets:
* `all` (default) : Build the project, including the Java code for the Java Application API, the SPL `com.ibm.streamsx.topology` SPL toolkit and the sample applications. If SCALA_HOME is set then Scala support is also built.
* `clean` : Clean the project
* `test` : Run the JUnit tests, most of the tests are run twice, once in embedded mode (within the JVM) and once in Streams standalone mode.
* `test.report` : Build a test report for the JUnit test runs. This is invoked automatically when the `test` target passes, but in case of a failure, this may be invoked to produce a test report to easily display the failure(s).
* `test.quick` : Run the Junit tests quickly as a sanity check, this runs a subset of the tests, avoiding SPL generation & compilation and code coverage. *This target currently may still invoke some SPL compilation (sc) so may not be a quick as it could be.*

### Additional testing

By default the Ant `test` target does not run the tests against a Streams instance (distributed), as it requires an instance to be running, which may not always be the case. A sub-set of the tests can also be run against a Streams instance like this:
```
cd test/java
ant unittest.distributed
```
This requires that your environment is setup so that `streamtool submitjob` submit jobs to an instance without requiring any authentication. This is the case for the [Streams Quicksttart VM image](http://www-01.ibm.com/software/data/infosphere/stream-computing/trials.html).

### Test reports

Running the test targets produces two reports:
* `test/java/report/junit/index.html` - JUnit test report
* `test/java/report/coverage/index.html` - Code soverage report. Full coverage numbers are obtained by running the top-level `test` and `unittest.distributed` targets.

