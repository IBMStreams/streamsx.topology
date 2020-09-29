# Testing

Note: Build the toolkit prior testing, run `ant` in repository root directory

Note: Some "distributed" tests require consistent region be configured: [Configuring a checkpoint data store](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.cfg.doc/doc/ibminfospherestreams-configuring-checkpoint-data-store.html)

Note: Python 2.7 is no longer supported with v1.14 and later.

Note: Python 3.6 is required

See end of document for tests run to verify a release.

## `test/java`

Some of the JUnit tests include Python application API and decorator operator tests (e.g. test packages with `python`, `splpy`).

Ant targets:

* `unittest.quick` - Runs `unittest.main` and `unittest.standalone`
* `unittest.main` - Runs Java application api tests using `EMBEDDED_TESTER` context
* `unittest.standalone` - Runs Java application api tests using `STANDALONE_TESTER` context
* `unittest.distributed` - Runs Java application api tests using `DISTRIBUTED_TESTER` context. Streams instance must be started prior the test.
* `unittest.streaminganalytics` - Runs Java application api tests using the `STREAMING_ANALYTICS_SERVICE` context. Streaming Analytics service must be started prior the test.
* `unittest.streaminganalytics.remote` - Runs Java application api tests using the `STREAMING_ANALYTICS_SERVICE` context and performs builds remotely. Streaming Analytics service must be started prior the test.
* `unittest.restapi` - Runs the Java rest tests using the `DISTRIBUTED` context (requires `STREAMS_REST_URL` environment variable) and the `STREAMING_ANALYTICS_SERVICE` context (requires `STREAMING_ANALYTICS_SERVICE_NAME` and `VCAP_SERVICES` environment variables). Streaming Analytics service must be started prior the test.

### Changing Streams install or toolkit release

By default the tests run against:
 * Streams release at $STREAMS_INSTALL
   * including compiling the underlying SPL applications
   * running or submitting the bundles
 * Toolkit at `streamsx.topology/com.ibm.streamsx.topology`

The version of Streams used to compile the applications can be
set by using the `topology.install.compile` ant property.

```
cd test/java
ant unittest.distributed -Dtopology.install.compile=/opt/ibm/InfoSphere_Streams/4.3.0.0
```

The release of the toolkit used can be specified using `topology.toolkit.release` ant property.

```
cd test/java
ant unittest.distributed -Dtopology.toolkit.release=$HOME/testtk/com.ibm.streamsx.topology
```

### Running selected Java test classes

The tests can be specified using `topology.test.base.pattern` ant property.

Run using `EMBEDDED_TESTER` context:
```
cd test/java
ant unittest.main -Dtopology.test.base.pattern='**/SimpleEmbeddedTest.java'
```

Run using `STANDALONE_TESTER` context:
```
cd test/java
ant unittest.standalone -Dtopology.test.base.pattern='**/FileStreamsTest.java'
```

Run using `DISTRIBUTED_TESTER` context:
```
cd test/java
ant unittest.distributed -Dtopology.test.base.pattern='**/PublishSubscribeTest.java'
```

## `test/python`

Ant targets:

* `test` - Runs Python tests calling the targets `test.application.api` and `test.translation`
* `test.application.api` - Runs Python topology API tests (all unittests in `test/python/topology`)
* `test.distributed` - Runs some TestDistributed* tests using Streams instance
* `test.sas` - Runs some TestSas* tests using Streaming Analytics Service
* `test.translation` - Runs translation tests (globalization support)

Most testing is performed using Python unittest.
* `test/python/topology` - Python topology tests (application API).
  * Any test starting with `test2` uses the testing capability supported by the toolkit.
  * Most `test2` tests have three classes running the same test in standalone, distributed and streaming analytics service. Typically the base class is the standalone tests, then subclasses exist for distributed and streaming analytics.
  * `test2` standalone tests are skipped if environment variable `STREAMS_INSTALL` is not set.
  * `test2` distributed tests are skipped if any of the environment variables `STREAMS_INSTALL`, `STREAMS_DOMAIN_ID` or `STREAMS_INSTANCE_ID`is not set is not set.
  * `test2` streaming analytics tests are skipped if environment variable `VCAP_SERVICES` is not set.
* `test/rest` -  Python Streams REST API tests.
  
 These tests can be run directly using Python, e.g.
 ```
 cd test/python/topology
 # Run all test2 tests
 python3 -u -m unittest test2*.py
  
 # Run all tests in test2_spl.py
 python3 -u -m unittest test2_spl
 
 # Run a single class from a test file
 # e.g. run only distributed tests for test2_spl
 python3 -u -m unittest test2_spl.TestDistributedSPL
 
 # Run a single test
 python3 -u -m unittest test2_spl.TestDistributedSPL.test_SPLBeaconFilter
 ```

Naming pattern for Python test classes:

* TestXyz - Tests Xyz using standalone or a test that does not require a Streams application (e.g. api testing)
* TestDistributedXyz - Tests Xyz using distributed
* TestSasXyz - Tests Xyz using Streaming Analytics service

### Running selected Python test

Test is performed with streamsx in the repository.

```
cd test/python
ant test.single -Dtopology.unittest.name='test2_pkg.TestPackages.test_TopologySymlinkFile'
```

### Running Python distributed test

Test is performed with streamsx in the repository.

```
cd test/python
ant test.distributed
```

### Python `test2_scikit.py` requires `scikit-learn` installed:

```
pip install scikit-learn
cd test/python/topology
python3 -u -m unittest test2_scikit.TestScikit.test_scikit_learn
```

## Quick test set for a release

Tests assume setup is correct for running distributed.

### Java

Embedded and standalone tests only:

```
cd test/java
ant unittest.quick
```

### Python

Distributed tests only (Streams instance must be started prio the test):

```
cd test/python
ant test.distributed
```

### Perform quick tests on release package 

To create a release package and perform a quick test (python rest test, distributed only) on this package, run the following:

```
cd test/release
ant
```

### Perform Java Topology quick test with release from Maven Central

To perform a quick test (download topology.jar from Maven and build bundle from Java Topology app), run the following:

```
cd test/release
ant mvntest
```

## Full test set for a release

Full set of tests assumes setup is correct for running distributed and Streaming Analytics
 
### test/java

Run these `ant` targets in `test/java`

* `unittest.quick`
* `unittest.distributed`
* `unittest.restapi`

### test/spl

Run basic pub-sub in SPL (distributed and Streaming Analytics):
```
cd test/spl/tests
python3 -u -m unittest discover -v 
```

### test/python

#### Python 3.6

Run `python3 -u -m unittest discover -v` in each of these directories:

   * `test/python/topology`
   * `test/python/spl/tests`
   * `test/python/scripts`
   * `test/python/rest`


