# Testing
## Test targets:

### `test/java`

These Ant targets include the Scala tests.

Some of the JUnit tests include Python application API and decorator operator tests (e.g. test packages with `python`, `splpy`).

* `unitest.main` - Runs Java application api tests using `EMBEDDED_TESTER` context 
* `unitest.standalone` - Runs Java application api tests using `STANDALONE_TESTER` context
* `unitest.distributed` - Runs Java application api tests using `DISTRIBUTED_TESTER` context

### `test/python`

Ant targets:

* `test.python2` - Runs a subset of the tests for functionality that is supported on Python 2.
* `test.python3` - Runs Python tests.

Most testing is performed using Python unittest.
* `test/python/topology` - Python topology tests (application API).
  * Any test starting with `test2` uses the testing capability supported by the toolkit and thus only runs on Streams 4.2 and Python 3.5.
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
