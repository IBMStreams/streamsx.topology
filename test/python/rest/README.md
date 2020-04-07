# Python REST API Tests

Prepare the tests with `ant all` in repository root directory. This prepares some toolkits and/or creates toolkit.xml files required by some test cases.

These tests require a distributed Streams instance:

* Distributed instance (local Streams)
* Distributed instance (Streams in Cloud Pak for Data)
* Streaming Analytics service

## Configuring the REST tests

For the "rest local tests" specify the environment variables:

* STREAMS_DOMAIN_ID
* STREAMS_INSTANCE_ID
* STREAMS_USERNAME
* STREAMS_PASSWORD

To run the Streaming Analytics service tests, specify the environment variables:

* VCAP_SERVICES
* STREAMING_ANALYTICS_SERVICE_NAME

To run the tests with Streams in Cloud Pak for Data, specify the environment variables:

* CP4D_URL
* STREAMS_INSTANCE_ID
* STREAMS_USERNAME
* STREAMS_PASSWORD


## Running the REST tests

To run the test suite, just invoke
```
python -u -m unittest discover -v
```

Run a subset of tests using the standard Python unittest approaches.


## Adding tests

To add another suite of tests, create a file of the form *tests.py. Any classes inside the created module which
inherit from unittest.TestCase will be added to the list of suites which are run by the test runner. For example

**string_test.py**
```Python
class TestStringFeatures(unittest.TestCase):
    def test_upper(self):
        unittest.assertEquals("asdf".upper(), "ASDF")
```
