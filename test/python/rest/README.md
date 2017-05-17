## Adding tests
To add another suite of tests, create a file of the form *tests.py. Any classes inside the created module which
inherit from unittest.TestCase will be added to the list of suites which are run by the test runner. For example

**string_test.py**
```Python
class TestStringFeatures(unittest.TestCase):
    def test_upper(self):
        unittest.assertEquals("asdf".upper(), "ASDF")
```

## Running tests
To run the test suite, just invoke
```
python -m unittest discover
```

Run a subset of tests using the standard Python unittest approaches.

## Configuring the rest tests
A default set of SWS username and password for the rest_local_tests are supplied:
* username: streamsadmin
* password: passw0rd

To change them, specify the environment variables:
* STREAMS_USERNAME
* STREAMS_PASSWORD

To run the rest_bluemix_tests, specify the environment variables:
* VCAP_SERVICES
* STREAMING_ANALYTICS_SERVICE_NAME
