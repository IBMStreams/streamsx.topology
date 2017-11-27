## Adding tests
To add another suite of tests, create a file of the form *tests.py. Any classes inside the created module which
inherit from unittest.TestCase will be added to the list of suites which are run by the test runner. For example

**string_tests.py**
```Python
import unittest
import logging
logger = logging.getLogger('string_tests')
class TestStringFeatures(unittest.TestCase):
    def test_upper(self):
        logger.debug("Beginning test: test_upper")
        self.assertEqual("asdf".upper(), "ASDF")
```

## Running tests
To run all test suites, just invoke:
```
python -m unittest discover
```

To run just one test suite, invoke it like this:
```
python -m unittest -v string_tests
```

Run a subset of tests using the standard Python unittest approaches.

## Configuring the rest tests
A default set of SWS username and password for the rest_local_tests are supplied:
* username: streamsadmin
* password: passw0rd

To change them, specify the environment variables:
* STREAMS_USERNAME
* STREAMS_PASSWORD

To run the rest_bluemix_tests and submit_tests, specify these environment variables:
* STREAMS_INSTALL
* VCAP_SERVICES
* STREAMING_ANALYTICS_SERVICE_NAME
