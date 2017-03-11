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
python test_runner.py
```

### Configuring the rest tests
If you would like to supply a different SWS username and password for the rest tests, modify the sws_credentials.json
to include the correct values.