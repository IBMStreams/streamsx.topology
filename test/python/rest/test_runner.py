import logging
import unittest
import subprocess
import sys, os
from os.path import dirname

if __name__ == '__main__':
    # Ensure correct rest.py is used. Prepend path to sys.path
    python_dir = dirname(dirname(os.path.realpath(__file__)))
    packages_dir = os.path.join(python_dir, 'packages')
    sys.path.insert(0, packages_dir)

    # Set up a logging framework. Test output from streams.test.* will percolate up to the streamsx.test logger
    # and be output on stdout.
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger('streamsx').setLevel(logging.DEBUG)

    # Search for valid test suites.
    if len(sys.argv) < 2:
        raise ValueError("Please provide either 'all', 'local', or 'bluemix' as an argument")

    mode = sys.argv[1]
    if mode == "all":
        # Run all tests
        suite = unittest.TestLoader().discover('.', pattern='*tests.py')

    elif mode == "local":
        # Run only local tests
        suite = unittest.TestLoader().discover('.', pattern='*local_tests.py')

    elif mode == "bluemix":
        # Run only Bluemix tests
        suite = unittest.TestLoader().discover('.', pattern='*bluemix_tests.py')

    else:
        raise ValueError("'" + mode + "' is an invalid argument to test_runner.")

    unittest.TextTestRunner(verbosity=4).run(suite)

    #subprocess.call(["st canceljob $(st lsjobs | tail -n +3| cut -d' ' -f 3|tr '\n' ' ')"])