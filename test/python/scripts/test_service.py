# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
import unittest
import unittest.mock

import streamsx.scripts.service

class TestSasServiceScript(unittest.TestCase):

    def _service_args(self, add_name=False):
        args = []
        args.append('service.py')
        if add_name:
            args.append('--service-name')
            args.append(os.environ['STREAMING_ANALYTICS_SERVICE_NAME'])
        return args

    def _run(self, args, rc=0, running=True):
        with unittest.mock.patch('sys.argv', args):
             sr = streamsx.scripts.service.main()
             self.assertEqual(rc, sr['return_code'])
             if rc == 0:
                 self.assertEqual('running', sr['status'])
                 self.assertEqual('STARTED', sr['state'])
                 self.assertTrue(int(sr['job_count']) >= 0)
             return sr
  

    def test_service(self):
        args = self._service_args()
        args.append('start')
        self._run(args)
  
        args = self._service_args()
        args.append('status')
        self._run(args)

        args = self._service_args()
        args.append('--full-response')
        args.append('status')
        sr = self._run(args)
        self.assertTrue(len(sr) > 4)
