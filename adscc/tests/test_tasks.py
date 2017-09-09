import sys
import os
import json

import unittest
from adscc import app, tasks
from mock import patch


class TestWorkers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = tasks.app.conf['PROJ_HOME']
        self._app = tasks.app
        self.app = app.ADSCitationCaptureCelery('test', proj_home=self.proj_home, local_config={})
        tasks.app = self.app # monkey-patch the app object

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.app.close_app()
        tasks.app = self._app


    def test_process_citation_changes(self):
        #message = {}
        #tasks.task_process_citation_changes(message)
        self.assertTrue(True)

    def test_task_output_results(self):
        with patch('adscc.app.ADSCitationCaptureCelery.forward_message', return_value=None) as forward_message:
            msg = { }
            tasks.task_output_results(msg)
            self.assertTrue(forward_message.called)

if __name__ == '__main__':
    unittest.main()
