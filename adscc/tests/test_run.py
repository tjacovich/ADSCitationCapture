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


    def test_run(self):
        sys.path.append(self.app.conf['PROJ_HOME'])
        from run import run

        # Call the task to check if it should be extracted but mock the extraction task
        with patch.object(tasks.task_check_citation, 'delay', return_value=None) as task_extract:
            message = {}
            run(message)
            self.assertTrue(task_extract.called)

if __name__ == '__main__':
    unittest.main()
