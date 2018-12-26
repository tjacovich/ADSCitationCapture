import sys
import os
import json
import adsmsg
from ADSCitationCapture import webhook
from ADSCitationCapture import doi
from ADSCitationCapture import url
from ADSCitationCapture import db
from .test_base import TestBase

import unittest
from ADSCitationCapture import app, tasks
from mock import patch


class TestWorkers(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)


if __name__ == '__main__':
    unittest.main()
