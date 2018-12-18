import unittest
import httpretty
from ADSCitationCapture import app, tasks
from ADSCitationCapture import url
from .test_base import TestBase


class TestWorkers(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)

    def test_url_is_alive(self):
        valid_url = "https://zenodo.org/record/1011088"
        expected_response_content = ''
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, valid_url, status=200, body="<!DOCTYPE html>\n <html lang=\"en\" dir=\"ltr\"> \n  <head></head>\n  <body></body>\n </html>")
        is_url_alive = url.is_alive(valid_url)
        self.assertTrue(is_url_alive)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history

    def test_url_is_dead(self):
        invalid_url = "https://zenodo.org/record/1"
        expected_response_content = ''
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, invalid_url, status=410, body="<!DOCTYPE html>\n <html lang=\"en\" dir=\"ltr\"> \n  <head></head>\n  <body></body>\n </html>")
        is_url_alive = url.is_alive(invalid_url)
        self.assertFalse(is_url_alive)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history


if __name__ == '__main__':
    unittest.main()
