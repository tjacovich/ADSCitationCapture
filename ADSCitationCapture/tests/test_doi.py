import unittest
import httpretty
from ADSCitationCapture import app, tasks
from ADSCitationCapture import doi
from .test_base import TestBase


class TestWorkers(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)

    def test_software_doi(self):
        doi_id = "10.5281/zenodo.11020" # software
        expected_response_content = self.mock_data[doi_id]['raw']
        expected_parsed_response = self.mock_data[doi_id]['parsed']
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, self.app.conf['DOI_URL']+doi_id, body=expected_response_content)
        raw_metadata = doi.fetch_metadata(self.app.conf['DOI_URL'], doi_id)
        parsed_metadata = doi.parse_metadata(raw_metadata)
        self.assertEqual(raw_metadata, expected_response_content)
        self.assertEqual(parsed_metadata, expected_parsed_response)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history

    def test_non_software_doi(self):
        doi_id = "10.1016/j.dsr2.2008.10.030" # Not software
        expected_response_content = ''
        expected_parsed_response = {}
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, self.app.conf['DOI_URL']+doi_id, body=expected_response_content)
        raw_metadata = doi.fetch_metadata(self.app.conf['DOI_URL'], doi_id)
        parsed_metadata = doi.parse_metadata(raw_metadata)
        self.assertEqual(raw_metadata, expected_response_content)
        self.assertEqual(parsed_metadata, expected_parsed_response)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history


if __name__ == '__main__':
    unittest.main()
