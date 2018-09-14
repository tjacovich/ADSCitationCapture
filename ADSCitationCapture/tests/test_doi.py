import unittest
import httpretty
from ADSCitationCapture import app, tasks
from ADSCitationCapture import doi


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

    def test_doi_is_software(self):
        doi_id = "10.5281/zenodo.11020" # software
        expected_response_content = '<?xml version="1.0" encoding="UTF-8"?>\n<resource xmlns="http://datacite.org/schema/kernel-3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://datacite.org/schema/kernel-3 http://schema.datacite.org/meta/kernel-3/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/zenodo.11020</identifier>\n  <creators>\n    <creator>\n      <creatorName>Dan Foreman-Mackey</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Adrian Price-Whelan</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Geoffrey Ryan</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Emily</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Michael Smith</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Kyle Barbary</creatorName>\n    </creator>\n    <creator>\n      <creatorName>David W. Hogg</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Brendon J. Brewer</creatorName>\n    </creator>\n  </creators>\n  <titles>\n    <title>triangle.py v0.1.1</title>\n  </titles>\n  <publisher>ZENODO</publisher>\n  <publicationYear>2014</publicationYear>\n  <dates>\n    <date dateType="Issued">2014-07-24</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="URL">http://zenodo.org/record/11020</alternateIdentifier>\n  </alternateIdentifiers>\n  <relatedIdentifiers>\n    <relatedIdentifier relationType="IsSupplementTo" relatedIdentifierType="URL">https://github.com/dfm/triangle.py/tree/v0.1.1</relatedIdentifier>\n  </relatedIdentifiers>\n  <rightsList>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n    <rights rightsURI="">Other (Open)</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;p&gt;This is a citable release with a better name.&lt;/p&gt;</description>\n  </descriptions>\n</resource>'
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, self.app.conf['DOI_URL']+doi_id, body=expected_response_content)
        is_software = doi.is_software(self.app.conf['DOI_URL'], doi_id)
        self.assertTrue(is_software)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history

    def test_doi_is_not_software(self):
        doi_id = "10.1016/j.dsr2.2008.10.030" # Not software
        expected_response_content = ''
        httpretty.enable()  # enable HTTPretty so that it will monkey patch the socket module
        httpretty.register_uri(httpretty.GET, self.app.conf['DOI_URL']+doi_id, body=expected_response_content)
        is_software = doi.is_software(self.app.conf['DOI_URL'], doi_id)
        self.assertFalse(is_software)
        httpretty.disable()
        httpretty.reset()   # clean up registered urls and request history


if __name__ == '__main__':
    unittest.main()
