import sys
import os
import json
import adsmsg
from ADSCitationCapture import webhook
from ADSCitationCapture import doi
from ADSCitationCapture import url
from ADSCitationCapture import db
from ADSCitationCapture import forward
from adsmsg import DenormalizedRecord
from .test_base import TestBase

import unittest
from ADSCitationCapture import app, tasks
from mock import patch


class TestWorkers(TestBase):

    def _common_citation_changes_doi(self, status):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2005CaJES..42.1987P'
        citation_change.cited = '...................'
        citation_change.content = '10.5281/zenodo.11020'
        citation_change.content_type = adsmsg.CitationChangeContentType.doi
        citation_change.resolved = False
        citation_change.status = status
        return citation_changes

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)
        
    def test_build_bib_record_no_associated_works(self):
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_bib_record.json")
        with open(content_filename) as f:
            expect_bib_record = json.load(f)
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_nonbib_record.json")
        with open(content_filename) as f:
            expect_nonbib_record = json.load(f)

        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        citation_change = tasks._protobuf_to_adsmsg_citation_change(citation_changes.changes[0])
        doi_id = "10.5281/zenodo.11020" # software
        parsed_metadata = self.mock_data[doi_id]['parsed']
        citations =['']
        db_versions = {"":""}
        bib_record, nonbib_record = forward.build_record(self.app, citation_change, parsed_metadata, citations, db_versions)  
        self.assertEqual(bib_record.toJSON(),expect_bib_record)
        self.assertEqual(nonbib_record.toJSON(),expect_nonbib_record)

    def test_build_bib_record_associated_works(self):
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_bib_record_associated.json")
        with open(content_filename) as f:
            expect_bib_record = json.load(f)
        content_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample_nonbib_record_associated.json")
        with open(content_filename) as f:
            expect_nonbib_record = json.load(f)

        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        citation_change = tasks._protobuf_to_adsmsg_citation_change(citation_changes.changes[0])
        doi_id = "10.5281/zenodo.4475376" # software
        parsed_metadata = self.mock_data[doi_id]['parsed']
        citations =['']
        db_versions = self.mock_data[doi_id]['associated']
        bib_record, nonbib_record = forward.build_record(self.app, citation_change, parsed_metadata, citations, db_versions)  
        
        self.assertEqual(bib_record.toJSON(),expect_bib_record)
        self.assertEqual(nonbib_record.toJSON(),expect_nonbib_record)
if __name__ == '__main__':
    unittest.main()
