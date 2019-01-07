import sys
import os
import json
import adsmsg
from ADSCitationCapture import webhook
from ADSCitationCapture import doi
from ADSCitationCapture import url
from ADSCitationCapture import db
from ADSCitationCapture import api
from .test_base import TestBase

import unittest
from ADSCitationCapture import app, tasks
from mock import patch


class TestWorkers(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def tearDown(self):
        TestBase.tearDown(self)

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

    def test_process_new_citation_changes_doi(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertTrue(fetch_metadata.called)
                self.assertTrue(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertTrue(get_canonical_bibcode.called)
                self.assertTrue(get_canonical_bibcodes.called)
                self.assertTrue(get_citations_by_bibcode.called)
                self.assertTrue(store_citation_target.called)
                self.assertTrue(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertTrue(forward_message.called)
                self.assertTrue(webhook_emit_event.called)

    def test_process_updated_citation_changes_doi(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=True) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertTrue(get_canonical_bibcode.called)
                self.assertTrue(get_canonical_bibcodes.called)
                self.assertTrue(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertTrue(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertTrue(forward_message.called)
                self.assertTrue(webhook_emit_event.called)

    def test_process_deleted_citation_changes_doi(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.deleted)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=True) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertTrue(get_canonical_bibcode.called)
                self.assertTrue(get_canonical_bibcodes.called)
                self.assertTrue(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertTrue(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertTrue(forward_message.called)
                self.assertTrue(webhook_emit_event.called)

    def test_process_new_citation_changes_doi_when_target_exists_citation_doesnt(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertTrue(get_canonical_bibcode.called)
                self.assertTrue(get_canonical_bibcodes.called)
                self.assertTrue(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertTrue(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertTrue(forward_message.called)
                self.assertTrue(webhook_emit_event.called)

    def test_process_updated_citation_changes_doi_when_target_exists_citation_doesnt(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertFalse(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called)

    def test_process_deleted_citation_changes_doi_when_target_exists_citation_doesnt(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.deleted)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertFalse(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called)

    def test_process_new_citation_changes_doi_when_target_and_citation_exist(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=True) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertFalse(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called)

    def test_process_updated_citation_changes_doi_when_citation_doesnt_exist(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertFalse(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called)

    def test_process_deleted_citation_changes_doi_when_citation_doesnt_exist(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.deleted)
        doi_id = "10.5281/zenodo.11020" # software
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertFalse(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called)

    def test_process_citation_changes_ascl(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017MNRAS.470.1687B'
        citation_change.cited = '...................'
        citation_change.content = 'ascl:1210.002'
        citation_change.content_type = adsmsg.CitationChangeContentType.pid
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=None) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value={}) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertTrue(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called) # because we don't know if an URL is software

    def test_process_citation_changes_url(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = 'https://github.com/ComputationalRadiationPhysics/graybat'
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=None) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value={}) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertTrue(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called) # because we don't know if an URL is software


    def test_process_citation_changes_malformed_url(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = 'malformedhttps://github.com/ComputationalRadiationPhysics/graybat'
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=None) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value={}) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertTrue(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called) # because we don't know if an URL is software

    def test_process_citation_changes_empty(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = ''
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=None) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value={}) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertFalse(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called) # Not executed because content is empty
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertFalse(store_citation_target.called)
                self.assertFalse(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called) # because we don't know if an URL is software

    def test_process_new_citation_changes_doi_unparsable_http_response(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value="Unparsable response") as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value={}) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertTrue(fetch_metadata.called)
                self.assertTrue(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertTrue(store_citation_target.called)
                self.assertTrue(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called) # because we don't know if an URL is software


    def test_process_new_citation_changes_doi_http_error(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        with patch.object(db, 'citation_already_exists', return_value=False) as citation_already_exists, \
            patch.object(db, 'get_citation_target_metadata', return_value={}) as get_citation_target_metadata, \
            patch.object(doi, 'fetch_metadata', return_value=None) as fetch_metadata, \
            patch.object(doi, 'parse_metadata', return_value={}) as parse_metadata, \
            patch.object(url, 'is_alive', return_value=True) as url_is_alive, \
            patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing) as get_canonical_bibcode, \
            patch.object(api, 'get_canonical_bibcodes', return_value=[]) as get_canonical_bibcodes, \
            patch.object(db, 'get_citations_by_bibcode', return_value=[]) as get_citations_by_bibcode, \
            patch.object(db, 'store_citation_target', return_value=True) as store_citation_target, \
            patch.object(db, 'store_citation', return_value=True) as store_citation, \
            patch.object(db, 'update_citation', return_value=True) as update_citation, \
            patch.object(db, 'mark_citation_as_deleted', return_value=True) as mark_citation_as_deleted, \
            patch.object(db, 'get_citations', return_value=[]) as get_citations, \
            patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True) as forward_message, \
            patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(citation_already_exists.called)
                self.assertTrue(get_citation_target_metadata.called)
                self.assertTrue(fetch_metadata.called)
                self.assertFalse(parse_metadata.called)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(get_canonical_bibcode.called)
                self.assertFalse(get_canonical_bibcodes.called)
                self.assertFalse(get_citations_by_bibcode.called)
                self.assertTrue(store_citation_target.called)
                self.assertTrue(store_citation.called)
                self.assertFalse(update_citation.called)
                self.assertFalse(mark_citation_as_deleted.called)
                self.assertFalse(get_citations.called)
                self.assertFalse(forward_message.called)
                self.assertFalse(webhook_emit_event.called) # because we don't know if an URL is software


    def test_task_output_results(self):
        with patch('ADSCitationCapture.app.ADSCitationCaptureCelery.forward_message', return_value=None) as forward_message:
            citation_change = adsmsg.CitationChange(content_type=adsmsg.CitationChangeContentType.doi, status=adsmsg.Status.active)
            parsed_metadata = {
                    'bibcode': 'test123456789012345',
                    'authors': ['Test, Unit'],
                    'normalized_authors': ['Test, U']
                    }
            citations = []
            tasks.task_output_results(citation_change, parsed_metadata, citations)
            self.assertTrue(forward_message.called)
            self.assertEqual(forward_message.call_count, 2)

if __name__ == '__main__':
    unittest.main()
