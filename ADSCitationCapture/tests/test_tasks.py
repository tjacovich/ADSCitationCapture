import sys
import os
import json
import adsmsg
from datetime import datetime
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
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertTrue(mocked['fetch_metadata'].called)
            self.assertTrue(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertTrue(mocked['get_canonical_bibcodes'].called)
            self.assertTrue(mocked['get_citations_by_bibcode'].called)
            self.assertTrue(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertTrue(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertTrue(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertTrue(mocked['store_event'].called)
            self.assertTrue(mocked['webhook_dump_event'].called)
            self.assertTrue(mocked['webhook_emit_event'].called)


    def test_process_updated_citation_changes_doi(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=True), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertTrue(mocked['get_canonical_bibcodes'].called)
            self.assertTrue(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertTrue(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertTrue(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_new_citation_with_associated_works(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        doi_id = "10.5281/zenodo.4475376" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=self.mock_data["2017zndo....248351D"]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'fetch_all_versions_doi': patch.object(doi, 'fetch_all_versions_doi',  return_value=self.mock_data[doi_id]['versions']), \
                'get_associated_works_by_doi': patch.object(db, 'get_associated_works_by_doi', return_value=self.mock_data[doi_id]['associated']), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertTrue(mocked['fetch_metadata'].called)
            self.assertTrue(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertTrue(mocked['get_canonical_bibcodes'].called)
            self.assertTrue(mocked['get_citations_by_bibcode'].called)
            self.assertTrue(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertTrue(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertTrue(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['fetch_all_versions_doi'].called)
            self.assertTrue(mocked['get_associated_works_by_doi'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertTrue(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertTrue(mocked['store_event'].called)
            self.assertTrue(mocked['webhook_dump_event'].called)
            self.assertTrue(mocked['webhook_emit_event'].called)

    def test_process_deleted_citation_changes_doi(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.deleted)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=True), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertTrue(mocked['get_canonical_bibcodes'].called)
            self.assertTrue(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertTrue(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertTrue(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_new_citation_changes_doi_when_target_exists_citation_doesnt(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertTrue(mocked['get_canonical_bibcodes'].called)
            self.assertTrue(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertTrue(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertTrue(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertTrue(mocked['store_event'].called)
            self.assertTrue(mocked['webhook_dump_event'].called)
            self.assertTrue(mocked['webhook_emit_event'].called)

    def test_process_updated_citation_changes_doi_when_target_exists_citation_doesnt(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertFalse(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_deleted_citation_changes_doi_when_target_exists_citation_doesnt(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.deleted)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertFalse(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_new_citation_changes_doi_when_target_and_citation_exist(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=True), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertFalse(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_updated_citation_changes_doi_when_citation_doesnt_exist(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.updated)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertFalse(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_deleted_citation_changes_doi_when_citation_doesnt_exist(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.deleted)
        doi_id = "10.5281/zenodo.11020" # software
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value=self.mock_data[doi_id]), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=self.mock_data[doi_id]['raw']), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value=self.mock_data[doi_id]['parsed']), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertFalse(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertFalse(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called)

    def test_process_citation_changes_ascl(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017MNRAS.470.1687B'
        citation_change.cited = '...................'
        citation_change.content = 'ascl:1210.002'
        citation_change.content_type = adsmsg.CitationChangeContentType.pid
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=None), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value={}), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertTrue(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called) # because we don't know if an URL is software

    def test_process_citation_changes_url(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = 'https://github.com/ComputationalRadiationPhysics/graybat'
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=None), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value={}), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertTrue(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertTrue(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertTrue(mocked['store_event'].called)
            self.assertTrue(mocked['webhook_dump_event'].called)
            self.assertTrue(mocked['webhook_emit_event'].called) # because we don't know if an URL is software

    def test_process_citation_changes_malformed_url(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = 'malformedhttps://github.com/ComputationalRadiationPhysics/graybat'
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=None), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value={}), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertTrue(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertTrue(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertTrue(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertTrue(mocked['store_event'].called)
            self.assertTrue(mocked['webhook_dump_event'].called)
            self.assertTrue(mocked['webhook_emit_event'].called) # because we don't know if an URL is software

    def test_process_citation_changes_empty(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = ''
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=None), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value={}), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertFalse(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called) # Not executed because content is empty
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertFalse(mocked['store_citation_target'].called)
            self.assertFalse(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called) # because we don't know if an URL is software

    def test_process_new_citation_changes_doi_unparsable_http_response(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value="Unparsable response"), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value={}), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertTrue(mocked['fetch_metadata'].called)
            self.assertTrue(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertTrue(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called) # because we don't know if an URL is software

    def test_process_new_citation_changes_doi_http_error(self):
        citation_changes = self._common_citation_changes_doi(adsmsg.Status.new)
        with TestBase.mock_multiple_targets({
                'citation_already_exists': patch.object(db, 'citation_already_exists', return_value=False), \
                'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={}), \
                'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
                'store_citation_target': patch.object(db, 'store_citation_target', return_value=True), \
                'store_citation': patch.object(db, 'store_citation', return_value=True), \
                'store_event': patch.object(db, 'store_event', return_value=True), \
                'update_citation': patch.object(db, 'update_citation', return_value=True), \
                'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
                'get_citations': patch.object(db, 'get_citations', return_value=[]), \
                'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
                'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
                'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
                'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=[]), \
                'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
                'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
                'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value=citation_changes.changes[0].citing), \
                'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value=None), \
                'parse_metadata': patch.object(doi, 'parse_metadata', return_value={}), \
                'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_citation_changes(citation_changes)
            self.assertTrue(mocked['citation_already_exists'].called)
            self.assertTrue(mocked['get_citation_target_metadata'].called)
            self.assertTrue(mocked['fetch_metadata'].called)
            self.assertFalse(mocked['parse_metadata'].called)
            self.assertFalse(mocked['url_is_alive'].called)
            self.assertTrue(mocked['get_canonical_bibcode'].called)
            self.assertFalse(mocked['get_canonical_bibcodes'].called)
            self.assertFalse(mocked['get_citations_by_bibcode'].called)
            self.assertTrue(mocked['store_citation_target'].called)
            self.assertTrue(mocked['store_citation'].called)
            self.assertFalse(mocked['update_citation'].called)
            self.assertFalse(mocked['mark_citation_as_deleted'].called)
            self.assertFalse(mocked['get_citations'].called)
            self.assertFalse(mocked['forward_message'].called)
            self.assertFalse(mocked['update_citation_target_metadata'].called)
            self.assertFalse(mocked['get_citation_target_count'].called)
            self.assertFalse(mocked['get_citation_count'].called)
            self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
            self.assertFalse(mocked['get_citation_targets_by_doi'].called)
            self.assertFalse(mocked['get_citation_targets'].called)
            self.assertFalse(mocked['request_existing_citations'].called)
            self.assertFalse(mocked['build_bibcode'].called)
            self.assertFalse(mocked['is_url'].called)
            self.assertFalse(mocked['citation_change_to_event_data'].called)
            self.assertFalse(mocked['identical_bibcodes_event_data'].called)
            self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
            self.assertFalse(mocked['store_event'].called)
            self.assertFalse(mocked['webhook_dump_event'].called)
            self.assertFalse(mocked['webhook_emit_event'].called) # because we don't know if an URL is software

    def test_process_updated_associated_works(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '2017zndo....248351D'
        citation_change.content = '10.5281/zenodo.248351'
        citation_change.content_type = adsmsg.CitationChangeContentType.doi
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.updated
        bibcode_id = "2017zndo....248351D"
        associated_registered_record = self.mock_data[bibcode_id][0]
        associated_citation_change = adsmsg.CitationChange(content=associated_registered_record['content'],
                                                       content_type=getattr(adsmsg.CitationChangeContentType, associated_registered_record['content_type'].lower()),
                                                       status=adsmsg.Status.updated,
                                                       timestamp=datetime.now()
                                                       )
        associated_versions = {"v2.0.0": "2017zndo....248351D", "v3.3.4": "2021zndo...4475376C"}
        with TestBase.mock_multiple_targets({
            'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', return_value={'status': 'REGISTERED','parsed':{'bibcode': bibcode_id},'raw':{"test":True}}), \
            'fetch_metadata': patch.object(doi, 'fetch_metadata', return_value={"test":True}), \
            'parse_metadata': patch.object(doi, 'parse_metadata', return_value={'bibcode': bibcode_id}), \
            'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', return_value=[]), \
            'store_event': patch.object(db, 'store_event', return_value=True), \
            'update_citation': patch.object(db, 'update_citation', return_value=True), \
            'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', return_value=(True, 'REGISTERED')), \
            'get_citations': patch.object(db, 'get_citations', return_value=[]), \
            'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', return_value=True), \
            'get_citation_target_count': patch.object(db, 'get_citation_target_count', return_value=0), \
            'get_citation_count': patch.object(db, 'get_citation_count', return_value=0), \
            'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', return_value=self.mock_data["2017zndo....248351D"]), \
            'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', return_value=[]), \
            'get_citation_targets': patch.object(db, 'get_citation_targets', return_value=[]), \
            'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
            'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
            'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
            'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
            'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
            'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
            'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
            'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
            'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
            'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
            'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
            tasks.task_process_updated_associated_works(associated_citation_change,associated_versions)
        self.assertTrue(mocked['get_citation_target_metadata'].called)
        self.assertFalse(mocked['fetch_metadata'].called)
        self.assertFalse(mocked['parse_metadata'].called)
        self.assertFalse(mocked['url_is_alive'].called)
        self.assertTrue(mocked['get_canonical_bibcodes'].called)
        self.assertTrue(mocked['get_citations_by_bibcode'].called)
        self.assertFalse(mocked['mark_citation_as_deleted'].called)
        self.assertFalse(mocked['get_citations'].called)
        self.assertTrue(mocked['forward_message'].called)
        self.assertTrue(mocked['update_citation_target_metadata'].called)
        self.assertFalse(mocked['get_citation_target_count'].called)
        self.assertFalse(mocked['get_citation_count'].called)
        self.assertFalse(mocked['get_citation_targets_by_doi'].called)
        self.assertFalse(mocked['get_citation_targets'].called)
        self.assertFalse(mocked['request_existing_citations'].called)
        self.assertFalse(mocked['build_bibcode'].called)
        self.assertFalse(mocked['is_url'].called)
        self.assertFalse(mocked['citation_change_to_event_data'].called)
        self.assertFalse(mocked['identical_bibcodes_event_data'].called)
        self.assertFalse(mocked['identical_bibcode_and_doi_event_data'].called)
        self.assertFalse(mocked['store_event'].called)
        self.assertFalse(mocked['webhook_dump_event'].called)
        self.assertFalse(mocked['webhook_emit_event'].called)

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

    def test_task_output_results_if_bibcode_replaced(self):
        with patch('ADSCitationCapture.app.ADSCitationCaptureCelery.forward_message', return_value=None) as forward_message:
            citation_change = adsmsg.CitationChange(content_type=adsmsg.CitationChangeContentType.doi, status=adsmsg.Status.active)
            parsed_metadata = {
                    'bibcode': 'test123456789012345',
                    'authors': ['Test, Unit'],
                    'normalized_authors': ['Test, U']
                    }
            citations = []
            bibcode_replaced = {"new":'test123456789012345',"previous":'test123456789054321'}
            tasks.task_output_results(citation_change, parsed_metadata, citations, bibcode_replaced = bibcode_replaced)
            self.assertTrue(forward_message.called)
            self.assertEqual(forward_message.call_count, 4)
            
if __name__ == '__main__':
    unittest.main()
