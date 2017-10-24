import sys
import os
import json
import adsmsg
from adscc import webhook
from adscc import doi
from adscc import url

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

    def test_process_citation_changes_doi(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2005CaJES..42.1987P'
        citation_change.cited = '...................'
        citation_change.content = '10.1016/0277-3791'
        citation_change.content_type = adsmsg.CitationChangeContentType.doi
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new

        with patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
            with patch.object(doi, 'is_software', return_value=True) as doi_is_software:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(doi_is_software.called)
                self.assertTrue(webhook_emit_event.called)

    def test_process_citation_changes_ascl(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017MNRAS.470.1687B'
        citation_change.cited = '...................'
        citation_change.content = 'ascl:1210.002'
        citation_change.content_type = adsmsg.CitationChangeContentType.pid
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new

        with patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
            with patch.object(url, 'is_alive', return_value=True) as url_is_alive:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(url_is_alive.called)
                self.assertTrue(webhook_emit_event.called)

    def test_process_citation_changes_url(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = 'https://github.com/ComputationalRadiationPhysics/graybat'
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new

        with patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
            with patch.object(url, 'is_alive', return_value=True) as url_is_alive:
                tasks.task_process_citation_changes(citation_changes)
                self.assertTrue(url_is_alive.called)
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

        with patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
            tasks.task_process_citation_changes(citation_changes)
            self.assertFalse(webhook_emit_event.called) # because URL does not match an URL pattern

    def test_process_citation_changes_empty(self):
        citation_changes = adsmsg.CitationChanges()
        citation_change = citation_changes.changes.add()
        citation_change.citing = '2017arXiv170610086M'
        citation_change.cited = '...................'
        citation_change.content = ''
        citation_change.content_type = adsmsg.CitationChangeContentType.url
        citation_change.resolved = False
        citation_change.status = adsmsg.Status.new

        with patch.object(webhook, 'emit_event', return_value=True) as webhook_emit_event:
            with patch.object(url, 'is_alive', return_value=True) as url_is_alive:
                tasks.task_process_citation_changes(citation_changes)
                self.assertFalse(url_is_alive.called)
                self.assertFalse(webhook_emit_event.called)


    def test_task_output_results(self):
        with patch('adscc.app.ADSCitationCaptureCelery.forward_message', return_value=None) as forward_message:
            msg = { }
            tasks.task_output_results(msg)
            self.assertTrue(forward_message.called)

if __name__ == '__main__':
    unittest.main()
