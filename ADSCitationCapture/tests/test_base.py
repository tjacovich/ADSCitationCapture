import unittest
from ADSCitationCapture import app, tasks


class TestBase(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = tasks.app.conf['PROJ_HOME']
        self._app = tasks.app
        config = {
            "TESTING_MODE": False,
            "CELERY_ALWAYS_EAGER": False,
            "CELERY_EAGER_PROPAGATES_EXCEPTIONS": False,
        }
        self.app = app.ADSCitationCaptureCelery('test', proj_home=self.proj_home, local_config=config)
        tasks.app = self.app # monkey-patch the app object
        self._init_mock_data()


    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.app.close_app()
        tasks.app = self._app

    def _init_mock_data(self):
        self.mock_data = {}
        self.mock_data["10.5281/zenodo.11020"] = {
                'raw': '<?xml version="1.0" encoding="UTF-8"?>\n<resource xmlns="http://datacite.org/schema/kernel-3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://datacite.org/schema/kernel-3 http://schema.datacite.org/meta/kernel-3/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/zenodo.11020</identifier>\n  <creators>\n    <creator>\n      <creatorName>Dan Foreman-Mackey</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Adrian Price-Whelan</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Geoffrey Ryan</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Emily</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Michael Smith</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Kyle Barbary</creatorName>\n    </creator>\n    <creator>\n      <creatorName>David W. Hogg</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Brendon J. Brewer</creatorName>\n    </creator>\n  </creators>\n  <titles>\n    <title>triangle.py v0.1.1</title>\n  </titles>\n  <publisher>ZENODO</publisher>\n  <publicationYear>2014</publicationYear>\n  <dates>\n    <date dateType="Issued">2014-07-24</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="URL">http://zenodo.org/record/11020</alternateIdentifier>\n  </alternateIdentifiers>\n  <relatedIdentifiers>\n    <relatedIdentifier relationType="IsSupplementTo" relatedIdentifierType="URL">https://github.com/dfm/triangle.py/tree/v0.1.1</relatedIdentifier>\n  </relatedIdentifiers>\n  <rightsList>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n    <rights rightsURI="">Other (Open)</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;p&gt;This is a citable release with a better name.&lt;/p&gt;</description>\n  </descriptions>\n</resource>',
                'parsed': {
                    'bibcode': u'2014zndo.....11020F',
                    'version': '',
                    'pubdate': u'2014-07-24',
                    'title': u'triangle.py v0.1.1',
                    'described_by': [],
                    'abstract': u'<p>This is a citable release with a better name.</p>',
                    'versions': [],
                    'doctype': 'software',
                    'forked_from': [],
                    'affiliations': ['', '', '', '', '', '', '', ''],
                    'citations': [],
                    'references': [],
                    'description_of': [],
                    'authors': [u'Foreman-Mackey, Dan', u'Price-Whelan, Adrian', u'Ryan, Geoffrey', u'Emily', u'Smith, Michael', u'Barbary, Kyle', u'Hogg, David W.', u'Brewer, Brendon J.'],
                    'normalized_authors': [u'Foreman-Mackey, D', u'Price-Whelan, A', u'Ryan, G', u'Emily', u'Smith, M', u'Barbary, K', u'Hogg, D W', u'Brewer, B J'],
                    'keywords': [],
                    'forks': [],
                    'properties': {
                        'DOI': u'10.5281/zenodo.11020',
                        'OPEN': 1,
                        'ELECTR': u'http://zenodo.org/record/11020'
                    },
                    'version_of': [],
                    'source': u'ZENODO',
                    'link_alive': True
                },
                'status': 'REGISTERED'
        }
