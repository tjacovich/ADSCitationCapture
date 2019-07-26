import unittest
import contextlib
import mock
from sqlalchemy import create_engine
from adsputils import load_config
from ADSCitationCapture import app, tasks
from ADSCitationCapture.models import Base





class TestBase(unittest.TestCase):

    @staticmethod
    @contextlib.contextmanager
    def mock_multiple_targets(mock_patches):
        """
        `mock_patches` is a list (or iterable) of mock.patch objects

        This is required when too many patches need to be applied in a nested
        `with` statement, since python has a hardcoded limit (~20).

        Based on: https://gist.github.com/msabramo/dffa53e4f29ec2e3682e
        """
        mocks = {}

        for mock_name, mock_patch in mock_patches.iteritems():
            _mock = mock_patch.start()
            mocks[mock_name] = _mock

        yield mocks

        for mock_name, mock_patch in mock_patches.iteritems():
            mock_patch.stop()


    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = tasks.app.conf['PROJ_HOME']
        self._app = tasks.app
        # Use a different database for unit tests since they will modify it
        self.sqlalchemy_url = "{}_test".format(load_config().get('SQLALCHEMY_URL', 'postgres://postgres@localhost:5432/citation_capture_pipeline'))
        config = {
            "TESTING_MODE": False,
            "CELERY_ALWAYS_EAGER": False,
            "CELERY_EAGER_PROPAGATES_EXCEPTIONS": False,
            "SQLALCHEMY_URL": self.sqlalchemy_url,
        }
        self.app = app.ADSCitationCaptureCelery('test', proj_home=self.proj_home, local_config=config)
        tasks.app = self.app # monkey-patch the app object
        self._init_mock_data()
        try:
            Base.metadata.create_all(bind=self.app._engine, checkfirst=True)
        except:
            # Database not empty!
            raise


    def tearDown(self):
        unittest.TestCase.tearDown(self)
        # A CASCADE drop is required because sometimes drop_all tries to delete
        # ENUM before tables that depend on it and it raises and exception:
        for table_name in Base.metadata.tables.keys():
            self.app._engine.execute("DROP TABLE IF EXISTS {0} CASCADE;".format(table_name))
        # Make sure nothing else is left behind
        Base.metadata.drop_all(bind=self.app._engine)
        # Reset schemas to public in case any of them was changed in table's metadata
        # - This happens in delta_computation.py and affects subsequent tests
        import sys, inspect
        import sqlalchemy
        import ADSCitationCapture.models
        module_name = "ADSCitationCapture.models"
        for member_name, obj in inspect.getmembers(sys.modules[module_name]):
            if type(obj) is sqlalchemy.ext.declarative.api.DeclarativeMeta and hasattr(obj, '__table__'):
                obj.__table__.schema = "public"
        # Dispose of the connection pool used by this engine (closing all open connections)
        self.app._engine.dispose()
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
