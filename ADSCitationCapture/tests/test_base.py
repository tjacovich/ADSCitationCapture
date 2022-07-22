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

        for mock_name, mock_patch in mock_patches.items():
            _mock = mock_patch.start()
            mocks[mock_name] = _mock

        yield mocks

        for mock_name, mock_patch in mock_patches.items():
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
                    'bibcode': '2014zndo.....11020F',
                    'version': '',
                    'pubdate': '2014-07-24',
                    'title': 'triangle.py v0.1.1',
                    'described_by': [],
                    'abstract': '<p>This is a citable release with a better name.</p>',
                    'versions': [],
                    'doctype': 'software',
                    'forked_from': [],
                    'affiliations': ['', '', '', '', '', '', '', ''],
                    'citations': [],
                    'references': [],
                    'description_of': [],
                    'authors': ['Foreman-Mackey, Dan', 'Price-Whelan, Adrian', 'Ryan, Geoffrey', 'Emily', 'Smith, Michael', 'Barbary, Kyle', 'Hogg, David W.', 'Brewer, Brendon J.'],
                    'normalized_authors': ['Foreman-Mackey, D', 'Price-Whelan, A', 'Ryan, G', 'Emily', 'Smith, M', 'Barbary, K', 'Hogg, D W', 'Brewer, B J'],
                    'keywords': [],
                    'forks': [],
                    'properties': {
                        'DOI': '10.5281/zenodo.11020',
                        'OPEN': 1,
                        'ELECTR': 'http://zenodo.org/record/11020'
                    },
                    'version_of': [],
                    'source': 'ZENODO',
                    'link_alive': True
                },
                'status': 'REGISTERED',
        }
        self.mock_data["10.5281/zenodo.11021"] = {
                'raw': '<?xml version="1.0" encoding="UTF-8"?>\n<resource xmlns="http://datacite.org/schema/kernel-3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://datacite.org/schema/kernel-3 http://schema.datacite.org/meta/kernel-3/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/zenodo.11020</identifier>\n  <creators>\n    <creator>\n      <creatorName>Dan Foreman-Mackey</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Adrian Price-Whelan</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Geoffrey Ryan</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Emily</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Michael Smith</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Kyle Barbary</creatorName>\n    </creator>\n    <creator>\n      <creatorName>David W. Hogg</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Brendon J. Brewer</creatorName>\n    </creator>\n  </creators>\n  <titles>\n    <title>triangle.py v0.1.1</title>\n  </titles>\n  <publisher>ZENODO</publisher>\n  <publicationYear>2014</publicationYear>\n  <dates>\n    <date dateType="Issued">2014-07-24</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="URL">http://zenodo.org/record/11020</alternateIdentifier>\n  </alternateIdentifiers>\n  <relatedIdentifiers>\n    <relatedIdentifier relationType="IsSupplementTo" relatedIdentifierType="URL">https://github.com/dfm/triangle.py/tree/v0.1.1</relatedIdentifier>\n  </relatedIdentifiers>\n  <rightsList>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n    <rights rightsURI="">Other (Open)</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;p&gt;This is a citable release with a better name.&lt;/p&gt;</description>\n  </descriptions>\n</resource>',
                'parsed': {
                    'bibcode': '2014zndo.....11021F',
                    'version': '',
                    'pubdate': '2014-07-24',
                    'title': 'triangle.py v0.1.1',
                    'described_by': [],
                    'abstract': '<p>This is a citable release with a better name.</p>',
                    'versions': [],
                    'doctype': 'software',
                    'alternate_bibcode': ['2014zndo.....11021G'],
                    'forked_from': [],
                    'affiliations': ['', '', '', '', '', '', '', ''],
                    'citations': [],
                    'references': [],
                    'description_of': [],
                    'authors': ['Foreman-Mackey, Dan', 'Price-Whelan, Adrian', 'Ryan, Geoffrey', 'Emily', 'Smith, Michael', 'Barbary, Kyle', 'Hogg, David W.', 'Brewer, Brendon J.'],
                    'normalized_authors': ['Foreman-Mackey, D', 'Price-Whelan, A', 'Ryan, G', 'Emily', 'Smith, M', 'Barbary, K', 'Hogg, D W', 'Brewer, B J'],
                    'keywords': [],
                    'forks': [],
                    'properties': {
                        'DOI': '10.5281/zenodo.11020',
                        'OPEN': 1,
                        'ELECTR': 'http://zenodo.org/record/11020'
                    },
                    'version_of': [],
                    'source': 'ZENODO',
                    'link_alive': True
                },
                'status': 'REGISTERED',
        }
        self.mock_data["10.5281/zenodo.4475376"] = {
                'raw': '<?xml version="1.0" encoding="UTF-8"?><resource xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4/metadata.xsd">  <identifier identifierType="DOI">10.5281/ZENODO.4475376</identifier>  <creators>    <creator>      <creatorName nameType="Personal">Caswell, Thomas A</creatorName>      <givenName>Thomas A</givenName>      <familyName>Caswell</familyName>      <affiliation>Brookhaven National Lab</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Droettboom, Michael</creatorName>      <givenName>Michael</givenName>      <familyName>Droettboom</familyName>      <affiliation>Mozilla</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Lee, Antony</creatorName>      <givenName>Antony</givenName>      <familyName>Lee</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Andrade, Elliott Sales De</creatorName>      <givenName>Elliott Sales De</givenName>      <familyName>Andrade</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Hunter, John</creatorName>      <givenName>John</givenName>      <familyName>Hunter</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Firing, Eric</creatorName>      <givenName>Eric</givenName>      <familyName>Firing</familyName>      <affiliation>University of Hawaii</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Hoffmann, Tim</creatorName>      <givenName>Tim</givenName>      <familyName>Hoffmann</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Klymak, Jody</creatorName>      <givenName>Jody</givenName>      <familyName>Klymak</familyName>      <affiliation>University of Victoria</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Stansby, David</creatorName>      <givenName>David</givenName>      <familyName>Stansby</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Varoquaux, Nelle</creatorName>      <givenName>Nelle</givenName>      <familyName>Varoquaux</familyName>      <affiliation>TIMC-IMAG</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Nielsen, Jens Hedegaard</creatorName>      <givenName>Jens Hedegaard</givenName>      <familyName>Nielsen</familyName>      <affiliation>@qdev-dk</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Root, Benjamin</creatorName>      <givenName>Benjamin</givenName>      <familyName>Root</familyName>    </creator>    <creator>      <creatorName nameType="Personal">May, Ryan</creatorName>      <givenName>Ryan</givenName>      <familyName>May</familyName>      <affiliation>UCAR/@Unidata</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Elson, Phil</creatorName>      <givenName>Phil</givenName>      <familyName>Elson</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Seppänen, Jouni K.</creatorName>      <givenName>Jouni K.</givenName>      <familyName>Seppänen</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Dale, Darren</creatorName>      <givenName>Darren</givenName>      <familyName>Dale</familyName>      <affiliation>Cornell University</affiliation>    </creator>    <creator>      <creatorName>Jae-Joon Lee</creatorName>    </creator>    <creator>      <creatorName nameType="Personal">McDougall, Damon</creatorName>      <givenName>Damon</givenName>      <familyName>McDougall</familyName>      <affiliation>AMD</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Straw, Andrew</creatorName>      <givenName>Andrew</givenName>      <familyName>Straw</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Hobson, Paul</creatorName>      <givenName>Paul</givenName>      <familyName>Hobson</familyName>      <affiliation>@Geosyntec</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Gohlke, Christoph</creatorName>      <givenName>Christoph</givenName>      <familyName>Gohlke</familyName>      <affiliation>University of California, Irvine</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Yu, Tony S</creatorName>      <givenName>Tony S</givenName>      <familyName>Yu</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Ma, Eric</creatorName>      <givenName>Eric</givenName>      <familyName>Ma</familyName>      <affiliation>Novartis Institutes for Biomedical Research</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Vincent, Adrien F.</creatorName>      <givenName>Adrien F.</givenName>      <familyName>Vincent</familyName>      <affiliation>Bordeaux INP</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">, Hannah</creatorName>      <givenName>Hannah</givenName>    </creator>    <creator>      <creatorName nameType="Personal">Silvester, Steven</creatorName>      <givenName>Steven</givenName>      <familyName>Silvester</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Moad, Charlie</creatorName>      <givenName>Charlie</givenName>      <familyName>Moad</familyName>      <affiliation>@SalesLoft</affiliation>    </creator>    <creator>      <creatorName nameType="Personal">Kniazev, Nikita</creatorName>      <givenName>Nikita</givenName>      <familyName>Kniazev</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Ernest, Elan</creatorName>      <givenName>Elan</givenName>      <familyName>Ernest</familyName>    </creator>    <creator>      <creatorName nameType="Personal">Ivanov, Paul</creatorName>      <givenName>Paul</givenName>      <familyName>Ivanov</familyName>      <affiliation>@Bloomberg</affiliation>    </creator>  </creators>  <titles>    <title>matplotlib/matplotlib: REL: v3.3.4</title>  </titles>  <publisher>Zenodo</publisher>  <publicationYear>2021</publicationYear>  <resourceType resourceTypeGeneral="Software">SoftwareSourceCode</resourceType>  <dates>    <date dateType="Issued">2021-01-28</date>  </dates>  <relatedIdentifiers>    <relatedIdentifier relatedIdentifierType="URL" relationType="IsSupplementTo">https://github.com/matplotlib/matplotlib/tree/v3.3.4</relatedIdentifier>    <relatedIdentifier relatedIdentifierType="DOI" relationType="IsVersionOf">10.5281/zenodo.592536</relatedIdentifier>  </relatedIdentifiers>  <sizes/>  <formats/>  <version>v3.3.4</version>  <rightsList>    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>  </rightsList>  <descriptions>    <description descriptionType="Abstract">This is the fourth bugfix release of the 3.3.x series. This release contains several critical bug-fixes: Fix WebAgg initialization. Fix parsing &lt;code&gt;QT_API&lt;/code&gt; setting with mixed case. Fix build with link-time optimization disabled in environment. Fix test compatibility with Python 3.10. Fix test compatibility with NumPy 1.20. Fix test compatibility with pytest 6.2.</description>  </descriptions></resource>',
                'parsed': {
                    "forks": [],
                    "title": "matplotlib/matplotlib: REL: v3.3.4",
                    "source": "Zenodo",
                    "authors": [
                        "Caswell, Thomas A",
                        "Droettboom, Michael",
                        "Lee, Antony",
                        "Sales De Andrade, Elliott",
                        "Hunter, John",
                        "Firing, Eric",
                        "Hoffmann, Tim",
                        "Klymak, Jody",
                        "Stansby, David",
                        "Varoquaux, Nelle",
                        "Hedegaard Nielsen, Jens",
                        "Root, Benjamin",
                        "May, Ryan",
                        "Elson, Phil",
                        "Seppänen, Jouni K.",
                        "Dale, Darren",
                        "Lee, Jae-Joon",
                        "McDougall, Damon",
                        "Straw, Andrew",
                        "Hobson, Paul",
                        "Gohlke, Christoph",
                        "Yu, Tony S",
                        "Ma, Eric",
                        "Vincent, Adrien F.",
                        "Hannah",
                        "Silvester, Steven",
                        "Moad, Charlie",
                        "Kniazev, Nikita",
                        "Ernest, Elan",
                        "Ivanov, Paul"
                    ],
                    "bibcode": "2021zndo...4475376C",
                    "doctype": "software",
                    "pubdate": "2021-01-28",
                    "version": "v3.3.4",
                    "abstract": "This is the fourth bugfix release of the 3.3.x series. This release contains several critical bug-fixes: Fix WebAgg initialization. Fix parsing <code>QT_API</code> setting with mixed case. Fix build with link-time optimization disabled in environment. Fix test compatibility with Python 3.10. Fix test compatibility with NumPy 1.20. Fix test compatibility with pytest 6.2.",
                    "keywords": [],
                    "versions": [],
                    "citations": [],
                    "link_alive": True,
                    "properties": {
                        "DOI": "10.5281/ZENODO.4475376",
                        "OPEN": 1
                    },
                    "references": [],
                    "version_of": [
                        "10.5281/zenodo.592536"
                    ],
                    "forked_from": [],
                    "affiliations": [
                        "Brookhaven National Lab",
                        "Mozilla",
                        "",
                        "",
                        "",
                        "University of Hawaii",
                        "",
                        "University of Victoria",
                        "",
                        "TIMC-IMAG",
                        "@qdev-dk",
                        "",
                        "UCAR/@Unidata",
                        "",
                        "",
                        "Cornell University",
                        "",
                        "AMD",
                        "",
                        "@Geosyntec",
                        "University of California, Irvine",
                        "",
                        "Novartis Institutes for Biomedical Research",
                        "Bordeaux INP",
                        "",
                        "",
                        "@SalesLoft",
                        "",
                        "",
                        "@Bloomberg"
                    ],
                    "described_by": [],
                    "description_of": [],
                    "normalized_authors": [
                        "Caswell, T A",
                        "Droettboom, M",
                        "Lee, A",
                        "Sales De Andrade, E",
                        "Hunter, J",
                        "Firing, E",
                        "Hoffmann, T",
                        "Klymak, J",
                        "Stansby, D",
                        "Varoquaux, N",
                        "Hedegaard Nielsen, J",
                        "Root, B",
                        "May, R",
                        "Elson, P",
                        "Seppanen, J K",
                        "Dale, D",
                        "Lee, J -",
                        "McDougall, D",
                        "Straw, A",
                        "Hobson, P",
                        "Gohlke, C",
                        "Yu, T S",
                        "Ma, E",
                        "Vincent, A F",
                        "Hannah",
                        "Silvester, S",
                        "Moad, C",
                        "Kniazev, N",
                        "Ernest, E",
                        "Ivanov, P"
                    ]
                    },
                'status': 'REGISTERED',
                'versions': {'concept_doi': "10.5281/zenodo.592536", 
                    'versions': [
                    "10.5281/zenodo.11451",
                    "10.5281/zenodo.12287",
                    "10.5281/zenodo.12400",
                    "10.5281/zenodo.15423",
                    "10.5281/zenodo.30988",
                    "10.5281/zenodo.31764",
                    "10.5281/zenodo.32914",
                    "10.5281/zenodo.44579",
                    "10.5281/zenodo.53816",
                    "10.5281/zenodo.56926",
                    "10.5281/zenodo.57619",
                    "10.5281/zenodo.58058",
                    "10.5281/zenodo.61948",
                    "10.5281/zenodo.61952",
                    "10.5281/zenodo.192512",
                    "10.5281/zenodo.208224",
                    "10.5281/zenodo.248351",
                    "10.5281/zenodo.570311",
                    "10.5281/zenodo.573577",
                    "10.5281/zenodo.877339",
                    "10.5281/zenodo.1004650",
                    "10.5281/zenodo.1098480",
                    "10.5281/zenodo.1154287",
                    "10.5281/zenodo.1171187",
                    "10.5281/zenodo.1189358",
                    "10.5281/zenodo.1202050",
                    "10.5281/zenodo.1202077",
                    "10.5281/zenodo.1343133",
                    "10.5281/zenodo.1343964",
                    "10.5281/zenodo.1404439",
                    "10.5281/zenodo.1420605",
                    "10.5281/zenodo.1482098",
                    "10.5281/zenodo.1482099",
                    "10.5281/zenodo.2577644",
                    "10.5281/zenodo.2669103",
                    "10.5281/zenodo.2647603",
                    "10.5281/zenodo.2667837",
                    "10.5281/zenodo.2893252",
                    "10.5281/zenodo.3264781",
                    "10.5281/zenodo.3563226",
                    "10.5281/zenodo.3633833",
                    "10.5281/zenodo.3633844",
                    "10.5281/zenodo.3633877",
                    "10.5281/zenodo.3695547",
                    "10.5281/zenodo.3714460",
                    "10.5281/zenodo.3898017",
                    "10.5281/zenodo.3948793",
                    "10.5281/zenodo.3984190",
                    "10.5281/zenodo.4030140",
                    "10.5281/zenodo.4268928",
                    "10.5281/zenodo.4475376",
                    "10.5281/zenodo.4550144",
                    "10.5281/zenodo.4595919",
                    "10.5281/zenodo.4595937",
                    "10.5281/zenodo.4638398",
                    "10.5281/zenodo.4649959",
                    "10.5281/zenodo.4743323",
                    "10.5281/zenodo.5194481",
                    "10.5281/zenodo.5242609",
                    "10.5281/zenodo.5545068",
                    "10.5281/zenodo.5706396",
                    "10.5281/zenodo.5773480",
                    "10.5281/zenodo.6513224"
                ]},
                'associated': {"Version v2.0.0": "2017zndo....248351D"}
        }
        self.mock_data["2017zndo....248351D"] = [{     
                    'bibcode': "2017zndo....248351D",
                    'alternate_bibcode':'',
                    'content': "10.5281/zenodo.248351",
                    'content_type': "DOI",
                    'associated_works': "",
                    'version': "v2.0.0"    
        }]
