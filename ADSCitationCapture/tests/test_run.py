import sys
import os
import json
import pytest
import unittest
from ADSCitationCapture import app, tasks, delta_computation, db
from ADSCitationCapture import webhook
from ADSCitationCapture import doi
from ADSCitationCapture import url
from ADSCitationCapture import db
from ADSCitationCapture import api
from .test_base import TestBase
from mock import patch
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine
import tempfile

class TestWorkers(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        sys.path.append(self.proj_home)
        from run import process
        self.process = process
        self.sqlalchemy_url = self.app._engine.url # Use testing database
        self.schema_prefix = "testing_citation_capture_"

    def tearDown(self):
        # Drop testing schemas
        existing_schema_names = Inspector.from_engine(self.app._engine).get_schema_names()
        existing_schema_names = [x for x in existing_schema_names if x.startswith(self.schema_prefix)]
        for schema_name in existing_schema_names:
            self.app._engine.execute("DROP SCHEMA {0} CASCADE;".format(schema_name))
        TestBase.tearDown(self)

    def _fetch_metadata(self, base_doi_url, base_datacite_url, doi_url):
        data = {
            '10.5281/zenodo.11020': '<?xml version="1.0" encoding="utf-8"?>\n<resource xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/ZENODO.11020</identifier>\n  <creators>\n    <creator>\n      <creatorName>Dan Foreman-Mackey</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Adrian Price-Whelan</creatorName>\n      <affiliation>Columbia University</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Geoffrey Ryan</creatorName>\n      <affiliation>NYU</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Emily</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Michael Smith</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Kyle Barbary</creatorName>\n    </creator>\n    <creator>\n      <creatorName>David W. Hogg</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Brendon J. Brewer</creatorName>\n      <affiliation>The University of Auckland</affiliation>\n    </creator>\n  </creators>\n  <titles>\n    <title>Triangle.Py V0.1.1</title>\n  </titles>\n  <publisher>Zenodo</publisher>\n  <publicationYear>2014</publicationYear>\n  <dates>\n    <date dateType="Issued">2014-07-24</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="url">https://zenodo.org/record/11020</alternateIdentifier>\n  </alternateIdentifiers>\n  <relatedIdentifiers>\n    <relatedIdentifier relatedIdentifierType="URL" relationType="IsSupplementTo">https://github.com/dfm/triangle.py/tree/v0.1.1</relatedIdentifier>\n  </relatedIdentifiers>\n  <rightsList>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;p&gt;This is a citable release with a better name.&lt;/p&gt;</description>\n  </descriptions>\n</resource>',
            '10.5281/zenodo.1049160': '<?xml version="1.0" encoding="utf-8"?>\n<resource xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/ZENODO.1049160</identifier>\n  <creators>\n    <creator>\n      <creatorName>Eastwood, Michael W.</creatorName>\n      <givenName>Michael W.</givenName>\n      <familyName>Eastwood</familyName>\n      <nameIdentifier nameIdentifierScheme="ORCID" schemeURI="http://orcid.org/">0000-0002-4731-6083</nameIdentifier>\n      <affiliation>Department of Astronomy, California Institute of Technology</affiliation>\n    </creator>\n  </creators>\n  <titles>\n    <title>Ttcal</title>\n  </titles>\n  <publisher>Zenodo</publisher>\n  <publicationYear>2016</publicationYear>\n  <dates>\n    <date dateType="Issued">2016-10-27</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="url">https://zenodo.org/record/1049160</alternateIdentifier>\n  </alternateIdentifiers>\n  <relatedIdentifiers>\n    <relatedIdentifier relatedIdentifierType="URL" relationType="IsSupplementTo">https://github.com/mweastwood/TTCal.jl/tree/v0.3.0</relatedIdentifier>\n    <relatedIdentifier relatedIdentifierType="DOI" relationType="IsVersionOf">10.5281/zenodo.1049159</relatedIdentifier>\n  </relatedIdentifiers>\n  <version>0.3.0</version>\n  <rightsList>\n    <rights rightsURI="http://www.opensource.org/licenses/GPL-3.0">GNU General Public License 3.0</rights>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;p&gt;TTCal is a calibration routine developed for the OVRO-LWA.&lt;/p&gt;\n\n&lt;p&gt;The standard procedure for phase calibrating a radio interferometer usually involves slewing a small number of large dishes to stare at a known point source. A point source at the phase center of the interferometer has zero phase on all baselines, so phase calibration essentially amounts to zeroing the phase on all baselines.&lt;/p&gt;\n\n&lt;p&gt;Low frequency telescopes (&amp;lt;300 MHz) tend to occupy an entirely different region of phase space. That is they are usually composed of numerous cheap dipole antennas with very broad beams (LOFAR, MWA). Furthermore, the low frequency sky is corrupted by propagation through the ionosphere. Until the field matures, the demand for a new and effective calibration technique is best met by a simple, adaptable, and relatively fast software package. This is why I wrote TTCal.&lt;/p&gt;</description>\n  </descriptions>\n</resource>',
            '10.5281/zenodo.11813': '<?xml version="1.0" encoding="utf-8"?>\n<resource xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/ZENODO.11813</identifier>\n  <creators>\n    <creator>\n      <creatorName>Newville, Matthew</creatorName>\n      <givenName>Matthew</givenName>\n      <familyName>Newville</familyName>\n      <affiliation>The University of Chicago</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Stensitzki, Till</creatorName>\n      <givenName>Till</givenName>\n      <familyName>Stensitzki</familyName>\n      <affiliation>Till Stensitzki, Freie Universitat Berlin</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Allen, Daniel B.</creatorName>\n      <givenName>Daniel B.</givenName>\n      <familyName>Allen</familyName>\n      <affiliation>Johns Hopkins University</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Ingargiola,  Antonino</creatorName>\n      <givenName>Antonino</givenName>\n      <familyName>Ingargiola</familyName>\n      <affiliation>University of California, Los Angeles</affiliation>\n    </creator>\n  </creators>\n  <titles>\n    <title>LMFIT: Non-Linear Least-Square Minimization and Curve-Fitting for Python</title>\n  </titles>\n  <publisher>Zenodo</publisher>\n  <publicationYear>2014</publicationYear>\n  <subjects>\n    <subject>python</subject>\n    <subject>non-linear least-squares optimization</subject>\n    <subject>curve-fitting</subject>\n  </subjects>\n  <dates>\n    <date dateType="Issued">2014-09-21</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="url">https://zenodo.org/record/11813</alternateIdentifier>\n  </alternateIdentifiers>\n  <relatedIdentifiers>\n    <relatedIdentifier relatedIdentifierType="DOI" relationType="IsVersionOf">10.5281/zenodo.598352</relatedIdentifier>\n    <relatedIdentifier relatedIdentifierType="URL" relationType="IsPartOf">https://zenodo.org/communities/zenodo</relatedIdentifier>\n  </relatedIdentifiers>\n  <version>0.8.0</version>\n  <rightsList>\n    <rights rightsURI="http://www.opensource.org/licenses/MIT">MIT License</rights>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;p&gt;Lmfit provides a high-level interface to non-linear optimization and curve fitting problems for Python. Lmfit builds on Levenberg-Marquardt algorithm of scipy.optimize.leastsq(), but also supports most of the optimization method from scipy.optimize.&amp;nbsp;&amp;nbsp; It has a number of useful enhancements, including:&lt;/p&gt;\n\n&lt;ul&gt;\n\t&lt;li&gt;&amp;nbsp;Using Parameter objects instead of plain floats as variables.'
            + '&amp;nbsp; A Parameter has a value that can be varied in the fit, fixed, have upper and/or lower bounds.&amp;nbsp; It can even have a value that is constrained by an algebraic expression of other Parameter values.&lt;/li&gt;\n\t&lt;li&gt;Ease of changing fitting algorithms.&amp;nbsp; Once a fitting model is set up, one can change the fitting algorithm without changing the objective function.&lt;/li&gt;\n\t&lt;li&gt;Improved estimation of confidence intervals.&amp;nbsp; While scipy.optimize.leastsq() will automatically calculate uncertainties and correlations from the covariance matrix, lmfit also has functions to explicitly explore parameter space to determine confidence levels even for the most difficult cases.&lt;/li&gt;\n\t&lt;li&gt;Improved curve-fitting with the Model class.&amp;nbsp; This which extends the capabilities of scipy.optimize.curve_fit(), allowing you to turn a function&amp;nbsp;that models for your data into a python class that helps you parametrize and fit data with that model.&lt;/li&gt;\n\t&lt;li&gt;&amp;nbsp;Many pre-built models for common lineshapes are included and ready to use.&lt;/li&gt;\n&lt;/ul&gt;\n\n&lt;p&gt;The lmfit package is Free software, using an MIT license&lt;/p&gt;</description>\n  </descriptions>\n</resource>',
            '10.5281/zenodo.27878': '<?xml version="1.0" encoding="utf-8"?>\n<resource xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd">\n  <identifier identifierType="DOI">10.5281/ZENODO.27878</identifier>\n  <creators>\n    <creator>\n      <creatorName>Sander Dieleman</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Jan Schl\xc3\xbcter</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Colin Raffel</creatorName>\n      <affiliation>LabROSA, Columbia University</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Eben Olson</creatorName>\n      <affiliation>Yale University</affiliation>\n    </creator>\n    <creator>\n      <creatorName>S\xc3\xb8ren Kaae S\xc3\xb8nderby</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Daniel Nouri</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Daniel Maturana</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Martin Thoma</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Eric Battenberg</creatorName>\n      <affiliation>Baidu Research</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Jack Kelly</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Jeffrey De Fauw</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Michael Heilman</creatorName>\n    </creator>\n    <creator>\n      <creatorName>diogo149</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Brian McFee</creatorName>\n      <affiliation>Center for Data Science - NYU</affiliation>\n    </creator>\n    <creator>\n      <creatorName>Hendrik Weideman</creatorName>\n      <affiliation>Rensselaer Polytechnic Institute</affiliation>\n    </creator>\n    <creator>\n      <creatorName>takacsg84</creatorName>\n    </creator>\n    <creator>\n      <creatorName>peterderivaz</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Jon</creatorName>\n    </creator>\n    <creator>\n      <creatorName>instagibbs</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Dr. Kashif Rasul</creatorName>\n      <affiliation>SpacialDB UG</affiliation>\n    </creator>\n    <creator>\n      <creatorName>CongLiu</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Britefury</creatorName>\n    </creator>\n    <creator>\n      <creatorName>Jonas Degrave</creatorName>\n    </creator>\n  </creators>\n  <titles>\n    <title>Lasagne: First Release.</title>\n  </titles>\n  <publisher>Zenodo</publisher>\n  <publicationYear>2015</publicationYear>\n  <dates>\n    <date dateType="Issued">2015-08-13</date>\n  </dates>\n  <resourceType resourceTypeGeneral="Software"/>\n  <alternateIdentifiers>\n    <alternateIdentifier alternateIdentifierType="url">https://zenodo.org/record/27878</alternateIdentifier>\n  </alternateIdentifiers>\n  '
            + '<relatedIdentifiers>\n    <relatedIdentifier relatedIdentifierType="URL" relationType="IsSupplementTo">https://github.com/Lasagne/Lasagne/tree/v0.1</relatedIdentifier>\n  </relatedIdentifiers>\n  <version>v0.1</version>\n  <rightsList>\n    <rights rightsURI="info:eu-repo/semantics/openAccess">Open Access</rights>\n  </rightsList>\n  <descriptions>\n    <description descriptionType="Abstract">&lt;ul&gt;\n&lt;li&gt;\n&lt;p&gt;core contributors, in alphabetical order:&lt;/p&gt;\n\n&lt;ul&gt;\n&lt;li&gt;Eric Battenberg (@ebattenberg)&lt;/li&gt;\n&lt;li&gt;Sander Dieleman (@benanne)&lt;/li&gt;\n&lt;li&gt;Daniel Nouri (@dnouri)&lt;/li&gt;\n&lt;li&gt;Eben Olson (@ebenolson)&lt;/li&gt;\n&lt;li&gt;A\xc3\xa4ron van den Oord (@avdnoord)&lt;/li&gt;\n&lt;li&gt;Colin Raffel (@craffel)&lt;/li&gt;\n&lt;li&gt;Jan Schl\xc3\xbcter (@f0k)&lt;/li&gt;\n&lt;li&gt;S\xc3\xb8ren Kaae S\xc3\xb8nderby (@skaae)&lt;/li&gt;\n&lt;/ul&gt;\n&lt;/li&gt;\n&lt;li&gt;\n&lt;p&gt;extra contributors, in chronological order:&lt;/p&gt;\n\n&lt;ul&gt;\n&lt;li&gt;Daniel Maturana (@dimatura): documentation, cuDNN layers, LRN&lt;/li&gt;\n&lt;li&gt;Jonas Degrave (@317070): get_all_param_values() fix&lt;/li&gt;\n&lt;li&gt;Jack Kelly (@JackKelly): help with recurrent layers&lt;/li&gt;\n&lt;li&gt;G\xc3\xa1bor Tak\xc3\xa1cs (@takacsg84): support broadcastable parameters in lasagne.updates&lt;/li&gt;\n&lt;li&gt;Diogo Moitinho de Almeida (@diogo149): MNIST example fixes&lt;/li&gt;\n&lt;li&gt;Brian McFee (@bmcfee): MaxPool2DLayer fix&lt;/li&gt;\n&lt;li&gt;Martin Thoma (@MartinThoma): documentation&lt;/li&gt;\n&lt;li&gt;Jeffrey De Fauw (@JeffreyDF): documentation, ADAM fix&lt;/li&gt;\n&lt;li&gt;Michael Heilman (@mheilman): NonlinearityLayer, lasagne.random&lt;/li&gt;\n&lt;li&gt;Gregory Sanders (@instagibbs): documentation fix&lt;/li&gt;\n&lt;li&gt;Jon Crall (@erotemic): check for non-positive input shapes&lt;/li&gt;\n&lt;li&gt;Hendrik Weideman (@hjweide): set_all_param_values() test, MaxPool2DCCLayer fix&lt;/li&gt;\n&lt;li&gt;Kashif Rasul (@kashif): ADAM simplification&lt;/li&gt;\n&lt;li&gt;Peter de Rivaz (@peterderivaz): documentation fix&lt;/li&gt;\n&lt;/ul&gt;\n&lt;/li&gt;\n&lt;/ul&gt;</description>\n  </descriptions>\n</resource>',
        }
        return data[doi_url]

    def _get_canonical_bibcode(bibcode):
        return bibcode


    def test_run(self):
        # This test modifies the public schema of the database, hence do not run it
        # if we detect that data exists to avoid affecting production by mistake
        citation_count = db.get_citation_count(self.app)
        citation_target_count = db.get_citation_target_count(self.app)
        if citation_count != 0 or citation_target_count != 0:
            pytest.skip("Skipped because this test assumes an empty public schema but the database already contains {} citations and {} citations targets (this is a protection against modifying an already used database)".format(citation_count, citation_target_count))
        else:
            first_refids_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample-refids1.dat")
            os.utime(first_refids_filename, (0, 0)) # set the access and modified times to 19700101_000000
            expected_citation_change_from_first_file = [
                    '\n\x132009arXiv0911.4940W\x12\x13...................\x18\x02"/http://github.com/b45ch1/hpsc_hanoi_2009_walter0\x02:\x00',
                    '\n\x132010arXiv1003.5943M\x12\x13...................\x18\x02" http://github.com/matsen/pplacer0\x02:\x00',
                    '\n\x132011arXiv1112.0312C\x12\x132012ascl.soft03003C\x18\x01"\rascl:1203.003(\x010\x02:\x00',
                    '\n\x132013arXiv1310.5912S\x12\x132012ascl.soft.8004S\x18\x01"\x0eascl:1208.80040\x02:\x00',
                    '\n\x132015ApJ...815L..10L\x12\x132015ascl.soft...10J\x18\x01"\rascl:1510.0100\x02:\x00',
                    '\n\x132015MNRAS.453..483K\x12\x13...................\x18\x01"\rascl:1208.0040\x02:\x00',
                    '\n\x132016AJ....152..123G\x12\x13...................\x18\x01"\x0eascl:1208.00420\x02:\x00',
                    '\n\x132015arXiv151003579A\x12\x132014spi..book11020F"\x1410.5281/zenodo.110200\x02:\x00',
                    '\n\x132015JCAP...08..043A\x12\x132014zndo.soft11020F"\x1410.5281/zenodo.11020(\x010\x02:\x00',
                    '\n\x132019ApJ...877L..39C\x12\x13..................."\x1610.5281/zenodo.10491600\x02:\x00',
                    '\n\x132019arXiv190105505T\x12\x13..................."\x1410.5281/zenodo.118130\x02:\x00',
                    '\n\x132019arXiv190105855L\x12\x13..................."\x1410.5281/zenodo.118130\x02:\x00',
                ]
            second_refids_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample-refids2.dat")
            os.utime(second_refids_filename, (24*60*60, 24*60*60)) # set the access and modified times to 19700102_000000
            expected_citation_change_from_second_file = [
                    '\n\x132009arXiv0911.4940W\x12\x13...................\x18\x02"/http://github.com/b45ch1/hpsc_hanoi_2009_walter(\x010\x03:\x04\x08\x80£\x05',
                    #'\n\x132010arXiv1003.5943M\x12\x13...................\x18\x02" http://github.com/matsen/pplacer0\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132015JCAP...08..043A\x12\x132014zndo.soft11020F"\x1410.5281/zenodo.110200\x03:\x04\x08\x80\xa3\x05',
                    '\n\x132011arXiv1112.0312C\x12\x132012ascl.soft03003C\x18\x01"\rascl:1203.003(\x010\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132013arXiv1310.5912S\x12\x132012ascl.soft.8004S\x18\x01"\x0eascl:1208.80040\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132015ApJ...815L..10L\x12\x132015ascl.soft...10J\x18\x01"\rascl:1510.0100\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132015arXiv150902512A\x12\x132015vsr..conf27878D"\x1410.5281/zenodo.278780\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132015MNRAS.453..483K\x12\x13hola...............\x18\x01"\rascl:1208.004(\x010\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132016AJ....152..123G\x12\x13...................\x18\x01"\x0eascl:1208.00420\x02:\x04\x08\x80\xa3\x05',
                    '\n\x132019arXiv190105855L\x12\x13..................."\x1410.5281/zenodo.118130\x01:\x04\x08\x80\xa3\x05',
                ]


            # Process first file
            i = 0
            with TestBase.mock_multiple_targets({
                    'task_process_citation_changes': patch.object(tasks.task_process_citation_changes, 'delay', wraps=tasks.task_process_citation_changes.delay), \
                    'citation_already_exists': patch.object(db, 'citation_already_exists', wraps=db.citation_already_exists), \
                    'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', wraps=db.get_citation_target_metadata), \
                    'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', wraps=db.get_citations_by_bibcode), \
                    'store_citation_target': patch.object(db, 'store_citation_target', wraps=db.store_citation_target), \
                    'store_citation': patch.object(db, 'store_citation', wraps=db.store_citation), \
                    'store_event': patch.object(db, 'store_event', wraps=db.store_event), \
                    'update_citation': patch.object(db, 'update_citation', wraps=db.update_citation), \
                    'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', wraps=db.mark_citation_as_deleted), \
                    'get_citations': patch.object(db, 'get_citations', wraps=db.get_citations), \
                    'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', wraps=db.update_citation_target_metadata), \
                    'get_citation_target_count': patch.object(db, 'get_citation_target_count', wraps=db.get_citation_target_count), \
                    'get_citation_count': patch.object(db, 'get_citation_count', wraps=db.get_citation_count), \
                    'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', wraps=db.get_citation_targets_by_bibcode), \
                    'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', wraps=db.get_citation_targets_by_doi), \
                    'get_citation_targets': patch.object(db, 'get_citation_targets', wraps=db.get_citation_targets), \
                    'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value="2015MNRAS.453..483K"), \
                    'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                    'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                    'fetch_metadata': patch.object(doi, 'fetch_metadata', wraps=self._fetch_metadata), \
                    'parse_metadata': patch.object(doi, 'parse_metadata', wraps=doi.parse_metadata), \
                    'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                    'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                    'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                    'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                    'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                    'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                    'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                    'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                    'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
                self.process(first_refids_filename, sqlalchemy_url=self.sqlalchemy_url, schema_prefix=self.schema_prefix)
                self.assertTrue(mocked['citation_already_exists'].called)
                self.assertTrue(mocked['get_citation_target_metadata'].called)
                self.assertTrue(mocked['fetch_metadata'].called)
                self.assertTrue(mocked['parse_metadata'].called)
                self.assertTrue(mocked['url_is_alive'].called)
                self.assertTrue(mocked['get_canonical_bibcode'].called)
                self.assertTrue(mocked['get_canonical_bibcodes'].called)
                self.assertTrue(mocked['get_citations_by_bibcode'].called)
                self.assertTrue(mocked['store_citation_target'].called)
                self.assertTrue(mocked['store_citation'].called)
                self.assertFalse(mocked['update_citation'].called)
                self.assertFalse(mocked['mark_citation_as_deleted'].called)
                self.assertTrue(mocked['get_citations'].called)
                self.assertTrue(mocked['forward_message'].called)
                self.assertFalse(mocked['update_citation_target_metadata'].called)
                self.assertFalse(mocked['get_citation_target_count'].called)
                self.assertFalse(mocked['get_citation_count'].called)
                self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
                self.assertFalse(mocked['get_citation_targets_by_doi'].called)
                self.assertFalse(mocked['get_citation_targets'].called)
                self.assertFalse(mocked['request_existing_citations'].called)
                self.assertTrue(mocked['build_bibcode'].called)
                self.assertFalse(mocked['is_url'].called)
                self.assertTrue(mocked['citation_change_to_event_data'].called)
                self.assertTrue(mocked['identical_bibcodes_event_data'].called)
                self.assertTrue(mocked['identical_bibcode_and_doi_event_data'].called)
                self.assertTrue(mocked['store_event'].called)
                self.assertTrue(mocked['webhook_dump_event'].called)
                self.assertTrue(mocked['webhook_emit_event'].called)
                
                for args in mocked['task_process_citation_changes'].call_args_list:
                    citation_changes = args[0][0]
                    for citation_change in citation_changes.changes:
                        print(citation_change.SerializeToString())
                        self.assertEqual(citation_change.SerializeToString().decode('latin_1'), expected_citation_change_from_first_file[i])
                        i += 1

            # Process second file
            i = 0
            with TestBase.mock_multiple_targets({
                    'task_process_citation_changes': patch.object(tasks.task_process_citation_changes, 'delay', wraps=tasks.task_process_citation_changes.delay), \
                    'citation_already_exists': patch.object(db, 'citation_already_exists', wraps=db.citation_already_exists), \
                    'get_citation_target_metadata': patch.object(db, 'get_citation_target_metadata', wraps=db.get_citation_target_metadata), \
                    'get_citations_by_bibcode': patch.object(db, 'get_citations_by_bibcode', wraps=db.get_citations_by_bibcode), \
                    'store_citation_target': patch.object(db, 'store_citation_target', wraps=db.store_citation_target), \
                    'store_citation': patch.object(db, 'store_citation', wraps=db.store_citation), \
                    'store_event': patch.object(db, 'store_event', wraps=db.store_event), \
                    'update_citation': patch.object(db, 'update_citation', wraps=db.update_citation), \
                    'mark_citation_as_deleted': patch.object(db, 'mark_citation_as_deleted', wraps=db.mark_citation_as_deleted), \
                    'get_citations': patch.object(db, 'get_citations', wraps=db.get_citations), \
                    'update_citation_target_metadata': patch.object(db, 'update_citation_target_metadata', wraps=db.update_citation_target_metadata), \
                    'get_citation_target_count': patch.object(db, 'get_citation_target_count', wraps=db.get_citation_target_count), \
                    'get_citation_count': patch.object(db, 'get_citation_count', wraps=db.get_citation_count), \
                    'get_citation_targets_by_bibcode': patch.object(db, 'get_citation_targets_by_bibcode', wraps=db.get_citation_targets_by_bibcode), \
                    'get_citation_targets_by_doi': patch.object(db, 'get_citation_targets_by_doi', wraps=db.get_citation_targets_by_doi), \
                    'get_citation_targets': patch.object(db, 'get_citation_targets', wraps=db.get_citation_targets), \
                    'get_canonical_bibcode': patch.object(api, 'get_canonical_bibcode', return_value="2015MNRAS.453..483K"), \
                    'get_canonical_bibcodes': patch.object(api, 'get_canonical_bibcodes', return_value=[]), \
                    'request_existing_citations': patch.object(api, 'request_existing_citations', return_value=[]), \
                    'fetch_metadata': patch.object(doi, 'fetch_metadata', wraps=self._fetch_metadata), \
                    'parse_metadata': patch.object(doi, 'parse_metadata', wraps=doi.parse_metadata), \
                    'build_bibcode': patch.object(doi, 'build_bibcode', wraps=doi.build_bibcode), \
                    'url_is_alive': patch.object(url, 'is_alive', return_value=True), \
                    'is_url': patch.object(url, 'is_url', wraps=url.is_url), \
                    'citation_change_to_event_data': patch.object(webhook, 'citation_change_to_event_data', wraps=webhook.citation_change_to_event_data), \
                    'identical_bibcodes_event_data': patch.object(webhook, 'identical_bibcodes_event_data', wraps=webhook.identical_bibcodes_event_data), \
                    'identical_bibcode_and_doi_event_data': patch.object(webhook, 'identical_bibcode_and_doi_event_data', wraps=webhook.identical_bibcode_and_doi_event_data), \
                    'webhook_dump_event': patch.object(webhook, 'dump_event', return_value=True), \
                    'webhook_emit_event': patch.object(webhook, 'emit_event', return_value=True), \
                    'forward_message': patch.object(app.ADSCitationCaptureCelery, 'forward_message', return_value=True)}) as mocked:
                self.process(second_refids_filename, sqlalchemy_url=self.sqlalchemy_url, schema_prefix=self.schema_prefix)
                self.assertTrue(mocked['citation_already_exists'].called)
                self.assertTrue(mocked['get_citation_target_metadata'].called)
                self.assertTrue(mocked['fetch_metadata'].called)
                self.assertTrue(mocked['parse_metadata'].called)
                self.assertTrue(mocked['url_is_alive'].called)
                self.assertTrue(mocked['get_canonical_bibcode'].called)
                self.assertTrue(mocked['get_canonical_bibcodes'].called)
                self.assertTrue(mocked['get_citations_by_bibcode'].called)
                self.assertTrue(mocked['store_citation_target'].called)
                self.assertTrue(mocked['store_citation'].called)
                self.assertTrue(mocked['update_citation'].called)
                self.assertTrue(mocked['mark_citation_as_deleted'].called)
                self.assertTrue(mocked['get_citations'].called)
                self.assertTrue(mocked['forward_message'].called)
                self.assertFalse(mocked['update_citation_target_metadata'].called)
                self.assertFalse(mocked['get_citation_target_count'].called)
                self.assertFalse(mocked['get_citation_count'].called)
                self.assertFalse(mocked['get_citation_targets_by_bibcode'].called)
                self.assertFalse(mocked['get_citation_targets_by_doi'].called)
                self.assertFalse(mocked['get_citation_targets'].called)
                self.assertFalse(mocked['request_existing_citations'].called)
                self.assertTrue(mocked['build_bibcode'].called)
                self.assertFalse(mocked['is_url'].called)
                self.assertTrue(mocked['citation_change_to_event_data'].called)
                self.assertTrue(mocked['identical_bibcodes_event_data'].called)
                self.assertTrue(mocked['identical_bibcode_and_doi_event_data'].called)
                self.assertTrue(mocked['store_event'].called)
                self.assertTrue(mocked['webhook_dump_event'].called)
                self.assertTrue(mocked['webhook_emit_event'].called)

                for args in mocked['task_process_citation_changes'].call_args_list:
                    citation_changes = args[0][0]
                    for citation_change in citation_changes.changes:
                        #print(citation_change.SerializeToString())
                        self.assertEqual(citation_change.SerializeToString().decode('latin_1'), expected_citation_change_from_second_file[i])
                        i += 1

if __name__ == '__main__':
    unittest.main()
