import sys
import os
import json
import pytest

import unittest
from ADSCitationCapture import app, tasks, delta_computation, db
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
        self.schema_prefix = "testing_citation_capture_"

    def tearDown(self):
        TestBase.tearDown(self)
        # Drop testing schemas
        engine = create_engine(self.app.conf['SQLALCHEMY_URL'], echo=False)
        connection = engine.connect()
        existing_schema_names = Inspector.from_engine(engine).get_schema_names()
        existing_schema_names = filter(lambda x: x.startswith(self.schema_prefix), existing_schema_names)
        for schema_name in existing_schema_names:
            connection.execute("drop schema {0} cascade;".format(schema_name))


    @unittest.skip("TODO: Broken since the pipeline reconstruct the previous expanded table")
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
                    '\n\x131005PhRvC..71c4906H\x12\x131990PhLB..243..432G"\x1c10.1016/0370-2693(90)91409-5(\x010\x02:\x00',
                    '\n\x131005PhRvC..71c4906H\x12\x131976NuPhB.113..395J"\x1c10.1016/0550-3213(76)90133-4(\x010\x02:\x00',
                    '\n\x131799AnP.....2..154P\x12\x13..................."\x1910.1088/0022-3727/2/1/4220\x02:\x00',
                    '\n\x131910GeoRu...1....1D\x12\x13..................."\x1910.2475/ajs.s2-46.137.2400\x02:\x00',
                    '\n\x131910GeoRu...1...36M\x12\x13..................."\x1610.2475/ajs.s4-6.31.750\x02:\x00',
                    '\n\x132009arXiv0911.4940W\x12\x13...................\x18\x02"/http://github.com/b45ch1/hpsc_hanoi_2009_walter0\x02:\x00',
                    '\n\x132010arXiv1003.5943M\x12\x13...................\x18\x02" http://github.com/matsen/pplacer0\x02:\x00',
                    '\n\x132011arXiv1112.0312C\x12\x132012ascl.soft03003C\x18\x01"\rascl:1203.003(\x010\x02:\x00',
                    '\n\x132013arXiv1310.5912S\x12\x13...................\x18\x01"\x0eascl:1208.80040\x02:\x00',
                    '\n\x132015ApJ...815L..10L\x12\x13...................\x18\x01"\rascl:1510.0100\x02:\x00',
                    '\n\x132015MNRAS.453..483K\x12\x13...................\x18\x01"\rascl:1208.0040\x02:\x00',
                    '\n\x132016AJ....152..123G\x12\x13...................\x18\x01"\x0eascl:1208.00420\x02:\x00'
                ]
            second_refids_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample-refids2.dat")
            os.utime(second_refids_filename, (24*60*60, 24*60*60)) # set the access and modified times to 19700102_000000
            expected_citation_change_from_second_file = [
                    '\n\x132015MNRAS.453..483K\x12\x13hola...............\x18\x01"\rascl:1208.004(\x010\x03:\x04\x08\x80\xa3\x05',
                    '\n\x131800AnP.....3..113.\x12\x13..................."\x1910.1107/S00218898700059400\x02:\x04\x08\x80\xa3\x05',
                    '\n\x131005PhRvC..71c4906H\x12\x131990PhLB..243..432G"\x1c10.1016/0370-2693(90)91409-5(\x010\x01:\x04\x08\x80\xa3\x05'
                    '\n\x131005PhRvC..71c4906H\x12\x131976NuPhB.113..395J"\x1c10.1016/0550-3213(76)90133-4(\x010\x02:\x04\x08\x80\xa3\x05'
                ]

            # Process first file
            i = 0
            with patch.object(tasks.task_process_citation_changes, 'delay', return_value=None) as task_process_citation_changes, \
                    patch.object(delta_computation.DeltaComputation, '_find_not_processed_records_from_previous_run', return_value=[]) as find_not_processed_records_from_previous_run:
                self.process(first_refids_filename, schema_prefix=self.schema_prefix)
                self.assertTrue(task_process_citation_changes.called)
                self.assertFalse(find_not_processed_records_from_previous_run.called)

                for args in task_process_citation_changes.call_args_list:
                    citation_changes = args[0][0]
                    for citation_change in citation_changes.changes:
                        #print citation_change.SerializeToString()
                        self.assertEqual(citation_change.SerializeToString(), expected_citation_change_from_first_file[i])
                        i += 1

            # Process second file
            i = 0
            with patch.object(tasks.task_process_citation_changes, 'delay', return_value=None) as task_process_citation_changes, \
                    patch.object(delta_computation.DeltaComputation, '_find_not_processed_records_from_previous_run', return_value=[]) as find_not_processed_records_from_previous_run:
                self.process(second_refids_filename, schema_prefix=self.schema_prefix)
                self.assertTrue(task_process_citation_changes.called)
                self.assertTrue(find_not_processed_records_from_previous_run.called)

                for args in task_process_citation_changes.call_args_list:
                    citation_changes = args[0][0]
                    for citation_change in citation_changes.changes:
                        #print citation_change.SerializeToString()
                        self.assertEqual(citation_change.SerializeToString(), expected_citation_change_from_second_file[i])
                        i += 1

if __name__ == '__main__':
    unittest.main()
