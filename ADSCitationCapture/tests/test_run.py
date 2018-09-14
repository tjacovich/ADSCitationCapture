import sys
import os
import json

import unittest
from ADSCitationCapture import app, tasks
from mock import patch
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine
import tempfile


class TestWorkers(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = tasks.app.conf['PROJ_HOME']
        sys.path.append(self.proj_home)
        from run import run
        self.run = run
        self._app = tasks.app
        self.app = app.ADSCitationCaptureCelery('test', proj_home=self.proj_home, local_config={})
        tasks.app = self.app # monkey-patch the app object
        self.schema_prefix = "testing_citation_capture_"

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.app.close_app()
        tasks.app = self._app
        # Drop testing schemas
        engine = create_engine(self.app.conf['SQLALCHEMY_URL'], echo=False)
        connection = engine.connect()
        existing_schema_names = Inspector.from_engine(engine).get_schema_names()
        existing_schema_names = filter(lambda x: x.startswith(self.schema_prefix), existing_schema_names)
        for schema_name in existing_schema_names:
            connection.execute("drop schema {0} cascade;".format(schema_name))


    def test_run(self):
        first_refids_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample-refids1.dat")
        os.utime(first_refids_filename, (0, 0)) # set the access and modified times to 19700101_000000
        expected_citation_change_from_first_file = [
                '\n\x131005PhRvC..71c4906H\x12\x131990PhLB..243..432G"\x1c10.1016/0370-2693(90)91409-5(\x010\x02',
                '\n\x131005PhRvC..71c4906H\x12\x131976NuPhB.113..395J"\x1c10.1016/0550-3213(76)90133-4(\x010\x02',
                '\n\x131799AnP.....2..154P\x12\x13..................."\x1910.1088/0022-3727/2/1/4220\x02',
                '\n\x131910GeoRu...1....1D\x12\x13..................."\x1910.2475/ajs.s2-46.137.2400\x02',
                '\n\x131910GeoRu...1...36M\x12\x13..................."\x1610.2475/ajs.s4-6.31.750\x02',
                '\n\x132009arXiv0911.4940W\x12\x13...................\x18\x02"/http://github.com/b45ch1/hpsc_hanoi_2009_walter0\x02',
                '\n\x132010arXiv1003.5943M\x12\x13...................\x18\x02" http://github.com/matsen/pplacer0\x02',
                '\n\x132011arXiv1112.0312C\x12\x132012ascl.soft03003C\x18\x01"\rascl:1203.003(\x010\x02',
                '\n\x132013arXiv1310.5912S\x12\x13...................\x18\x01"\x0eascl:1208.80040\x02',
                '\n\x132015ApJ...815L..10L\x12\x13...................\x18\x01"\rascl:1510.0100\x02',
                '\n\x132015MNRAS.453..483K\x12\x13...................\x18\x01"\rascl:1208.0040\x02',
                '\n\x132016AJ....152..123G\x12\x13...................\x18\x01"\x0eascl:1208.00420\x02'
            ]
        second_refids_filename = os.path.join(self.app.conf['PROJ_HOME'], "ADSCitationCapture/tests/data/sample-refids2.dat")
        os.utime(second_refids_filename, (24*60*60, 24*60*60)) # set the access and modified times to 19700102_000000
        expected_citation_change_from_second_file = [
                '\n\x132015MNRAS.453..483K\x12\x13hola...............\x18\x01"\rascl:1208.004(\x010\x03',
                '\n\x131800AnP.....3..113.\x12\x13..................."\x1910.1107/S00218898700059400\x02',
                '\n\x131005PhRvC..71c4906H\x12\x131990PhLB..243..432G"\x1c10.1016/0370-2693(90)91409-5(\x010\x01'
            ]

        # Process first file
        i = 0
        with patch.object(tasks.task_process_citation_changes, 'delay', return_value=None) as task_process_citation_changes:
            self.run(first_refids_filename, schema_prefix=self.schema_prefix)
            self.assertTrue(task_process_citation_changes.called)

            for args in task_process_citation_changes.call_args_list:
                citation_changes = args[0][0]
                for citation_change in citation_changes.changes:
                    print citation_change.SerializeToString()
                    self.assertEqual(citation_change.SerializeToString(), expected_citation_change_from_first_file[i])
                    i += 1

        # Process second file
        i = 0
        with patch.object(tasks.task_process_citation_changes, 'delay', return_value=None) as task_process_citation_changes:
            self.run(second_refids_filename, schema_prefix=self.schema_prefix)
            self.assertTrue(task_process_citation_changes.called)

            for args in task_process_citation_changes.call_args_list:
                citation_changes = args[0][0]
                for citation_change in citation_changes.changes:
                    print citation_change.SerializeToString()
                    self.assertEqual(citation_change.SerializeToString(), expected_citation_change_from_second_file[i])
                    i += 1

if __name__ == '__main__':
    unittest.main()
