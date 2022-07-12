import os
from datetime import datetime
import postgres_copy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine
from ADSCitationCapture.models import ReaderData, ReaderChanges
from adsputils import setup_logging
import adsmsg


class ReaderImport():
    """
    Loads refids file into DB, crossmatches with the previous loaded file (if
    exists) and identifies citation changes. The class is iterable.
    """

    def __init__(self, sqlachemy_url, group_changes_in_chunks_of=1, sqlalchemy_echo=False, schema_prefix="citation_capture_", force=False):
        """
        Initializes the class and prepares DB connection.

        :param sqlachemy_url: URL to connect to the DB.
        :param group_changes_in_chunks_of: Number of citation changes to be
            grouped when iterating.
        :param sqlalchemy_echo: Print every SQL statement.
        :param schema_prefix: Data is stored in schemas that correspond to a
            prefix + file last access date.
        :param force: If tables already exists in DB, drop them and re-ingest.
        """
        self.engine = create_engine(sqlachemy_url, echo=sqlalchemy_echo)
        self.connection = self.engine.connect()
        self.session = sessionmaker(bind=self.engine)()
        #
        # - Use app logger:
        #import logging
        #self.logger = logging.getLogger('ads-citation-capture')
        # - Or individual logger for this file:
        from adsputils import setup_logging, load_config
        proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
        config = load_config(proj_home=proj_home)
        self.logger = setup_logging(__name__, proj_home=proj_home,
                                level=config.get('LOGGING_LEVEL', 'INFO'),
                                attach_stdout=config.get('LOG_STDOUT', False))
        #
        self.table_name = ReaderData.__tablename__
        self.previous_table_name = ReaderData.__tablename__
        self.joint_table_name = ReaderChanges.__tablename__
        self.schema_prefix = schema_prefix
        self.schema_name = None
        self.previous_schema_name = None
        self.input_reader_filename = None
        self.group_changes_in_chunks_of=group_changes_in_chunks_of
        self.offset = 0
        self.ref_bibcode = ""
        self.n_changes = 0
        self.force = force
        self.last_modification_date = None

    def _citation_changes_query(self):
        if self.joint_table_name in Inspector.from_engine(self.engine).get_table_names(schema=self.schema_name):
            ReaderChanges.__table__.schema = self.schema_name
        sqlalchemy_query = self.session.query(ReaderChanges)
        return sqlalchemy_query

    def compute(self, input_reader_filename):
        """
        Loads refids file into DB, crossmatches with the previous loaded file
        (if exists) and identifies citation changes.

        :param input_reader_filename: Path to the file to be imported.
        """
        self.offset = 0
        self.input_reader_filename = input_reader_filename
        self._setup_schemas()
        if self.force or self.joint_table_name not in Inspector.from_engine(self.engine).get_table_names(schema=self.schema_name):
            self.logger.info("Importing '%s' into table '%s.%s'", self.input_reader_filename, self.schema_name, self.table_name)
            try:
                self._import()
            except:
                self.logger.exception("Problem importing file '%s', dropping schema '%s'", self.input_reader_filename, self.schema_name)
                # Roll back created schema for this file
                drop_schema = "drop schema {0} cascade;"
                self._execute_sql(drop_schema, self.schema_name)
                raise
            if self.previous_schema_name is not None:
                self.logger.info("Comparing table '%s.%s' with recreated table from previous process '%s.%s'", self.schema_name, self.table_name, self.previous_schema_name, self.previous_table_name)
            self._join_tables()
            self._calculate_delta()
            self.logger.info("Created table '%s.%s'", self.schema_name, self.joint_table_name)
        else:
            self.logger.info("Table '%s.%s' already exists, re-using results without importing the specified file '%s'", self.schema_name, self.joint_table_name, self.input_reader_filename)
        self.n_changes = self._compute_n_changes()
        self.logger.info("Table '%s.%s' contains '%s' citation changes", self.schema_name, self.joint_table_name, self.n_changes)

    def _execute_sql(self, sql_template, *args):
        """Build sql from template and execute"""
        sql_command = sql_template.format(*args)
        self.logger.debug("Executing SQL: %s", sql_command)
        return self.connection.execute(sql_command)

    def _reader_changes_query(self):
        if self.joint_table_name in Inspector.from_engine(self.engine).get_table_names(schema=self.schema_name):
            ReaderChanges.__table__.schema = self.schema_name
        sqlalchemy_query = self.session.query(ReaderChanges)
        return sqlalchemy_query

    def __iter__(self):
        return self

    def __next__(self): # Python 3: def __next__(self)
        """Iterates over the results, grouping changes in chunks"""
        if self.offset >= self.n_changes or self.n_changes == 0:
            raise StopIteration
        else:
            reader_changes = []
            # Get citation changes from DB
            temp_offset = 0
            #set the reference bibcode if it is not set already
            if self.ref_bibcode == "": 
                instance = self._citation_changes_query().offset(self.offset).yield_per(1)[0]
                prefix = "previous_" if instance.status == "DELETED" else "new_"
                self.ref_bibcode = getattr(instance, prefix+"bibcode")
            
            for instance in self._citation_changes_query().offset(self.offset).yield_per(100):
                # Use new_ or previous_ fields depending if status is NEW/UPDATED or DELETED
                prefix = "previous_" if instance.status == "DELETED" else "new_"
                #Append reader change if is for the reference bibcode.
                if self.ref_bibcode ==  getattr(instance, prefix+"bibcode"):
                    reader_changes.append({'bibcode': getattr(instance, prefix+"bibcode"), 'reader': getattr(instance, prefix+"reader"), 'timestamp': self.last_modification_date, 'status': getattr(instance, "status")})
                    temp_offset += 1
                else:
                    self.ref_bibcode = getattr(instance, prefix+"bibcode")
                    self.session.commit()
                    break
            
            self.offset += temp_offset
            return reader_changes

    def _setup_schemas(self):
        """
        Create new schema, identify previous and drop older ones.
        It also verifies if all the data from the previous schema has been
        processed.
        """
        # Schema name for current file
        self.last_modification_date = datetime.utcfromtimestamp(os.stat(self.input_reader_filename).st_mtime)
        self.schema_name = self.schema_prefix + self.last_modification_date.strftime("%Y%m%d_%H%M%S")

        # Create schema if needed
        existing_schema_names = Inspector.from_engine(self.engine).get_schema_names()
        existing_schema_names = [x for x in existing_schema_names if x.startswith(self.schema_prefix)]
        if self.schema_name not in existing_schema_names:
            self.connection.execute(CreateSchema(self.schema_name))
            filtered_existing_schema_names = existing_schema_names
        else:
            filtered_existing_schema_names = [x for x in existing_schema_names if x != self.schema_name]


        # Determine previous schema name if any
        if len(filtered_existing_schema_names) > 0:
            filtered_existing_schema_names.sort(reverse=True)
            filtered_existing_schema_names = [schema_name for schema_name in filtered_existing_schema_names if "reader" in schema_name]
            self.previous_schema_name = filtered_existing_schema_names[0]

            # Verify the data that is going to be imported is newer than the data already imported
            schema_date_fingerprint = int(self.schema_name.replace(self.schema_prefix, "").replace("_", ""))
            previous_schema_date_fingerprint = int(self.previous_schema_name.replace(self.schema_prefix, "").replace("_", ""))
            if previous_schema_date_fingerprint >= schema_date_fingerprint:
                raise Exception("The data to be imported has a date fingerprint '{0}' equal or older than the data already in the DB '{1}'".format(self.schema_name, self.previous_schema_name))
            
            # Drop old schemas (just keep last 3)
            if len(filtered_existing_schema_names) > 2:
                for old_schema_name in filtered_existing_schema_names[2:]:
                    drop_schema = "drop schema {0} cascade;"
                    self._execute_sql(drop_schema, old_schema_name)

    def _import(self):
        """Import from file, expand its JSON column and delete duplicates"""
        self._copy_from_file()
        self._add_datetime()
        self._drop_nonzenodo_records()
        self._delete_dups()

        # try:
        #     self._verify_input_data()
        # except:
        #     self.logger.exception("Input data does not comply with some assumptions")
        #     raise

    def _copy_from_file(self):
        """Import file into DB"""
        table_already_exists = self.table_name in Inspector.from_engine(self.engine).get_table_names(schema=self.schema_name)
        if table_already_exists and self.force:
            self.logger.info("Dropping table '%s.%s' due to force mode", self.schema_name, self.table_name)
            drop_table = "drop table if exists {0}.{1};"
            self._execute_sql(drop_table, self.schema_name, self.table_name)
        elif table_already_exists:
            return

        ReaderData.__table__.schema = self.schema_name
        ReaderData.__table__.create(bind=self.engine)

        # Import a tab-delimited file
        with open(self.input_reader_filename) as fp:
            l = postgres_copy.copy_from(fp, ReaderData, self.engine, columns=('bibcode', 'reader'))

    def _drop_nonzenodo_records(self):
        """Remove all entries that are not Zenodo records."""
        drop_row_sql = \
                    "DELETE FROM {0}.{1} \
                        WHERE bibcode NOT LIKE '%%zndo%%' "
        self._execute_sql(drop_row_sql, self.schema_name, self.table_name) 

    def _add_datetime(self):
        """
        Adds a datetime column to a given table.
        """
        add_datetime_sql = \
                    "ALTER TABLE {0}.{1} \
                         ADD COLUMN timestamp TIMESTAMP DEFAULT '{2}'"
        self._execute_sql(add_datetime_sql, self.schema_name, self.table_name, self.last_modification_date.isoformat()) 

    def _delete_dups(self):
        """
        The input file can have duplicates such as:
            2014zndo.....11813N 3922c1a910c5f22e
            2014zndo.....11813N 3922c1a910c5f22e
        Note: I am not sure if this is true, for readers, but it is worth keeping for now.
        """
        delete_duplicates_sql = \
            "DELETE FROM {0}.{1} WHERE id IN ( \
                SELECT id FROM \
                    (SELECT id, row_number() over(partition by bibcode, reader order by reader desc) AS dup_id FROM {0}.{1}) t \
                WHERE t.dup_id > 1 \
            )"
        self._execute_sql(delete_duplicates_sql, self.schema_name, self.table_name)

    def _compute_n_changes(self):
        """Count how many citation changes were identified"""
        if self.joint_table_name in Inspector.from_engine(self.engine).get_table_names(schema=self.schema_name):
            n_changes = self._reader_changes_query().count()
            return n_changes
        else:
            return 0

    def _join_tables(self):
        """
        Full join between the previous and the new recreated table but keeping only NEW,
        DELETED and UPDATED records. Previous and new values are preserved in
        columns with names composed by a prefix "previous_" or "new_".

        If there was no previous table, a new fake joint table is built with
        null values for all the "previous_" columns.
        """
        # ~1h
        table_already_exists = self.joint_table_name in Inspector.from_engine(self.engine).get_table_names(schema=self.schema_name)
        if table_already_exists and self.force:
            self.logger.info("Dropping table '%s.%s' due to force mode", self.schema_name, self.joint_table_name)
            drop_table = "drop table if exists {0}.{1};"
            self._execute_sql(drop_table, self.schema_name, self.joint_table_name)
        elif table_already_exists:
            return

        if self.previous_schema_name is None:
            # Not really a JOIN since there is no previous table
            joint_table_sql = \
                    "create table {0}.{2} as \
                        select \
                            {0}.{1}.id as new_id, \
                            {0}.{1}.bibcode as new_bibcode, \
                            {0}.{1}.reader as new_reader, \
                            {0}.{1}.timestamp as new_timestamp, \
                            cast(null as text) as previous_id, \
                            cast(null as text) as previous_bibcode, \
                            cast(null as text) as previous_reader, \
                            cast(null as text) as previous_timestamp \
                        from {0}.{1};"
            self._execute_sql(joint_table_sql, self.schema_name, self.table_name, self.joint_table_name)
        else:
            joint_table_sql = \
                    "create table {0}.{4} as \
                        select \
                            {0}.{2}.id as new_id, \
                            {0}.{2}.bibcode as new_bibcode, \
                            {0}.{2}.reader as new_reader, \
                            {0}.{2}.timestamp as new_timestamp, \
                            {1}.{3}.id as previous_id, \
                            {1}.{3}.bibcode as previous_bibcode, \
                            {1}.{3}.reader as previous_reader, \
                            {1}.{3}.timestamp as previous_timestamp \
                        from {1}.{3} full join {0}.{2} \
                        on \
                            {0}.{2}.bibcode={1}.{3}.bibcode \
                            and {0}.{2}.reader={1}.{3}.reader \
                        where \
                            ({0}.{2}.id is not null and {1}.{3}.id is null) \
                            or ({0}.{2}.id is null and {1}.{3}.id is not null) \
                            or ({0}.{2}.id is not null and {1}.{3}.id is not null and ({0}.{2}.bibcode<>{1}.{3}.bibcode or {0}.{2}.reader<>{1}.{3}.reader)) \
                        ;"
            self._execute_sql(joint_table_sql, self.schema_name, self.previous_schema_name, self.table_name, self.previous_table_name, self.joint_table_name)

        add_id_column_sql = "ALTER TABLE {0}.{1} ADD COLUMN id SERIAL PRIMARY KEY;"
        self._execute_sql(add_id_column_sql, self.schema_name, self.joint_table_name)

        status_enum_name = "status_type"
        enum_names = [e['name'] for e in Inspector.from_engine(self.engine).get_enums(schema=self.schema_name)]
        if status_enum_name not in enum_names:
            create_enum_type = "CREATE TYPE {0}.{1} AS ENUM ('NEW', 'DELETED', 'UPDATED');"
            self._execute_sql(create_enum_type, self.schema_name, status_enum_name)
        add_status_column_sql = "ALTER TABLE {0}.{1} ADD COLUMN status {0}.{2};"
        self._execute_sql(add_status_column_sql, self.schema_name, self.joint_table_name, status_enum_name)

        ## ~1h
        #create_index = "CREATE INDEX status_idx ON {0}.{1} (status);"
        #self._execute_sql(create_index, self.schema_name,  self.joint_table_name)

    def _calculate_delta(self):
        """Classify citation changes as NEW, UPDATED or DELETED"""
        update_status_updated_sql = \
           "update {0}.{1} \
            set status='UPDATED' \
            where \
                {0}.{1}.status is null \
                and {0}.{1}.new_id is not null \
                and {0}.{1}.previous_id is not null \
                and ({0}.{1}.new_bibcode<>{0}.{1}.previous_bibcode \
                    or {0}.{1}.new_reader<>{0}.{1}.previous_reader);"
        self._execute_sql(update_status_updated_sql, self.schema_name, self.joint_table_name)

        update_status_new_sql = \
           "update {0}.{1} \
            set status='NEW' \
            where \
                {0}.{1}.status is null \
                and {0}.{1}.new_id is not null \
                and {0}.{1}.previous_id is null;"

        self._execute_sql(update_status_new_sql, self.schema_name, self.joint_table_name)

        update_status_deleted_sql = \
           "update {0}.{1} \
            set status='DELETED' \
            where \
                {0}.{1}.status is null \
                and {0}.{1}.new_id is null \
                and {0}.{1}.previous_id is not null;"
        self._execute_sql(update_status_deleted_sql, self.schema_name, self.joint_table_name)


