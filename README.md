<!---[![Waffle.io - Columns and their card count](https://badge.waffle.io/adsabs/ADSCitationCapture.svg?columns=all)](https://waffle.io/adsabs/ADSCitationCapture)--->
[![Build Status](https://github.com/adsabs/ADSCitationCapture/actions/workflows/python_actions.yml/badge.svg)](https://github.com/adsabs/ADSCitationCapture/)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSCitationCapture/badge.svg)](https://coveralls.io/r/adsabs/ADSCitationCapture)
[![Code Climate](https://codeclimate.com/github/adsabs/ADSCitationCapture/badges/gpa.svg)](https://codeclimate.com/github/adsabs/ADSCitationCapture)
[![Issue Count](https://codeclimate.com/github/adsabs/ADSCitationCapture/badges/issue_count.svg)](https://codeclimate.com/github/adsabs/ADSCitationCapture)
# ADSCitationCapture

## Logic

The Citation Capture pipeline will process an ADS Classic generated file that contains the list of identified citations to DOI/PID/URLs:

```
python3 run.py PROCESS refids_zenodo.dat.20180911
```

The input file can have duplicates such as:

```
2011arXiv1112.0312C	{"cited":"2012ascl.soft03003C","citing":"2011arXiv1112.0312C","pid":"ascl:1203.003","score":"1","source":"/proj/ads/references/resolved/arXiv/1112/0312.raw.result:10"}
2011arXiv1112.0312C	{"cited":"2012ascl.soft03003C","citing":"2011arXiv1112.0312C","pid":"ascl:1203.003","score":"1","source":"/proj/ads/references/resolved/AUTHOR/2012/0605.pairs.result:89"}
```

This is because the same citation was identified in more than one source. Only one entry will be selected among these duplicates, prioritising a resolved one if there is any.

In a synchronous fashion, a schema is created with a name based on the file last modification date (e.g., `citation_capture_20180919_113032`), the file is imported into a new table named `raw_citation` and the JSON fields are expanded in a second table named `expanded_raw_citation`.

To detect changes between the new imported file and the previous one, instead of using the previous `expanded_raw_citation` in the previous database schema, the pipeline reconstructs the data from the table where all the processed records are stored (`public` schema), creating the table `recreated_previous_expanded_citation` in the previous schema. This ensures that the pipeline is going to detect not only changes with respect to the previous imported file, but changes to what it was actually correctly processed from the previous imported file. If any record was not processed from the previous imported file, then it will be stored in another table named `not_processed_raw_citation` in case further manual analysis is required.

Next, a full join based on `citing` and `content` fields (which supposed to be unique) is executed between the previous recreated expanded table and the new expanded table but keeping only NEW, DELETED and UPDATED records. The resulting table is named `citation_changes`. Previous and new values are preserved in columns with names composed by a prefix `previous_` or `new_`. If there was no previous table, a new emulated joint table is built with null values for all the `previous_` columns. The meaning of the status field:

- NEW: Citation that did not exist before in the pipeline database (i.e., `citing` and `content` had never been ingested before)
- UPDATED: Citation that already exists in the database and for which the `cited` or `resolved` fields have changed. The most frequent case is when `cited` was empty and now there is a bibcode assigned, and `resolve` may become `True` for a subset of these cases.
- DELETED: Citation that disappeared (i.e., `citing` and `content` ingested before, now do not exist in the input file). These are the case for corrected bibcodes (i.e., changed `citing`), merged records (i.e., `citing` now contains the publisher instead of the arXiv bibcode), changes to `content` due to a different parsing of the publication source file, deleted citations because a new version of an arXiv paper has been published without previously detected citations.

Every `citation change` is sent to an asynchronous task for processing and they all have a timestamp that matches the last modification date from the original imported file:

- NEW: Skip it if the `citing` bibcode does not exist in the system (query to ADS API). If it does exist, metadata in datacite format will be fetched/parsed for citations to DOIs (only case that we care about given the current scope of the ASCLEPIAS project), and a new citation entry will be created in the `citation` table in the database. For records citing DOIs of software records:
    - If the ADS API returns a canonical bibcode different from the current `citing` bibcode, a `IsIdenticalTo` event is sent to Zenodo's broker linking the both bibcodes.
    - The cited target is created in Solr with a list of citations that is built using the ADS API to make sure that all the bibcodes exist. Additionally, `run.py` offers a `MAINTENANCE` option to verify the citations of all the registered and detect merges/deletions via the ADS API.
    - This will trigger a `Cites` event to Zenodo's broker, creating a relationship between the canonical version of the `citing` bibcode (we use the API to obtain the canonical if it exists) and the DOI.
    - This will trigger a `IsIdenticalTo` event to Zenodo's broker, creating a relationship between the DOI and the newly created bibcode of the record that will be send to Solr.
- UPDATED: its respective entry in `citation` is updated. For records citing DOIs of software records:
    - If the citation field `resolved` is `True`, this update will NOT trigger a `IsIdenticalTo` event to Zenodo's broker given that the process that generates the raw input file does not have access to the required data to do a proper match.
    - This case will not generate events to the broker.
- DELETED: its respective entry in `citation` is marked as status DELETED but the row is not actually deleted.
    - No deletions are sent to Zenodo's broker since they are not supported, but the code is ready for when there will be a specification.

The timestamp field is used to avoid race conditions (e.g., older messages are processed after newer messages), no changes will be made to the database if the timestamp of the `citation change` is older than the timestamp registered in the database.

All the generated events are stored in the `logs/` directory and in the database.


## Setup

### Minimal development/testing environment

This setup relies on docker compose to create two containers, one with a postgres database and the other with the pipeline. To enable it, run:

```
scripts/run-pytest.sh
```

The tests will automatically run, and at the end the container will wait until the user presses CTRL+c. While waiting, the user can run in a separate terminal:

```
docker exec -it pytest_citation_capture_pipeline bash
```

To interactively re-run tests if needed.

To clean everything created by the run command, this can be executed:

```
scripts/clean-pytest.sh
```


### Simple development/testing environment

The simple development/testing environment only requires a PostgreSQL instance. The easiest is to  run it on the local machine via docker:

```
docker stop postgres
docker rm postgres
docker run -d -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -p 5432:5432 --name postgres  postgres:9.6 # http://localhost:15672
```

The creation of a user and a database is also required:

```
docker exec -it postgres bash -c "psql -c \"CREATE ROLE citation_capture_pipeline WITH LOGIN PASSWORD 'citation_capture_pipeline';\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE citation_capture_pipeline;\""
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE citation_capture_pipeline TO citation_capture_pipeline;\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE citation_capture_pipeline_test;\""
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE citation_capture_pipeline_test TO citation_capture_pipeline;\""
```

Copy `config.py` to `local_config.py` and modify its content to reflect your system setup. Add the following flags to `local_config.py` to convert all the asynchronous calls into synchronous, not needing a worker (nor RabbitMQ) and allowing easier debugging (e.g., `import pudb; pudb.set_trace()`):

```
### Testing:
# When 'True', no events are emitted to the broker via the webhook
TESTING_MODE = True
# When 'True', it converts all the asynchronous calls into synchronous,
# thus no need for rabbitmq, it does not forward to master
# and it allows debuggers to run if needed:
CELERY_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
```

Then, prepare the python environment:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
pip3 install -r dev-requirements.txt
```

**NOTE** You will need postgres installed in your system for psycog2 to compile (e.g., `sudo apt install libpq-dev`). If you installed via MacPorts, you may need to run to include `pg_config` in the PATH:

```
PATH=/opt/local/lib/postgresql10/bin/:$PATH
pip install -r requirements.txt
```

Initialize the database:

```
alembic upgrade head
```

Or restore a previous database backup:

```
docker restart postgres
docker exec -it postgres bash -c "psql -c \"DROP DATABASE citation_capture_pipeline;\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE citation_capture_pipeline;\""
cat backups/citation_capture_pipeline.sql.20181231 | docker exec -i postgres bash -c "psql"
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE citation_capture_pipeline TO citation_capture_pipeline;\""
```

You can access the database with and use several key commands to inspect the database from [the PostgreSQL cheatsheet](https://gist.github.com/Kartones/dd3ff5ec5ea238d4c546):

```
docker exec -it postgres bash -c "psql citation_capture_pipeline"
```


Then you can run the diagnose with fake data (beware this will end up in the database):

```
python3 run.py DIAGNOSE
```

Or you can run an ingestion (input files can be found in ADS back-end servers `/proj/ads/references/links/refids_zenodo.dat`):

```
python3 run.py PROCESS refids_zenodo.dat.20180911
python3 run.py PROCESS refids_zenodo.dat.20180914
```

If you need to dump/export/backup the database to a file:

```
docker exec -it postgres bash -c "pg_dump --clean --if-exists --create  citation_capture_pipeline" > citation_capture_pipeline.sql
```


### Complex development/testing environment

The complex development/testing environment requires a PostgreSQL database as described before, but also other elements such as RabbitMQ, master pipeline, resolver service and a Solr instance. In this setup, tasks will be executed asynchronously and the `local_config.py` needs to reflect it by setting `CELERY_ALWAYS_EAGER` and `CELERY_EAGER_PROPAGATES_EXCEPTIONS` to `False`:

```
### Testing:
# When 'True', no events are emitted to the broker via the webhook
TESTING_MODE = True
# When 'True', it converts all the asynchronous calls into synchronous,
# thus no need for rabbitmq, it does not forward to master
# and it allows debuggers to run if needed:
CELERY_ALWAYS_EAGER = False
CELERY_EAGER_PROPAGATES_EXCEPTIONS = False
```

Hence, the pipeline will send messages via RabbitMQ to execute tasks from the master pipeline, which will store its own data in PostgreSQL, update Solr and send data to the resolver service.

#### RabbitMQ

To run an instance of RabbitMQ on the local machine via docker:

```
docker stop rabbitmq
docker rm rabbitmq
docker run -d --hostname rabbitmq -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest -p 15672:15672 -p 5672:5672 --name rabbitmq rabbitmq:3.6-management
```

The creation of virtual hosts is also required:

```
docker exec -it rabbitmq bash -c "rabbitmqctl add_vhost citation_capture_pipeline"
docker exec -it rabbitmq bash -c "rabbitmqctl set_permissions -p citation_capture_pipeline guest '.*' '.*' '.*'"
```

Copy `config.py` to `local_config.py` and modify its content to reflect your system. The RabbitMQ web interface can be found at [http://localhost:15672](http://localhost:15672)


#### Master Pipeline

Prepare the database:

```
docker exec -it postgres bash -c "psql -c \"CREATE ROLE master_pipeline WITH LOGIN PASSWORD 'master_pipeline';\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE master_pipeline;\""
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE master_pipeline TO master_pipeline;\""
```

And RabbitMQ:

```
docker exec -it rabbitmq bash -c "rabbitmqctl add_vhost master_pipeline"
docker exec -it rabbitmq bash -c "rabbitmqctl set_permissions -p master_pipeline guest '.*' '.*' '.*'"
```

Clone [ADSMasterPipeline](https://github.com/adsabs/ADSMasterPipeline/) and copy `config.py` to `local_config.py` and modify its content to reflect your system. Then, install dependencies:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
pip3 install -r dev-requirements.txt
alembic upgrade head
```
Currently, `python3` versions `python 3.10` and newer are incompatible as certain required features have been deprecated or renamed.

Diagnose your setup by running an asynchronous worker (it requires a RabbitMQ instance):

```
celery worker -l DEBUG -A ADSCitationCapture.tasks -c 1
```

When data is sent to master pipeline, it can be asked to process it (i.e., send to Solr and resolver service) with:

```
python3 run.py -r s -o -f --ignore_checksums -s 1972
python3 run.py -r l -o -f --ignore_checksums -s 1972
```

A list of bibcodes can be specified to force sending them to solr/resolver service:

```
psql10 -h localhost -p 6432 -U citation_capture_pipeline citation_capture_pipeline -c "SELECT parsed_cited_metadata->'bibcode' FROM citation_target WHERE status='REGISTERED';" > bibcodes_force_citation_capture.txt
python3 run.py -f -r s -n logs/bibcodes_force_citation_capture.txt --ignore_checksums
```

#### Resolver service

Prepare the database:

```
docker exec -it postgres bash -c "psql -c \"CREATE ROLE resolver_service WITH LOGIN PASSWORD 'resolver_service';\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE resolver_service;\""
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE resolver_service TO resolver_service;\""
```

Clone [resolver-service](https://github.com/adsabs/resolver_service) and copy `config.py` to `local_config.py` and modify its content to reflect your system. Then, install dependencies:

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
pip3 install -r dev-requirements.txt
alembic upgrade head
```

And run it (it will listen on `http://0.0.0.0:5000/`):

```
python3 wsgi.py
```

#### Solr

Download the compiled version of montysolr and run it:

```
https://github.com/romanchyla/montysolr/releases
wget -c https://github.com/romanchyla/montysolr/releases/download/v63.1.0.36/montysolr.zip
unzip montysolr.zip
cd montysolr/
bash bin/solr start -f -p 8984
```

Access the solr query interface via [http://localhost:8984/solr/#/collection1/query](http://localhost:8984/solr/#/collection1/query)


#### Verifying your complex development/testing environment

Run unit tests via:

**WARNING**: Unit tests will concatenate `_test` to the database string defined in the `config` file, they assume that database is empty and they will insert/delete data.

```
py.test ADSCitationCapture/tests/
```

Diagnose your setup by running an asynchronous worker (it requires a RabbitMQ instance):

```
celery worker -l DEBUG -A ADSCitationCapture.tasks -c 1
```

Then you can run the diagnose with fake data (beware this will end up in the database/solr):

```
python3 run.py DIAGNOSE
```

Or you can run an ingestion (input files can be found in ADS back-end servers `/proj/ads/references/links/refids_zenodo.dat`):

```
python3 run.py PROCESS refids_zenodo.dat.20180911
python3 run.py PROCESS refids_zenodo.dat.20180914
```

### Production

To deploy in production we need to follow these steps:


- Create a version of the pipeline and upload it to AWS S3:

```
ssh ads@backoffice_hostname
git clone https://github.com/adsabs/eb-deploy
cd eb-deploy/
source init.sh
cd backoffice/backoffice/
sudo apt install zip # Make sure zip command exists
cv citation_capture_pipeline --force # Ignore errors related to `aws elasticbeanstalk create-application-version`
```

- Deploy the pipeline

```
cd /proj.backoffice_hostname/backoffice/
run-s3-locally.sh backoffice citation_capture_pipeline # /proj/ads/ads/devtools/bin/run-s3-locally.sh
```
 
- Access the pipeline container and setup the database schema

```
docker exec -it backoffice_citation_capture_pipeline bash
alembic upgrade head
```
 
- Access the pipeline database
 
```
docker exec -it backoffice_citation_capture_pipeline bash
apt install postgresql-client -y
psql -h database_hostname -p database_port -U database_user citation_capture_pipeline
```

- Dump/export/backup database to a file

```
ssh database_hostname
docker exec -it backoffice_postgres bash -c "pg_dump --username=citation_capture_pipeline --clean --if-exists --create  citation_capture_pipeline" > citation_capture_pipeline.sql
```

- Process the file `refids_zenodo_small_software_record_sample.dat`:

```
2012arXiv1208.3124R	{"cited":"...................","citing":"2012arXiv1208.3124R","doi":"10.5281/zenodo.1048204","score":"0","source":"/proj/ads/references/resolved/arXiv/1208/3124.raw.result:52"}
2012arXiv1212.1095R	{"cited":"...................","citing":"2012arXiv1212.1095R","doi":"10.5281/zenodo.1048204","score":"0","source":"/proj/ads/references/resolved/arXiv/1212/1095.raw.result:67"}
2013arXiv1305.5675A	{"cited":"2014hesa.conf11759.","citing":"2013arXiv1305.5675A","doi":"10.5281/zenodo.11759","score":"5","source":"/proj/ads/references/resolved/arXiv/1305/5675.raw.result:42"}
2013arXiv1307.4030S	{"cited":"...................","citing":"2013arXiv1307.4030S","doi":"10.5281/zenodo.10679","score":"0","source":"/proj/ads/references/resolved/arXiv/1307/4030.raw.result:54"}
2014NatCo...5E5024S	{"cited":"...................","citing":"2014NatCo...5E5024S","doi":"10.5281/zenodo.10679","score":"0","source":"/proj/ads/references/resolved/NatCo/0005/iss._chunk_.2014NatCo___5E50.nature.xml.result:54"}
```

with the commands:

```
docker exec -it backoffice_citation_capture_pipeline bash
python3 run.py PROCESS refids_zenodo_small_software_record_sample.dat
```

which should create:

```
citation_capture_pipeline=> select content, parsed_cited_metadata->'bibcode' from citation_target;
        content         |       ?column?
------------------------+-----------------------
 10.5281/zenodo.1048204 | "2017zndo...1048204R"
 10.5281/zenodo.11759   | "2014zndo.....11759V"
 10.5281/zenodo.10679   | "2014zndo.....10679I"
 
citation_capture_pipeline=> select content, citing from citation;
        content         |       citing
------------------------+---------------------
 10.5281/zenodo.1048204 | 2012arXiv1208.3124R
 10.5281/zenodo.1048204 | 2012arXiv1212.1095R
 10.5281/zenodo.11759   | 2013arXiv1305.5675A
 10.5281/zenodo.10679   | 2013arXiv1307.4030S
 10.5281/zenodo.10679   | 2014NatCo...5E5024S
(5 rows)

master_pipeline=> select bibcode,solr_processed,datalinks_processed,processed,status from records where bibcode in ('2017zndo...1048204R', '2014zndo.....11759V', '2014zndo.....10679I');
       bibcode       | solr_processed | datalinks_processed | processed | status
---------------------+----------------+---------------------+-----------+--------
 2014zndo.....10679I |                |                     |           |
 2014zndo.....11759V |                |                     |           |
 2017zndo...1048204R |                |                     |           |
(3 rows)
```

- Access the pipeline broker

```
ssh broker_hostname
docker exec -it backoffice_rabbitmq rabbitmqctl list_queues -q -p citation_capture_pipeline
```


## Usage

- Process file:

```
python3 run.py PROCESS refids_zenodo.dat.20180911
```

- Update references to their canonical form using ADS API:

```
# All the registered citation targets
python3 run.py MAINTENANCE --canonical
# Specific bibcode (space separated list)
python3 run.py MAINTENANCE --canonical --bibcode 2017zndo....840393W
# Specific doi (space separated list)
python3 run.py MAINTENANCE --canonical --doi 10.5281/zenodo.840393
```

- Update metadata:
    - It will identify as concept DOI all the records that report an empty `version_of` and a filled `versions` with at least one item (concept DOIs are basically pointers to the last release of a Zenodo record, thus publication year and authors can change with time).
    - If the record is identified as a concept DOI, metadata updates are merged in the system (overwriting all metadata) but the bibcode will not be changed. Hence, the bibcode will have the year of the first time the record was ingested in ADS.
    - If the bibcode changes, it generates `IsIdenticalTo` events and the old bibcode will also be listed in the alternate bibcodes/identifiers fields

```
# All the registered citation targets
python3 run.py MAINTENANCE --metadata
# Specific bibcode (space separated list)
python3 run.py MAINTENANCE --metadata --bibcode 2017zndo....840393W
# Specific doi (space separated list)
python3 run.py MAINTENANCE --metadata --doi 10.5281/zenodo.840393
# File containing doi and version columns (tab separated)
python3 run.py MAINTENANCE --metadata --doi /proj/ads/references/links/zenodo_updates_09232019.out
```

- Curate metadata:
    - User supplies an input file containing entries they wish to modify with each line in json form:
    ```
    {"bibcode":"XYZ....2022","key_to_change_1":"value_1","key_to_change_2":"value_2"}
    ```
    - Will identify database entries specified in `input_filename`
    - Parse each line of `input_filename` into separate json entries.
    - Replace `parsed_cited_metadata` field entries with those from `input_filename`.
    - Save `input_filename` entries in separate `curated_metadata` field to prevent automated updates from overwriting curated changes.


```
# Curating based on an input file.
python3 run.py MAINTENANCE --curation --input_filename $path/to/input_file
# Curating based on an JSON from a command line argument.
python3 run.py MAINTENANCE --curation --json {'curated_metadata'}
# Delete curated_metadata for a given entry by bibcode
python3 run.py MAINTENANCE --curation --bibcode "YYYYzndo...BCDEFGR" --delete
# Delete curated_metadata for a given entry by doi
python3 run.py MAINTENANCE --curation --doi "10.XYZA/ZENODO.BCDEFG" --delete
# Delete curated_metadata by file
python3 run.py MAINTENANCE --curation --input_filename $/path/to/input_file --delete

```
For deleting by input file, only the `doi` or `bibcode` needs to be specified in the file. Any other details entered into the entry will be added as new metadata. You can use this to remove a single value from the curated metadata by passing a json entry with all the old values except for the one to be deleted.

- Curated Example

    For a given citation target in the database, the `parsed_cited_metadata` takes the form

    ```
    parsed_cited_metadata
    ----------------------

    {"forks": [], "title": "Some Title", "source": "Zenodo", "authors": ["Last, First"], "bibcode": "YYYYzndo...BCDEFGR", "doctype": "software", "pubdate": "YYYY-MM-DD", "version": "X.Y", "abstract": "abstract text", "keywords": ], "versions": ["list of dois"], "citations": [], "link_alive": true, "properties": {"DOI": "10.XYZA/ZENODO.BCDEFG", "OPEN": 1}, "references": ["doi", "arxiv"], "version_of": ["doi"], "forked_from": [],"affiliations": ["Some Institution<ORCID>0000-0009-8765-4321</ORCID>"],"described_by": [],"description_of": [],"normalized_authors": ["Last, F"]}
    ```
   
    Modifications to the metadata can be made by supplying a file of the form


    ```
    sample_input_file.dat
    ----------------------

    {"authors": ["Some, Name"], "bibcode": YYYYzndo...BCDEFGR", "affiliations": ["Some Other Institution <ORCID>0000-0001-2345-6789</ORCID>"], "normalized_authors": ["Some, N"]}
    ```
   
   `input_filename` requires either `"bibcode"` or `"doi"` to be set for the entry to be retrieved. All other entries are optional.

    The `--curation` flag takes this input and converts `parsed_cited_metadata` to


    ```
    parsed_cited_metadata
    ----------------------

    {"forks": [], "title": "Some Title", "source": "Zenodo", "authors": ["Some, Name"], "bibcode": "YYYYzndo...BCDEFGR", "doctype": "software", "pubdate": "YYYY-MM-DD", "version": "X.Y", "abstract": "abstract text", "keywords": ["keyword1", "keyword2", "keyword3", "keyword4"], "versions": ["list of dois"], "citations": [], "link_alive": true, "properties": {"DOI": "10.XYZA/ZENODO.BCDEFG", "OPEN": 1}, "references": ["doi", "arxiv"], "version_of": ["doi"], "forked_from": [], "affiliations": ["Some Other Institution <ORCID>0000-0001-2345-6789</ORCID>"],"described_by": [],"description_of": [],"normalized_authors": ["Some, N"]}
    ```

    The modified `parsed_cited_metadata` is then used to construct the message forwarded to Master Pipeline.

# Miscellaneous

## Alembic

The project incorporates Alembic to manage PostgreSQL migrations based on SQLAlchemy models. It has already been initialized with:

```
alembic init alembic
```

And the file `alembic/env.py` was modified to import SQLAlchemy models and use the database connection coming from the `local_config.py` file. As a reminder, some key commands from [Alembic tutorial](http://alembic.zzzcomputing.com/en/latest/):

```
alembic revision -m "baseline" --autogenerate
alembic current
alembic history
alembic upgrade head
alembic downgrade -1
alembic upgrade +1
```

## PostgreSQL commands

Useful SQL requests:

- List schemas and tables where imports happen:
    
```
\dt c*.*
```

- List tables with the processed data:
    
```
\dt
\d+ citation_target
```

- Show description of a table
    
```
\d+ citation_target
```

- List and delete schemas
    
```
SELECT nspname FROM pg_catalog.pg_namespace;
DROP SCHEMA citation_capture_20180919_153032 CASCADE;
```

- Access JSONB fields

```
SELECT parsed_cited_metadata->'bibcode' AS bibcode, parsed_cited_metadata->'doctype' AS doctype, parsed_cited_metadata->'title' AS title, parsed_cited_metadata->'version' AS version, content FROM citation_target;
```

- Access nested JSONB fields

```
SELECT * FROM event WHERE (data ->> 'RelationshipType')::json ->> 'SubType'  = 'IsIdenticalTo';
```

- Top registered citations

```
SELECT citation_target.parsed_cited_metadata->'title' AS title, citation_target.parsed_cited_metadata->'version' AS version, g.count FROM (SELECT content, count(*) FROM citation WHERE status = 'REGISTERED' GROUP BY content) AS g INNER JOIN citation_target USING (content) ORDER BY g.count DESC;
```
 
- More frequent updated citations

```
SELECT citing, content, count(*) FROM citation_version GROUP BY citing, content ORDER BY count(*) DESC HAVING count(*) > 1 ;
```

- Status statistics

```
SELECT status, count(*) FROM citation_target GROUP BY status;
SELECT status, count(*) FROM citation GROUP BY status;
```

- Reconstruct expanded raw data

```
SELECT id, citing, cited, CASE WHEN citation_target.content_type = 'DOI' THEN true ELSE false END AS doi, CASE WHEN citation_target.content_type = 'PID' THEN true ELSE false END AS pid, CASE WHEN citation_target.content_type = 'URL' THEN true ELSE false END AS url, citation.content, citation.resolved, citation.timestamp FROM citation INNER JOIN citation_target ON citation.content = citation_target.content WHERE citation.status != 'DELETED';
```

## Build sample refids.dat

```
grep -E '"doi":.*"score":"0"' /proj/ads/references/links/refids.dat | head -2 > sample-refids.dat
grep -E '"doi":.*"score":"1"' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"doi":.*"score":"5"' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"pid":.*"score":"0"' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"pid":.*"score":"1"' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"pid":.*"score":"5"' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"0".*"url":.*github' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"1".*"url":.*github' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"5".*"url":.*github' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"0".*"url":.*sourceforge' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"1".*"url":.*sourceforge' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"5".*"url":.*sourceforge' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"0".*"url":.*bitbucket' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"1".*"url":.*bitbucket' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"5".*"url":.*bitbucket' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"0".*"url":.*google' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"1".*"url":.*google' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
grep -E '"score":"5".*"url":.*google' /proj/ads/references/links/refids.dat | head -2 >> sample-refids.dat
```

# Build sample refids_zenodo.dat

This will contain only entries with the zenodo word, which mostly are going to be zenodo URLs:

```
cp /proj/ads/references/links/refids_zenodo.dat refids_zenodo.dat.20180918
zgrep -i zenodo /proj/ads/references/links/refids.dat.20180914.gz > refids_zenodo.dat.20180914
zgrep -i zenodo /proj/ads/references/links/refids.dat.20180911.gz > refids_zenodo.dat.20180911
```

