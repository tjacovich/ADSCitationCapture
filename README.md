[![Waffle.io - Columns and their card count](https://badge.waffle.io/adsabs/ADSCitationCapture.svg?columns=all)](https://waffle.io/adsabs/ADSCitationCapture)
[![Build Status](https://travis-ci.org/adsabs/ADSCitationCapture.svg?branch=master)](https://travis-ci.org/adsabs/ADSCitationCapture)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSCitationCapture/badge.svg)](https://coveralls.io/r/adsabs/ADSCitationCapture)
[![Code Climate](https://codeclimate.com/github/adsabs/ADSCitationCapture/badges/gpa.svg)](https://codeclimate.com/github/adsabs/ADSCitationCapture)
[![Issue Count](https://codeclimate.com/github/adsabs/ADSCitationCapture/badges/issue_count.svg)](https://codeclimate.com/github/adsabs/ADSCitationCapture)
# ADSCitationCapture


## Setup

### Database

To run the pipeline, it is necessary to have access to a postgres instances. To run it on the local machine via docker:

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
```

Copy `config.py` to `local_config.py` and modify its content to reflect your system. Then, prepare the database:

```
virtualenv python/
source python/bin/activate
pip install -r requirements.txt
pip install -r dev-requirements.txt
alembic upgrade head
```

You can access the database with and use several key commands to inspect the database from [the PostgreSQL cheatsheet](https://gist.github.com/Kartones/dd3ff5ec5ea238d4c546):

```
docker exec -it postgres bash -c "psql citation_capture_pipeline"
```


### RabbitMQ

An instance of RabbitMQ is necessary to run the pipeline in asynchronous mode (as it will be in production). To run it on the local machine via docker:

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

### Master Pipeline

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
virtualenv python/
source python/bin/activate
pip install -r requirements.txt
pip install -r dev-requirements.txt
alembic upgrade head
```

Diagnose your setup by running an asynchronous worker (it requires a rabbitmq instance):

```
celery worker -l DEBUG -A ADSCitationCapture.tasks -c 1
```

When data is sent to master pipeline, it can be asked to process it (i.e., send to Solr and resolver service) with:

```
python run.py -r s -o -f --ignore_checksums -s 1972
python run.py -r l -o -f --ignore_checksums -s 1972
```

### Resolver service

Prepare the database:

```
docker exec -it postgres bash -c "psql -c \"CREATE ROLE resolver_service WITH LOGIN PASSWORD 'resolver_service';\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE resolver_service;\""
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE resolver_service TO resolver_service;\""
```

Clone [resolver-service](https://github.com/adsabs/resolver_service) and copy `config.py` to `local_config.py` and modify its content to reflect your system. Then, install dependencies:

```
virtualenv python/
source python/bin/activate
pip install -r requirements.txt
pip install -r dev-requirements.txt
alembic upgrade head
```

And run it (it will listen on `http://0.0.0.0:5000/`):

```
python wsgi.py
```

### Solr

Download the compiled version of montysolr and run it:

```
https://github.com/romanchyla/montysolr/releases
wget -c https://github.com/romanchyla/montysolr/releases/download/v63.1.0.36/montysolr.zip
unzip montysolr.zip
cd montysolr/
bash bin/solr start -f -p 8984
```

Access the solr query interface via [http://localhost:8984/solr/#/collection1/query](http://localhost:8984/solr/#/collection1/query)


## Testing your setup

Run unit tests via (it requires a postgres instance):

```
py.test ADSCitationCapture/tests/
```

Diagnose your setup by running an asynchronous worker (it requires a rabbitmq instance):

```
celery worker -l DEBUG -A ADSCitationCapture.tasks -c 1
```

And then running the diagnose:

```
python run.py --diagnose
```

## Development

It may be useful to add the following flags to `local_config.py` (only for development) which will convert all the asynchronous calls into synchronous, not needing a worker to run them (nor rabbitmq) and allowing easier debugging (e.g., `import pudb; pudb.set_trace()`):

```
LOG_STDOUT = True
LOGGING_LEVEL = 'DEBUG'
CELERY_ALWAYS_EAGER = True
CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
```

Then you can run the diagnose or an ingestion (input files can be found in ADS back-end servers `/proj/ads/references/links/refids_zenodo.dat`):

```
python run.py --diagnose
python run.py -r refids_zenodo.dat.20180911
python run.py -r refids_zenodo.dat.20180914
```

To dump the database to a file:

```
docker exec -it postgres bash -c "pg_dump --clean --if-exists --create  citation_capture_pipeline" > citation_capture_pipeline.sql
```

To restore/import the database from a file:

```
cat citation_capture_pipeline.sql | docker exec -i postgres bash -c "psql"
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE citation_capture_pipeline TO citation_capture_pipeline;\""
```

# Miscellaneous


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

## Logic

The pipeline will process a ADS Classic generated file that contains the list of identified citations to DOI/PID/URLs:

```
python run.py -r refids_zenodo.dat.20180911
```

The input file can have duplicates such as:

```
2011arXiv1112.0312C	{"cited":"2012ascl.soft03003C","citing":"2011arXiv1112.0312C","pid":"ascl:1203.003","score":"1","source":"/proj/ads/references/resolved/arXiv/1112/0312.raw.result:10"}
2011arXiv1112.0312C	{"cited":"2012ascl.soft03003C","citing":"2011arXiv1112.0312C","pid":"ascl:1203.003","score":"1","source":"/proj/ads/references/resolved/AUTHOR/2012/0605.pairs.result:89"}
```

This is because the same citation was identified in more than one source. Only one entry will be selected among these duplicates, prioritising a resolved one if there is any.

In a synchronous fashion, a schema is created with a name based on the file last modification date (e.g., `citation_capture_20180919_113032`), the file is imported into a new table named `raw_citation` and the JSON fields are expanded in a second table named `expanded_raw_citation`.

- `citation_changes`: Changes with respect to the last data ingestion (i.e., NEW, UPDATED, DELETED)
    
Next, a full join based on `citing` and `content` fields (which supposed to be unique) is executed between the previous and the new expanded tables but keeping only NEW, DELETED and UPDATED records. The resulting table is named `citation_changes`. Previous and new values are preserved in columns with names composed by a prefix `previous_` or `new_`. If there was no previous table, a new emulated joint table is built with null values for all the `previous_` columns.

Every `citation change` is sent to an asynchronous task for processing and they all have a timestamp that matches the last modification date from the original imported file:

- If the citation change type is NEW, metadata in datacite format will be fetched/parsed for citations to DOIs (only case that we care about given the current scope of the ASCLEPIAS project), and a new citation entry will be created in the `citation` table in the database.
- If the citation change type is UPDATED, its respective entry in `citation` is updated.
- If the citation change type is DELETED, its respective entry in `citation` is marked as status DELETED but the row is not actually deleted.

Updates and deletion for records that do not exist in `citation` are logged and ignored, NEW for records that are already in `citation` are logged and ignored. The timestamp field is used to avoid race conditions (e.g., older messages are processed after newer messages), no changes will be made to the database if the timestamp of the `citation change` is older than the timestamp registered in the database. This implies that it is possible to check if all the citation changes have been processed by comparing these two numbers:

```
docker exec -it postgres bash -c "psql citation_capture_pipeline"
SELECT timestamp, count(*) FROM citation GROUP BY timestamp;
SELECT new_timestamp, count(*) FROM citation_capture_20180919_153032.citation_changes GROUP BY new_timestamp;
```


Other useful SQL requests:

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

## Deploy in production

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
cd /proj.backoffice_hostname
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
 
- Start a worker in debug mode:

```
docker exec -it backoffice_citation_capture_pipeline bash
celery worker -l DEBUG -A ADSCitationCapture.tasks -c 1
```

- Import the file `refids_zenodo_small_software_record_sample.dat`:

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
python run.py -r refids_zenodo_small_software_record_sample.dat
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

