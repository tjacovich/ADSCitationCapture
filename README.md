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
docker exec -it postgres bash -c "createuser --pwprompt citation_capture_pipeline" # pwd: citation_capture_pipeline
docker exec -it postgres bash -c "createdb citation_capture_pipeline citation_capture_pipeline"
docker exec -it postgres bash -c "psql -c 'GRANT CREATE ON DATABASE citation_capture_pipeline TO citation_capture_pipeline;'"
```

Copy `config.py` to `local_config.py` and modify its content to reflect your system. Then, prepare the database:

```
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
docker exec -it rabbitmq bash -c "rabbitmqctl add_vhost master_pipeline"
docker exec -it rabbitmq bash -c "rabbitmqctl set_permissions -p master_pipeline guest '.*' '.*' '.*'"
```

Copy `config.py` to `local_config.py` and modify its content to reflect your system. The RabbitMQ web interface can be found at [http://localhost:15672](http://localhost:15672)


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

Then you can run the diagnose or an ingestion:

```
python run.py --diagnose
python run.py -r refids_zenodo.dat.20180911
python run.py -r refids_zenodo.dat.20180914
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



