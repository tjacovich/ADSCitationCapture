[![Waffle.io - Columns and their card count](https://badge.waffle.io/adsabs/ADSCitationCapture.svg?columns=all)](https://waffle.io/adsabs/ADSCitationCapture)
[![Build Status](https://travis-ci.org/adsabs/ADSCitationCapture.svg?branch=master)](https://travis-ci.org/adsabs/ADSCitationCapture)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSCitationCapture/badge.svg)](https://coveralls.io/r/adsabs/ADSCitationCapture)
[![Code Climate](https://codeclimate.com/github/adsabs/ADSCitationCapture/badges/gpa.svg)](https://codeclimate.com/github/adsabs/ADSCitationCapture)
[![Issue Count](https://codeclimate.com/github/adsabs/ADSCitationCapture/badges/issue_count.svg)](https://codeclimate.com/github/adsabs/ADSCitationCapture)
# ADSCitationCapture

Copy ```config.py``` to ```local_config.py``` and modify its content to reflect your system.

The project incorporates Alembic to manage PostgreSQL migrations based on SQLAlchemy models. It has already been initialized with:

```
alembic init alembic
```

And the file ```alembic/env.py``` was modified to import SQLAlchemy models and use the database connection coming from the ```local_config.py``` file. As a reminder, some key commands from [Alembic tutorial](http://alembic.zzzcomputing.com/en/latest/):

```
alembic revision -m "baseline" --autogenerate
alembic current
alembic history
alembic upgrade head
alembic downgrade -1
alembic upgrade +1
```

Some key commands to inspect the database from [the PostgreSQL cheatsheet](https://gist.github.com/Kartones/dd3ff5ec5ea238d4c546):

```
sudo apt-get install postgresql-client --no-install-recommends
psql -h localhost database user
\dt public.*
\d public.alembic_version
```

Running unit tests is done via:

```
py.test
```



# Build sample refids.dat

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

