#!/usr/bin/env bash
set -e
psql "postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$POSTGRES_DB?sslmode=disable" <<-EOSQL
    CREATE USER citation_capture_pipeline WITH ENCRYPTED PASSWORD 'citation_capture_pipeline';
    CREATE DATABASE citation_capture_pipeline;
    CREATE DATABASE citation_capture_pipeline_test;
    GRANT ALL PRIVILEGES ON DATABASE citation_capture_pipeline TO citation_capture_pipeline;
    GRANT ALL PRIVILEGES ON DATABASE citation_capture_pipeline_test TO citation_capture_pipeline;
EOSQL
