#!/usr/bin/env bash
set -x
docker-compose rm -fsv
docker rmi pytest_citation_capture_pipeline:latest
