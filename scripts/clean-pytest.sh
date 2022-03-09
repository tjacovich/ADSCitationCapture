#!/usr/bin/env bash
set -x
docker-compose rm -fsv
docker rmi python:3.8.12
