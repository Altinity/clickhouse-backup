#!/bin/bash

echo "${DOCKER_PASSWORD}" | docker login -u alexakulov --password-stdin
docker push alexakulov/clickhouse-backup
