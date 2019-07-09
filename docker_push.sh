#!/bin/bash

echo "${DOCKER_PASSWORD}" | docker login -u alexakulov --password-stdin

docker push "alexakulov/clickhouse-backup:latest"

if [ "$1" == "release" ]; then
    docker tag "alexakulov/clickhouse-backup:latest" "alexakulov/clickhouse-backup:${TRAVIS_TAG//v}"
    docker push "alexakulov/clickhouse-backup:${TRAVIS_TAG//v}"
fi
