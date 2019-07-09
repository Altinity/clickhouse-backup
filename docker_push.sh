#!/bin/bash

echo "${DOCKER_PASSWORD}" | docker login -u alexakulov --password-stdin

if [ "$1" == "release" ]; then
    docker push "alexakulov/clickhouse-backup:${TRAVIS_TAG//v}"
else
    docker push "alexakulov/clickhouse-backup:latest"
fi
