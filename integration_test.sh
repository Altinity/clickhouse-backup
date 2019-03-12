#/bin/bash
set -x
set -e

docker-compose -f integration-test/docker-compose.yml up -d --force-recreate
go test -tags integration
# docker-compose -f integration-test/docker-compose.yml down