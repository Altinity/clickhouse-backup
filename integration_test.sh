#/bin/bash
set -x
set -e

docker-compose -f integration-test/docker-compose.yml down
docker-compose -f integration-test/docker-compose.yml up --detach
go test -tags integration
