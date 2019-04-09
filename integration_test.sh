#/bin/bash
set -x
set -e

GOOS=linux GOARCH=amd64 go build
docker-compose -f integration-test/docker-compose.yml down
docker-compose -f integration-test/docker-compose.yml up -d
go test -tags=integration -v
# docker-compose -f integration-test/docker-compose.yml down
