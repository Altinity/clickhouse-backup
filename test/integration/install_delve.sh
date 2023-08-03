# golang
rm -rf /etc/apt/sources.list.d/clickhouse.list
apt-get update && apt-get install -y software-properties-common
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E
add-apt-repository -y ppa:longsleep/golang-backports
apt-get update
apt-get install -y golang-1.20

mkdir -p ~/go/
export GOPATH=~/go/
grep -q -F 'export GOPATH=$GOPATH' ~/.bashrc  || echo "export GOPATH=$GOPATH" >> ~/.bashrc
grep -q -F 'export GOPATH=$GOPATH' /root/.bashrc         || echo "export GOPATH=$GOPATH" >> /root/.bashrc
export GOROOT=/usr/lib/go-1.20/
grep -q -F 'export GOROOT=$GOROOT' ~/.bashrc  || echo "export GOROOT=$GOROOT" >> ~/.bashrc
grep -q -F 'export GOROOT=$GOROOT' /root/.bashrc || echo "export GOROOT=$GOROOT" >> /root/.bashrc
ln -nsfv /usr/lib/go-1.20/bin/go /usr/bin/go

CGO_ENABLED=0 GO111MODULE=on go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest

# GO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags osusergo,netgo -gcflags "all=-N -l" -ldflags "-extldflags '-static' -X 'main.version=debug'" -o build/linux/amd64/clickhouse-backup ./cmd/clickhouse-backup
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-server/config.d/ch-backup.yaml upload debug_upload --table
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- download test_rbac_backup
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- download keep_remote_backup_4
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- server
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- restore --schema test_backup_8007633179464680930
# CLICKHOUSE_TIMEOUT=3m CLICKHOUSE_DEBUG=true LOG_LEVEL=debug /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- watch --watch-interval=1m --full-interval=2m
# LOG_LEVEL=debug /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- restore --data --restore-database-mapping database1:database2 --tables database1.* test_restore_database_mapping
# S3_DEBUG=true /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- upload test_rbac_backup
# run integration_test.go under debug, run from host OS not inside docker
# go test -timeout 30m -failfast -tags=integration -run "TestIntegrationEmbedded" -v ./test/integration/integration_test.go -c -o ./test/integration/integration_test
# sudo -H bash -c 'export CLICKHOUSE_IMAGE=clickhouse/clickhouse-server; export COMPOSE_FILE=docker-compose_advanced.yml; export CLICKHOUSE_VERSION=head; cd ./test/integration/; /root/go/bin/dlv --listen=127.0.0.1:40002 --headless=true --api-version=2 --accept-multiclient exec ./integration_test -- -test.timeout 30m -test.failfast -test.run "TestIntegrationEmbedded"'




