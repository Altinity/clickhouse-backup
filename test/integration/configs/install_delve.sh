# golang
rm -rf /etc/apt/sources.list.d/clickhouse.list
apt-get update && apt-get install -y software-properties-common
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E
add-apt-repository -y ppa:longsleep/golang-backports
apt-get update
apt-get install -y golang-1.25

mkdir -p ~/go/
export GOPATH=~/go/
grep -q -F 'export GOPATH=$GOPATH' ~/.bashrc  || echo "export GOPATH=$GOPATH" >> ~/.bashrc
grep -q -F 'export GOPATH=$GOPATH' /root/.bashrc         || echo "export GOPATH=$GOPATH" >> /root/.bashrc
export GOROOT=/usr/lib/go-1.25/
grep -q -F 'export GOROOT=$GOROOT' ~/.bashrc  || echo "export GOROOT=$GOROOT" >> ~/.bashrc
grep -q -F 'export GOROOT=$GOROOT' /root/.bashrc || echo "export GOROOT=$GOROOT" >> /root/.bashrc
ln -nsfv /usr/lib/go-1.25/bin/go /usr/bin/go

CGO_ENABLED=0 GO111MODULE=on go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest

# GO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags osusergo,netgo -gcflags "all=-N -l" -ldflags "-extldflags '-static' -X 'main.version=debug'" -o build/linux/amd64/clickhouse-backup ./cmd/clickhouse-backup
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-azblob.yml download --partitions=test_partitions_TestIntegrationAzure.t?:(0,'2022-01-02'),(0,'2022-01-03') full_backup_3691696362844433277
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-azblob.yml restore --schema TestIntegrationAzure_full_6516689450475708573
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-server/config.d/ch-backup.yaml upload debug_upload --table
# USE_RESUMABLE_STATE=0 CLICKHOUSE_SKIP_TABLES=*.test_memory /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-s3.yml download test_skip_full_backup
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- download test_rbac_backup
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- download keep_remote_backup_4
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- server
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- restore --schema test_backup_8007633179464680930
# CLICKHOUSE_TIMEOUT=3m CLICKHOUSE_DEBUG=true LOG_LEVEL=debug /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- watch --watch-interval=1m --full-interval=2m
# LOG_LEVEL=debug /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- restore --data --restore-database-mapping database1:database2 --tables database1.* test_restore_database_mapping
# S3_DEBUG=true /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- upload test_rbac_backup
# S3_DEBUG=true /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-s3.yml delete remote full_backup_339504125792808941
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-custom-kopia.yml list remote
# run integration_test.go under debug, run from host OS not inside docker
# go test -timeout 30m -failfast -tags=integration -run "TestIntegrationEmbedded" -v ./test/integration/integration_test.go -c -o ./test/integration/integration_test
# sudo -H bash -c 'export CLICKHOUSE_IMAGE=clickhouse/clickhouse-server; export COMPOSE_FILE=docker-compose_advanced.yml; export CLICKHOUSE_VERSION=head; cd ./test/integration/; /root/go/bin/dlv --listen=127.0.0.1:40002 --headless=true --api-version=2 --accept-multiclient exec ./integration_test -- -test.timeout 30m -test.failfast -test.run "TestIntegrationEmbedded"'
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-s3.yml download --partitions="test_partitions_TestIntegrationS3.t?:(0,'2022-01-02'),(0,'2022-01-03')" full_backup_5643339940028285692
# EMBEDDED_S3_COMPRESSION_FORMAT=zstd CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3-embedded.yml /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- upload TestIntegrationEmbedded_full_5990789107828261693
# S3_COMPRESSION_FORMAT=zstd CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- upload --resume TestIntegrationS3_full_8761350380051000966
# /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-backup/config-s3.yml restore --tables default.test_replica_wrong_path test_wrong_path
# CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3.yml LOG_LEVEL=debug /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- server --watch
# rootless
# cd /tmp/; wget https://go.dev/dl/go1.25.5.linux-arm64.tar.gz; tar -xzf go1.25.5.linux-arm64.tar.gz
# export PATH="/tmp/go/bin:$PATH"
# CGO_ENABLED=0 GO111MODULE=on go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest
# ~/go/bin/dlv attach 1 --listen=:40001 --headless=true --api-version=2 --accept-multiclient
# CLICKHOUSE_BACKUP_CONFIG=/etc/clickhouse-backup/config-s3-embedded.yml /root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup -- restore_remote --rm --rbac test_rbac_backup_with_data
