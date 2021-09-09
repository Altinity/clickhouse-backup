module github.com/AlexAkulov/clickhouse-backup

require (
	cloud.google.com/go/storage v1.16.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.10.1-0.20200807102407-24fe552e0870
	github.com/ClickHouse/clickhouse-go v1.4.7
	github.com/apex/log v1.9.0
	github.com/aws/aws-sdk-go v1.40.31
	github.com/djherbis/buffer v1.2.0
	github.com/go-logfmt/logfmt v0.5.1
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.1.2
	github.com/gorilla/mux v1.8.0
	github.com/jlaffaye/ftp v0.0.0-20210307004419-5d4190119067
	github.com/jmoiron/sqlx v1.3.4
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mattn/go-shellwords v1.0.12
	github.com/mholt/archiver v1.1.3-0.20190812163345-2d1449806793
	github.com/mholt/archiver/v3 v3.5.0
	github.com/otiai10/copy v1.6.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.2
	github.com/prometheus/client_golang v1.11.0
	github.com/stretchr/testify v1.7.0
	github.com/tencentyun/cos-go-sdk-v5 v0.7.30
	github.com/urfave/cli v1.22.5
	github.com/yargevad/filepathx v1.0.0
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5
	golang.org/x/mod v0.5.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.54.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28
	gopkg.in/djherbis/nio.v2 v2.0.3
	gopkg.in/yaml.v2 v2.4.0
)

go 1.16
