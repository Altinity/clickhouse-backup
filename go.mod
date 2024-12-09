module github.com/Altinity/clickhouse-backup/v2

// freeze version https://github.com/mholt/archiver/issues/428
replace github.com/mholt/archiver/v4 => github.com/mholt/archiver/v4 v4.0.0-alpha.8

require (
	cloud.google.com/go/storage v1.48.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/Azure/go-autorest/autorest v0.11.29
	github.com/Azure/go-autorest/autorest/adal v0.9.24
	github.com/ClickHouse/clickhouse-go/v2 v2.30.0
	github.com/antchfx/xmlquery v1.4.2
	github.com/aws/aws-sdk-go-v2 v1.32.6
	github.com/aws/aws-sdk-go-v2/config v1.28.6
	github.com/aws/aws-sdk-go-v2/credentials v1.17.47
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.43
	github.com/aws/aws-sdk-go-v2/service/s3 v1.71.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.2
	github.com/aws/smithy-go v1.22.1
	github.com/djherbis/buffer v1.2.0
	github.com/djherbis/nio/v3 v3.0.1
	github.com/eapache/go-resiliency v1.7.0
	github.com/go-zookeeper/zk v1.0.4
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/jlaffaye/ftp v0.2.0
	github.com/jolestar/go-commons-pool/v2 v2.1.2
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.17.11
	github.com/mattn/go-shellwords v1.0.12
	github.com/mholt/archiver/v4 v4.0.0-alpha.9
	github.com/otiai10/copy v1.14.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.7
	github.com/prometheus/client_golang v1.20.5
	github.com/puzpuzpuz/xsync v1.5.2
	github.com/ricochet2200/go-disk-usage/du v0.0.0-20210707232629-ac9918953285
	github.com/rs/zerolog v1.33.0
	github.com/stretchr/testify v1.10.0
	github.com/tencentyun/cos-go-sdk-v5 v0.7.59
	github.com/urfave/cli v1.22.16
	github.com/xyproto/gionice v1.3.0
	github.com/yargevad/filepathx v1.0.0
	go.etcd.io/bbolt v1.3.11
	golang.org/x/crypto v0.30.0
	golang.org/x/mod v0.18.0
	golang.org/x/sync v0.10.0
	golang.org/x/text v0.21.0
	google.golang.org/api v0.210.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cel.dev/expr v0.19.1 // indirect
	cloud.google.com/go v0.116.0 // indirect
	cloud.google.com/go/auth v0.12.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.6 // indirect
	cloud.google.com/go/compute/metadata v0.5.2 // indirect
	cloud.google.com/go/iam v1.3.0 // indirect
	cloud.google.com/go/longrunning v0.6.3 // indirect
	cloud.google.com/go/monitoring v1.22.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/mocks v0.4.2 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/ClickHouse/ch-go v0.63.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.25.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.49.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.49.0 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/antchfx/xpath v1.3.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.6 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bodgit/plumbing v1.3.0 // indirect
	github.com/bodgit/sevenzip v1.6.0 // indirect
	github.com/bodgit/windows v1.0.1 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/cncf/xds/go v0.0.0-20240905190251-b4127c9b8d78 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dsnet/compress v0.0.2-0.20230904184137-39efe44ab707 // indirect
	github.com/envoyproxy/go-control-plane v0.13.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.1 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.14.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.12 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mozillazg/go-httpheader v0.4.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nwaples/rardecode/v2 v2.0.0-beta.4 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.60.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/therootcompany/xz v1.0.1 // indirect
	github.com/ulikunitz/xz v0.5.12 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.32.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.57.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.57.0 // indirect
	go.opentelemetry.io/otel v1.32.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/sdk v1.32.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/trace v1.32.0 // indirect
	go4.org v0.0.0-20230225012048-214862532bf5 // indirect
	golang.org/x/net v0.32.0 // indirect
	golang.org/x/oauth2 v0.24.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20241206012308-a4fef0638583 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241206012308-a4fef0638583 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241206012308-a4fef0638583 // indirect
	google.golang.org/grpc v1.68.1 // indirect
	google.golang.org/grpc/stats/opentelemetry v0.0.0-20241028142157-ada6787961b3 // indirect
	google.golang.org/protobuf v1.35.2 // indirect
)

go 1.23
