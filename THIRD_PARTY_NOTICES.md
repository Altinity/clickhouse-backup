# Third-Party License Attribution

This file lists third-party Go dependencies used by the
`clickhouse-backup` binary, with their SPDX license identifiers.

Generated with [`go-licenses`](https://github.com/google/go-licenses)
for the `linux/amd64`, `linux/arm64`, `darwin/amd64` and `darwin/arm64` targets:

```bash
for t in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
  GOOS=${t%/*} GOARCH=${t#*/} go-licenses report ./cmd/clickhouse-backup \
    --ignore github.com/Altinity/clickhouse-backup
done
```

Dependencies are not vendored, so license texts are referenced by URL in the
**License URL** column. Packages whose license file contains several licenses
are listed with all of them separated by `/`.
The main project license is MIT; see [`LICENSE`](LICENSE).

## Summary

| License | Packages |
|---------|----------|
| Apache-2.0 | 59 |
| MIT | 39 |
| BSD-3-Clause | 34 |
| Apache-2.0 / BSD-3-Clause | 8 |
| MIT-0 | 6 |
| BSD-2-Clause | 4 |
| MPL-2.0 | 2 |
| Apache-2.0 / BSD-3-Clause / MIT | 1 |
| BSD-0-Clause | 1 |
| BSD-3-Clause / MIT | 1 |
| ISC | 1 |
| Unlicense | 1 |

**Total packages:** 157

## Packages

| Package | License | License URL |
|---------|---------|-------------|
| `cel.dev/expr` | Apache-2.0 | https://github.com/cel-expr/cel-spec/blob/v0.25.3/LICENSE |
| `cloud.google.com/go/auth` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/auth/v0.23.2/auth/LICENSE |
| `cloud.google.com/go/auth/oauth2adapt` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/auth/oauth2adapt/v0.2.8/auth/oauth2adapt/LICENSE |
| `cloud.google.com/go/compute/metadata` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/compute/metadata/v0.9.0/compute/metadata/LICENSE |
| `cloud.google.com/go/iam` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/iam/v1.13.0/iam/LICENSE |
| `cloud.google.com/go/internal` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/v0.123.0/LICENSE |
| `cloud.google.com/go/monitoring` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/monitoring/v1.30.0/monitoring/LICENSE |
| `cloud.google.com/go/storage` | Apache-2.0 | https://github.com/googleapis/google-cloud-go/blob/storage/v1.66.0/storage/LICENSE |
| `github.com/Azure/azure-sdk-for-go/sdk/azcore` | MIT | https://github.com/Azure/azure-sdk-for-go/blob/sdk/azcore/v1.23.1/sdk/azcore/LICENSE.txt |
| `github.com/Azure/azure-sdk-for-go/sdk/azidentity` | MIT | https://github.com/Azure/azure-sdk-for-go/blob/sdk/azidentity/v1.14.1/sdk/azidentity/LICENSE.txt |
| `github.com/Azure/azure-sdk-for-go/sdk/internal` | MIT | https://github.com/Azure/azure-sdk-for-go/blob/sdk/internal/v1.12.0/sdk/internal/LICENSE.txt |
| `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` | MIT | https://github.com/Azure/azure-sdk-for-go/blob/sdk/storage/azblob/v1.8.0/sdk/storage/azblob/LICENSE.txt |
| `github.com/AzureAD/microsoft-authentication-library-for-go/apps` | MIT | https://github.com/AzureAD/microsoft-authentication-library-for-go/blob/v1.9.0/LICENSE |
| `github.com/ClickHouse/ch-go` | Apache-2.0 | https://github.com/ClickHouse/ch-go/blob/v0.74.0/LICENSE |
| `github.com/ClickHouse/clickhouse-go/v2` | Apache-2.0 | https://github.com/ClickHouse/clickhouse-go/blob/v2.48.0/LICENSE |
| `github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp` | Apache-2.0 | https://github.com/GoogleCloudPlatform/opentelemetry-operations-go/blob/detectors/gcp/v1.37.0/detectors/gcp/LICENSE |
| `github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric` | Apache-2.0 | https://github.com/GoogleCloudPlatform/opentelemetry-operations-go/blob/exporter/metric/v0.61.0/exporter/metric/LICENSE |
| `github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping` | Apache-2.0 | https://github.com/GoogleCloudPlatform/opentelemetry-operations-go/blob/internal/resourcemapping/v0.61.0/internal/resourcemapping/LICENSE |
| `github.com/STARRY-S/zip` | BSD-3-Clause | https://github.com/STARRY-S/zip/blob/v0.2.3/LICENSE |
| `github.com/andybalholm/brotli` | MIT | https://github.com/andybalholm/brotli/blob/v1.2.3/LICENSE |
| `github.com/antchfx/xmlquery` | MIT | https://github.com/antchfx/xmlquery/blob/v1.5.1/LICENSE |
| `github.com/antchfx/xpath` | MIT | https://github.com/antchfx/xpath/blob/v1.3.8/LICENSE |
| `github.com/aws/aws-sdk-go-v2` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/v1.45.1/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/aws/protocol/eventstream/v1.7.20/aws/protocol/eventstream/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/config` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/config/v1.33.2/config/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/credentials` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/credentials/v1.20.2/credentials/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/feature/ec2/imds` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/feature/ec2/imds/v1.19.1/feature/ec2/imds/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/feature/s3/transfermanager/v0.4.2/feature/s3/transfermanager/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/internal/configsources` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/internal/configsources/v1.5.1/internal/configsources/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/internal/endpoints/v2` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/internal/endpoints/v2.8.1/internal/endpoints/v2/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/internal/sync/singleflight` | BSD-3-Clause | https://github.com/aws/aws-sdk-go-v2/blob/v1.45.1/internal/sync/singleflight/LICENSE |
| `github.com/aws/aws-sdk-go-v2/internal/v4a` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/internal/v4a/v1.5.1/internal/v4a/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/internal/accept-encoding/v1.13.19/service/internal/accept-encoding/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/internal/checksum` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/internal/checksum/v1.11.1/service/internal/checksum/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/internal/presigned-url` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/internal/presigned-url/v1.14.1/service/internal/presigned-url/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/internal/s3shared` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/internal/s3shared/v1.20.1/service/internal/s3shared/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/s3` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/s3/v1.110.0/service/s3/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/signin` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/signin/v1.8.0/service/signin/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/sso` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/sso/v1.36.0/service/sso/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/ssooidc` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/ssooidc/v1.41.0/service/ssooidc/LICENSE.txt |
| `github.com/aws/aws-sdk-go-v2/service/sts` | Apache-2.0 | https://github.com/aws/aws-sdk-go-v2/blob/service/sts/v1.48.0/service/sts/LICENSE.txt |
| `github.com/aws/smithy-go` | Apache-2.0 | https://github.com/aws/smithy-go/blob/v1.28.1/LICENSE |
| `github.com/aws/smithy-go/internal/sync/singleflight` | BSD-3-Clause | https://github.com/aws/smithy-go/blob/v1.28.1/internal/sync/singleflight/LICENSE |
| `github.com/beorn7/perks/quantile` | MIT | https://github.com/beorn7/perks/blob/v1.0.1/LICENSE |
| `github.com/bodgit/plumbing` | BSD-3-Clause | https://github.com/bodgit/plumbing/blob/v1.3.0/LICENSE |
| `github.com/bodgit/sevenzip` | BSD-3-Clause | https://github.com/bodgit/sevenzip/blob/v1.6.5/LICENSE |
| `github.com/bodgit/windows` | BSD-3-Clause | https://github.com/bodgit/windows/blob/v1.0.1/LICENSE |
| `github.com/buger/jsonparser` | MIT | https://github.com/buger/jsonparser/blob/v1.6.1/LICENSE |
| `github.com/cespare/xxhash/v2` | MIT | https://github.com/cespare/xxhash/blob/v2.3.0/LICENSE.txt |
| `github.com/clbanning/mxj` | BSD-3-Clause / MIT | https://github.com/clbanning/mxj/blob/v1.8.4/LICENSE |
| `github.com/cncf/xds/go` | Apache-2.0 | https://github.com/cncf/xds/blob/dba9d589def2/go/LICENSE |
| `github.com/djherbis/buffer` | MIT | https://github.com/djherbis/buffer/blob/v1.2.0/LICENSE.txt |
| `github.com/djherbis/nio/v3` | MIT | https://github.com/djherbis/nio/blob/v3.0.1/LICENSE.txt |
| `github.com/dsnet/compress` | BSD-3-Clause | https://github.com/dsnet/compress/blob/39efe44ab707/LICENSE.md |
| `github.com/eapache/go-resiliency/retrier` | MIT | https://github.com/eapache/go-resiliency/blob/v1.7.0/LICENSE |
| `github.com/envoyproxy/go-control-plane/envoy` | Apache-2.0 | https://github.com/envoyproxy/go-control-plane/blob/envoy/v1.39.0/envoy/LICENSE |
| `github.com/envoyproxy/protoc-gen-validate/validate` | Apache-2.0 | https://github.com/envoyproxy/protoc-gen-validate/blob/v1.3.3/LICENSE |
| `github.com/felixge/httpsnoop` | MIT | https://github.com/felixge/httpsnoop/blob/v1.1.0/LICENSE.txt |
| `github.com/go-faster/city` | MIT | https://github.com/go-faster/city/blob/v1.0.1/LICENSE |
| `github.com/go-faster/errors` | BSD-3-Clause | https://github.com/go-faster/errors/blob/v0.8.0/LICENSE |
| `github.com/go-jose/go-jose/v4` | Apache-2.0 | https://github.com/go-jose/go-jose/blob/v4.1.4/LICENSE |
| `github.com/go-jose/go-jose/v4/json` | BSD-3-Clause | https://github.com/go-jose/go-jose/blob/v4.1.4/json/LICENSE |
| `github.com/go-logr/logr` | Apache-2.0 | https://github.com/go-logr/logr/blob/v1.4.4/LICENSE |
| `github.com/go-logr/stdr` | Apache-2.0 | https://github.com/go-logr/stdr/blob/v1.2.2/LICENSE |
| `github.com/go-zookeeper/zk` | BSD-3-Clause | https://github.com/go-zookeeper/zk/blob/v1.0.4/LICENSE |
| `github.com/gocarina/gocsv` | MIT | https://github.com/gocarina/gocsv/blob/1713ebc4797a/LICENSE |
| `github.com/golang-jwt/jwt/v5` | MIT | https://github.com/golang-jwt/jwt/blob/v5.3.1/LICENSE |
| `github.com/golang/groupcache/lru` | Apache-2.0 | https://github.com/golang/groupcache/blob/2c02b8208cf8/LICENSE |
| `github.com/google/go-querystring/query` | BSD-3-Clause | https://github.com/google/go-querystring/blob/v1.2.0/LICENSE |
| `github.com/google/s2a-go` | Apache-2.0 | https://github.com/google/s2a-go/blob/v0.1.9/LICENSE.md |
| `github.com/google/shlex` | Apache-2.0 | https://github.com/google/shlex/blob/e7afc7fbc510/COPYING |
| `github.com/google/uuid` | BSD-3-Clause | https://github.com/google/uuid/blob/v1.6.0/LICENSE |
| `github.com/googleapis/enterprise-certificate-proxy/client` | Apache-2.0 | https://github.com/googleapis/enterprise-certificate-proxy/blob/v0.3.21/LICENSE |
| `github.com/googleapis/gax-go/v2` | BSD-3-Clause | https://github.com/googleapis/gax-go/blob/v2.24.0/v2/LICENSE |
| `github.com/gorilla/mux` | BSD-3-Clause | https://github.com/gorilla/mux/blob/v1.8.1/LICENSE |
| `github.com/hashicorp/golang-lru/v2` | MPL-2.0 | https://github.com/hashicorp/golang-lru/blob/v2.0.7/LICENSE |
| `github.com/hashicorp/golang-lru/v2/simplelru` | BSD-3-Clause | https://github.com/hashicorp/golang-lru/blob/v2.0.7/simplelru/LICENSE_list |
| `github.com/jlaffaye/ftp` | ISC | https://github.com/jlaffaye/ftp/blob/v0.2.4/LICENSE |
| `github.com/jolestar/go-commons-pool/v2` | Apache-2.0 | https://github.com/jolestar/go-commons-pool/blob/v2.1.2/LICENSE |
| `github.com/kelseyhightower/envconfig` | MIT | https://github.com/kelseyhightower/envconfig/blob/v1.4.0/LICENSE |
| `github.com/klauspost/compress` | Apache-2.0 / BSD-3-Clause / MIT | https://github.com/klauspost/compress/blob/v1.19.2/LICENSE |
| `github.com/klauspost/compress/internal/snapref` | BSD-3-Clause | https://github.com/klauspost/compress/blob/v1.19.2/internal/snapref/LICENSE |
| `github.com/klauspost/compress/s2` | BSD-3-Clause | https://github.com/klauspost/compress/blob/v1.19.2/s2/LICENSE |
| `github.com/klauspost/compress/zstd/internal/xxhash` | MIT | https://github.com/klauspost/compress/blob/v1.19.2/zstd/internal/xxhash/LICENSE.txt |
| `github.com/klauspost/pgzip` | MIT | https://github.com/klauspost/pgzip/blob/v1.2.6/LICENSE |
| `github.com/kr/fs` | BSD-3-Clause | https://github.com/kr/fs/blob/v0.1.0/LICENSE |
| `github.com/kylelemons/godebug` | Apache-2.0 | https://github.com/kylelemons/godebug/blob/v1.1.0/LICENSE |
| `github.com/mattn/go-colorable` | MIT | https://github.com/mattn/go-colorable/blob/v0.1.15/LICENSE |
| `github.com/mattn/go-isatty` | MIT | https://github.com/mattn/go-isatty/blob/v0.0.24/LICENSE |
| `github.com/mattn/go-shellwords` | MIT | https://github.com/mattn/go-shellwords/blob/v1.0.14/LICENSE |
| `github.com/mholt/archives` | MIT | https://github.com/mholt/archives/blob/v0.1.5/LICENSE |
| `github.com/mikelolasagasti/xz` | BSD-0-Clause | https://github.com/mikelolasagasti/xz/blob/v1.0.1/LICENSE |
| `github.com/minio/minlz` | Apache-2.0 | https://github.com/minio/minlz/blob/v1.2.0/LICENSE |
| `github.com/mitchellh/mapstructure` | MIT | https://github.com/mitchellh/mapstructure/blob/v1.5.0/LICENSE |
| `github.com/mozillazg/go-httpheader` | MIT | https://github.com/mozillazg/go-httpheader/blob/v0.4.0/LICENSE |
| `github.com/munnerz/goautoneg` | BSD-3-Clause | https://github.com/munnerz/goautoneg/blob/a7dc8b61c822/LICENSE |
| `github.com/nwaples/rardecode/v2` | BSD-2-Clause | https://github.com/nwaples/rardecode/blob/v2.4.1/LICENSE |
| `github.com/otiai10/copy` | MIT | https://github.com/otiai10/copy/blob/v1.14.1/LICENSE |
| `github.com/paulmach/orb` | MIT | https://github.com/paulmach/orb/blob/v0.13.0/LICENSE.md |
| `github.com/pierrec/lz4/v4` | BSD-3-Clause | https://github.com/pierrec/lz4/blob/v4.1.29/LICENSE |
| `github.com/pkg/browser` | BSD-2-Clause | https://github.com/pkg/browser/blob/5ac0b6a4141c/LICENSE |
| `github.com/pkg/errors` | BSD-2-Clause | https://github.com/pkg/errors/blob/v0.9.1/LICENSE |
| `github.com/pkg/sftp` | BSD-2-Clause | https://github.com/pkg/sftp/blob/v1.13.11/LICENSE |
| `github.com/prometheus/client_golang/internal/github.com/golang/gddo/httputil` | BSD-3-Clause | https://github.com/prometheus/client_golang/blob/v1.24.1/internal/github.com/golang/gddo/LICENSE |
| `github.com/prometheus/client_golang/prometheus` | Apache-2.0 | https://github.com/prometheus/client_golang/blob/v1.24.1/LICENSE |
| `github.com/prometheus/client_model/go` | Apache-2.0 | https://github.com/prometheus/client_model/blob/v0.6.3/LICENSE |
| `github.com/prometheus/common` | Apache-2.0 | https://github.com/prometheus/common/blob/v0.71.0/LICENSE |
| `github.com/prometheus/procfs` | Apache-2.0 | https://github.com/prometheus/procfs/blob/v0.22.0/LICENSE |
| `github.com/puzpuzpuz/xsync` | MIT | https://github.com/puzpuzpuz/xsync/blob/v1.5.2/LICENSE |
| `github.com/ricochet2200/go-disk-usage/du` | Unlicense | https://github.com/ricochet2200/go-disk-usage/blob/ac9918953285/du/LICENSE |
| `github.com/robfig/cron/v3` | MIT | https://github.com/robfig/cron/blob/v3.0.1/LICENSE |
| `github.com/rs/zerolog` | MIT | https://github.com/rs/zerolog/blob/v1.35.1/LICENSE |
| `github.com/segmentio/asm/bswap` | MIT-0 | https://github.com/segmentio/asm/blob/v1.2.1/LICENSE |
| `github.com/segmentio/asm/cpu` | MIT-0 | https://github.com/segmentio/asm/blob/v1.2.1/LICENSE |
| `github.com/segmentio/asm/cpu/arm` | MIT-0 | https://github.com/segmentio/asm/blob/v1.2.1/LICENSE |
| `github.com/segmentio/asm/cpu/arm64` | MIT-0 | https://github.com/segmentio/asm/blob/v1.2.1/LICENSE |
| `github.com/segmentio/asm/cpu/cpuid` | MIT-0 | https://github.com/segmentio/asm/blob/v1.2.1/LICENSE |
| `github.com/segmentio/asm/cpu/x86` | MIT-0 | https://github.com/segmentio/asm/blob/v1.2.1/LICENSE |
| `github.com/shirou/gopsutil/v3` | BSD-3-Clause | https://github.com/shirou/gopsutil/blob/v3.24.5/LICENSE |
| `github.com/shoenig/go-m1cpu` | MPL-2.0 | https://github.com/shoenig/go-m1cpu/blob/v0.2.2/LICENSE |
| `github.com/shopspring/decimal` | MIT | https://github.com/shopspring/decimal/blob/v1.4.0/LICENSE |
| `github.com/sorairolake/lzip-go` | Apache-2.0 | https://github.com/sorairolake/lzip-go/blob/v0.3.8/LICENSE-APACHE |
| `github.com/spf13/afero` | Apache-2.0 | https://github.com/spf13/afero/blob/v1.15.0/LICENSE.txt |
| `github.com/spiffe/go-spiffe/v2` | Apache-2.0 | https://github.com/spiffe/go-spiffe/blob/v2.8.1/LICENSE |
| `github.com/stangelandcl/ppmd` | MIT | https://github.com/stangelandcl/ppmd/blob/v0.1.1/LICENSE |
| `github.com/tencentyun/cos-go-sdk-v5` | MIT | https://github.com/tencentyun/cos-go-sdk-v5/blob/v0.7.75/LICENSE |
| `github.com/tklauser/go-sysconf` | BSD-3-Clause | https://github.com/tklauser/go-sysconf/blob/v0.4.0/LICENSE |
| `github.com/tklauser/numcpus` | Apache-2.0 | https://github.com/tklauser/numcpus/blob/v0.12.0/LICENSE |
| `github.com/ulikunitz/xz` | BSD-3-Clause | https://github.com/ulikunitz/xz/blob/v0.5.16/LICENSE |
| `github.com/urfave/cli/v3` | MIT | https://github.com/urfave/cli/blob/v3.11.0/LICENSE |
| `github.com/yargevad/filepathx` | MIT | https://github.com/yargevad/filepathx/blob/v1.0.0/LICENSE |
| `go.etcd.io/bbolt` | MIT | https://github.com/etcd-io/bbolt/blob/v1.5.0/LICENSE |
| `go.opentelemetry.io/auto/sdk` | Apache-2.0 | https://github.com/open-telemetry/opentelemetry-go-instrumentation/blob/sdk/v1.2.1/sdk/LICENSE |
| `go.opentelemetry.io/contrib/detectors/gcp` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go-contrib/blob/detectors/gcp/v1.46.0/detectors/gcp/LICENSE |
| `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go-contrib/blob/instrumentation/google.golang.org/grpc/otelgrpc/v0.71.0/instrumentation/google.golang.org/grpc/otelgrpc/LICENSE |
| `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go-contrib/blob/instrumentation/net/http/otelhttp/v0.71.0/instrumentation/net/http/otelhttp/LICENSE |
| `go.opentelemetry.io/otel` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go/blob/v1.46.0/LICENSE |
| `go.opentelemetry.io/otel/metric` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go/blob/metric/v1.46.0/metric/LICENSE |
| `go.opentelemetry.io/otel/sdk` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go/blob/sdk/v1.46.0/sdk/LICENSE |
| `go.opentelemetry.io/otel/sdk/metric` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go/blob/sdk/metric/v1.46.0/sdk/metric/LICENSE |
| `go.opentelemetry.io/otel/trace` | Apache-2.0 / BSD-3-Clause | https://github.com/open-telemetry/opentelemetry-go/blob/trace/v1.46.0/trace/LICENSE |
| `go4.org/readerutil` | Apache-2.0 | https://github.com/go4org/go4/blob/a5071408f32f/LICENSE |
| `golang.org/x/crypto` | BSD-3-Clause | https://cs.opensource.google/go/x/crypto/+/v0.55.0:LICENSE |
| `golang.org/x/net` | BSD-3-Clause | https://cs.opensource.google/go/x/net/+/v0.58.0:LICENSE |
| `golang.org/x/oauth2` | BSD-3-Clause | https://cs.opensource.google/go/x/oauth2/+/v0.36.0:LICENSE |
| `golang.org/x/sync` | BSD-3-Clause | https://cs.opensource.google/go/x/sync/+/v0.22.0:LICENSE |
| `golang.org/x/sys` | BSD-3-Clause | https://cs.opensource.google/go/x/sys/+/v0.47.0:LICENSE |
| `golang.org/x/text` | BSD-3-Clause | https://cs.opensource.google/go/x/text/+/v0.41.0:LICENSE |
| `golang.org/x/time/rate` | BSD-3-Clause | https://cs.opensource.google/go/x/time/+/v0.15.0:LICENSE |
| `google.golang.org/api` | BSD-3-Clause | https://github.com/googleapis/google-api-go-client/blob/v0.296.0/LICENSE |
| `google.golang.org/api/internal/third_party/uritemplates` | BSD-3-Clause | https://github.com/googleapis/google-api-go-client/blob/v0.296.0/internal/third_party/uritemplates/LICENSE |
| `google.golang.org/genproto/googleapis/api` | Apache-2.0 | https://github.com/googleapis/go-genproto/blob/18b4a7587f8a/googleapis/api/LICENSE |
| `google.golang.org/genproto/googleapis/rpc` | Apache-2.0 | https://github.com/googleapis/go-genproto/blob/18b4a7587f8a/googleapis/rpc/LICENSE |
| `google.golang.org/genproto/googleapis/type` | Apache-2.0 | https://github.com/googleapis/go-genproto/blob/18b4a7587f8a/LICENSE |
| `google.golang.org/grpc` | Apache-2.0 | https://github.com/grpc/grpc-go/blob/v1.83.2/LICENSE |
| `google.golang.org/protobuf` | BSD-3-Clause | https://github.com/protocolbuffers/protobuf-go/blob/v1.36.12/LICENSE |
| `gopkg.in/yaml.v3` | MIT | https://github.com/go-yaml/yaml/blob/v3.0.1/LICENSE |
