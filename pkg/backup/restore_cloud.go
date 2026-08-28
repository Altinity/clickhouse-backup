package backup

import (
	"context"
	"encoding/xml"
	stderrors "errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/partition"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// RestoreCloud restores a ClickHouse Cloud native backup (created by `BACKUP ... TO S3 / AzureBlobStorage`
// with proprietary Shared engines) onto an OSS / self-managed ClickHouse:
// reads the `.backup` manifest, resolves checksum-named metadata blobs, rewrites `Shared` database
// engine to `Atomic` and `Shared*MergeTree` to `Replicated*MergeTree`, applies the DDL and runs
// `RESTORE TABLE ... FROM S3(...) / AzureBlobStorage(...)` with allow_different_database_def/allow_different_table_def.
// https://github.com/Altinity/clickhouse-backup/issues/1508

const cloudManifestDefaultPrefixLength = 3

// cloudSkipDatabases - system databases never restored from a ClickHouse Cloud backup
var cloudSkipDatabases = map[string]struct{}{
	"system":                         {},
	"information_schema":             {},
	"INFORMATION_SCHEMA":             {},
	"_temporary_and_external_tables": {},
}

// longer names first so SharedMergeTree does not eat SharedReplacingMergeTree
var sharedMergeTreeRE = regexp.MustCompile(`\bShared(VersionedCollapsing|Replacing|Aggregating|Summing|Collapsing|Graphite|Coalescing|)MergeTree\b`)

// database engine Shared, but not SharedMergeTree / SharedSet / etc. (`\b` rejects a following word char)
var sharedDatabaseEngineRE = regexp.MustCompile(`(?i)(ENGINE\s*=\s*)Shared\b`)

// Cloud Shared* often has no zk args, OSS Replicated* needs them;
// the optional trailing `(` group replaces RE2-unsupported negative lookahead `(?!\s*\()`
var replicatedMergeTreeArgsRE = regexp.MustCompile(`(?i)ENGINE\s*=\s*Replicated(?:VersionedCollapsing|Replacing|Aggregating|Summing|Collapsing|Graphite|Coalescing)?MergeTree\b(\s*\()?`)

var cloudIfNotExistsRE = regexp.MustCompile(`(?i)^\s*CREATE\s+(DATABASE|TABLE|VIEW|MATERIALIZED\s+VIEW|DICTIONARY)\s+IF\s+NOT\s+EXISTS\b`)

// `BACKUP ... ON CLUSTER` stores every file under shards/<shard_num>/replicas/<replica_num>/
var cloudShardPrefixRE = regexp.MustCompile(`^shards/(\d+)/replicas/\d+/`)

// CREATE <kind> [IF NOT EXISTS] <name> [UUID '...'] - the position after which ON CLUSTER is injected
var cloudCreateHeaderRE = regexp.MustCompile("(?i)^\\s*CREATE\\s+(?:DATABASE|DICTIONARY|TABLE|(?:MATERIALIZED\\s+)?VIEW)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:`[^`]+`|\\w+)(?:\\s*\\.\\s*(?:`[^`]+`|\\w+))?(?:\\s+UUID\\s+'[^']+')?")
var cloudCreateKindRE = regexp.MustCompile(`(?i)^\s*CREATE\s+(DATABASE|MATERIALIZED\s+VIEW|VIEW|DICTIONARY|TABLE)\b`)
var cloudViewOrDictRE = regexp.MustCompile(`(?i)\bCREATE\s+((MATERIALIZED\s+)?VIEW|DICTIONARY)\b`)

type cloudManifestFile struct {
	Name      string `xml:"name"`
	Size      int64  `xml:"size"`
	Checksum  string `xml:"checksum"`
	DataFile  string `xml:"data_file"`
	UseBase   bool   `xml:"use_base"`
	ObjectKey string `xml:"object_key"`
}

type cloudBackupManifest struct {
	XMLName                  xml.Name            `xml:"config"`
	DataFileNameGenerator    string              `xml:"data_file_name_generator"`
	DataFileNamePrefixLength int                 `xml:"data_file_name_prefix_length"`
	Files                    []cloudManifestFile `xml:"contents>file"`
}

func parseCloudManifest(r io.Reader) (*cloudBackupManifest, error) {
	m := &cloudBackupManifest{}
	if err := xml.NewDecoder(r).Decode(m); err != nil {
		return nil, errors.Wrap(err, "can't parse .backup manifest")
	}
	if m.DataFileNameGenerator == "" {
		m.DataFileNameGenerator = "FirstFileName"
	}
	if m.DataFileNamePrefixLength == 0 {
		m.DataFileNamePrefixLength = cloudManifestDefaultPrefixLength
	}
	return m, nil
}

// blobKey maps a logical manifest entry to the backup-prefix-relative key of its blob
func (m *cloudBackupManifest) blobKey(f *cloudManifestFile) string {
	if f.DataFile != "" {
		return f.DataFile
	}
	if strings.EqualFold(m.DataFileNameGenerator, "checksum") && f.Checksum != "" {
		c := strings.ToLower(f.Checksum)
		if n := m.DataFileNamePrefixLength; 0 < n && n < len(c) {
			return c[:n] + "/" + c[n:]
		}
		return c
	}
	return f.Name
}

// cloudLogicalNames extracts (database, table) from `metadata/<db>.sql` / `metadata/<db>/<table>.sql`,
// names are percent-encoded in the backup layout and may carry a shards/<N>/replicas/<M>/ prefix
func cloudLogicalNames(name string) (string, string) {
	if m := cloudShardPrefixRE.FindString(name); m != "" {
		name = name[len(m):]
	}
	rel := strings.TrimSuffix(strings.TrimPrefix(name, "metadata/"), ".sql")
	parts := strings.SplitN(rel, "/", 2)
	db, table := parts[0], ""
	if len(parts) > 1 {
		table = parts[1]
	}
	if decoded, err := url.PathUnescape(db); err == nil {
		db = decoded
	}
	if decoded, err := url.PathUnescape(table); err == nil && table != "" {
		table = decoded
	}
	return db, table
}

// rewriteCloudSchema converts ClickHouse Cloud DDL to OSS DDL:
// database ENGINE = Shared -> Atomic, Shared*MergeTree -> Replicated*MergeTree (zk args added when absent),
// and injects IF NOT EXISTS so restore into a pre-created object is idempotent
func rewriteCloudSchema(sql, kind, replicatedZkPath, replicatedReplica string) string {
	out := sql
	if kind == "table" {
		out = sharedMergeTreeRE.ReplaceAllString(out, "Replicated${1}MergeTree")
		out = replicatedMergeTreeArgsRE.ReplaceAllStringFunc(out, func(m string) string {
			if strings.HasSuffix(m, "(") {
				return m
			}
			return fmt.Sprintf("%s(%s, %s)", m, replicatedZkPath, replicatedReplica)
		})
	} else if kind == "database" {
		out = sharedDatabaseEngineRE.ReplaceAllString(out, "${1}Atomic")
	}
	if cloudCreateKindRE.MatchString(out) && !cloudIfNotExistsRE.MatchString(out) {
		out = cloudCreateKindRE.ReplaceAllString(out, "${0} IF NOT EXISTS")
	}
	return out
}

// cloudApplyOrder - DDL apply order inside one database: dictionaries and tables before views
func cloudApplyOrder(sql string) int {
	m := cloudCreateKindRE.FindStringSubmatch(sql)
	if m == nil {
		return 5
	}
	switch strings.ToUpper(strings.Join(strings.Fields(m[1]), " ")) {
	case "DATABASE":
		return 0
	case "DICTIONARY":
		return 1
	case "TABLE":
		return 2
	case "VIEW":
		return 3
	case "MATERIALIZED VIEW":
		return 4
	}
	return 5
}

func matchCloudTablePattern(tablePattern, db, table string) bool {
	if tablePattern == "" {
		return true
	}
	for _, pattern := range strings.Split(tablePattern, ",") {
		pattern = strings.Trim(pattern, " \r\t\n")
		if matched, _ := filepath.Match(pattern, db+"."+table); matched {
			return true
		}
	}
	return false
}

func cloudQuoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// restoreCloudRedact removes credentials from text destined for logs and error messages
func restoreCloudRedact(s string, secrets ...string) string {
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "***")
		}
	}
	return s
}

type RestoreCloudOptions struct {
	Prefix            string // key prefix of the backup inside the bucket/container (directory that contains `.backup`)
	Bucket            string
	Region            string
	Endpoint          string
	Container         string // azblob container, switches the source to AzureBlobStorage
	BasePrefix        string // prefix of the base backup for incremental backups with use_base
	S3RestoreURL      string // URL passed to RESTORE ... FROM S3('...'), default https://s3.<region>.amazonaws.com/<bucket>/<prefix>
	AzblobRestoreURL  string // blob endpoint passed to RESTORE ... FROM AzureBlobStorage(...), e.g. http://azurite:10000/devstoreaccount1, switches the source to AzureBlobStorage
	TablePattern      string
	Partitions        []string
	RestoreOnCluster  string // cluster name for CREATE/RESTORE ... ON CLUSTER, macros like {cluster} are resolved
	ReplicatedZkPath  string
	ReplicatedReplica string
	SkipEmptyTables   bool
	ContinueOnError   bool
}

// cloudSource abstracts the backup storage (S3 / GCS-over-S3 / AzureBlobStorage) for RestoreCloud
type cloudSource struct {
	reader interface {
		GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error)
	}
	isNotFound      func(error) bool
	restoreLocation string   // FROM clause of the RESTORE statement, contains credentials
	secrets         []string // credentials to redact from logs and errors
	label           string   // human-readable source for logs, e.g. s3://bucket
	close           func(ctx context.Context)
}

// connectCloudSourceS3 - S3 and any S3-compatible endpoint (GCS with HMAC keys, MinIO)
func (b *Backuper) connectCloudSourceS3(ctx context.Context, opts *RestoreCloudOptions, prefix string) (*cloudSource, error) {
	s3cfg := b.cfg.S3
	if opts.Bucket != "" {
		s3cfg.Bucket = opts.Bucket
	}
	if opts.Region != "" {
		s3cfg.Region = opts.Region
	}
	if opts.Endpoint != "" {
		s3cfg.Endpoint = opts.Endpoint
	}
	s3cfg.Path = ""
	s3cfg.ObjectDiskPath = ""
	if s3cfg.Bucket == "" {
		return nil, errors.New("provide --bucket or s3->bucket in config")
	}
	accessKey, secretKey := s3cfg.AccessKey, s3cfg.SecretKey
	if accessKey == "" {
		accessKey, secretKey = os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if accessKey == "" || secretKey == "" {
		if s3cfg.AssumeRoleARN == "" {
			return nil, errors.New("provide s3->access_key and s3->secret_key in config, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables, or s3->assume_role_arn for keyless IAM role access, RESTORE ... FROM S3(...) requires explicit credentials or extra_credentials(role_arn='...')")
		}
		// keyless IAM role: the Go S3 client resolves the STS AssumeRole base from the ambient
		// provider chain (shared credentials file, IRSA, EC2/ECS instance profile), and the RESTORE
		// statement carries only extra_credentials(role_arn=...) so the ClickHouse server signs the
		// STS AssumeRole call with its own ambient identity (use_environment_credentials defaults to true)
		accessKey, secretKey = "", ""
	}
	s3Client := &storage.S3{Config: &s3cfg, Concurrency: 1}
	if err := s3Client.Connect(ctx); err != nil {
		return nil, errors.Wrap(err, "can't connect to s3")
	}
	restoreURL := strings.TrimSuffix(opts.S3RestoreURL, "/")
	if restoreURL == "" {
		if s3cfg.Endpoint != "" {
			restoreURL = fmt.Sprintf("%s/%s/%s", strings.TrimSuffix(s3cfg.Endpoint, "/"), s3cfg.Bucket, prefix)
		} else {
			restoreURL = fmt.Sprintf("https://s3.%s.amazonaws.com/%s/%s", s3cfg.Region, s3cfg.Bucket, prefix)
		}
	}
	// s3->assume_role_arn: the RESTORE reads the bucket with the assumed role's permissions,
	// the static keys (when present) only sign the STS AssumeRole call, without them the
	// ClickHouse server's ambient identity signs it (same semantics as the manifest reads above)
	var restoreLocation string
	switch {
	case s3cfg.AssumeRoleARN != "" && accessKey == "":
		restoreLocation = fmt.Sprintf("S3('%s', extra_credentials(role_arn = '%s'))", restoreURL, s3cfg.AssumeRoleARN)
	case s3cfg.AssumeRoleARN != "":
		restoreLocation = fmt.Sprintf("S3('%s', '%s', '%s', extra_credentials(role_arn = '%s'))", restoreURL, accessKey, secretKey, s3cfg.AssumeRoleARN)
	default:
		restoreLocation = fmt.Sprintf("S3('%s', '%s', '%s')", restoreURL, accessKey, secretKey)
	}
	return &cloudSource{
		reader: s3Client,
		isNotFound: func(err error) bool {
			var noSuchKey *s3types.NoSuchKey
			return stderrors.As(err, &noSuchKey)
		},
		restoreLocation: restoreLocation,
		secrets:         []string{accessKey, secretKey},
		label:           fmt.Sprintf("s3://%s", s3cfg.Bucket),
		close: func(ctx context.Context) {
			if closeErr := s3Client.Close(ctx); closeErr != nil {
				log.Warn().Msgf("can't close S3 connection: %v", closeErr)
			}
		},
	}, nil
}

// connectCloudSourceAzblob - AzureBlobStorage backup source (ClickHouse Cloud on Azure)
func (b *Backuper) connectCloudSourceAzblob(ctx context.Context, opts *RestoreCloudOptions, prefix string) (*cloudSource, error) {
	azcfg := b.cfg.AzureBlob
	if opts.Container != "" {
		azcfg.Container = opts.Container
	}
	azcfg.Path = ""
	azcfg.ObjectDiskPath = ""
	if azcfg.Container == "" {
		return nil, errors.New("provide --container or azblob->container in config")
	}
	if azcfg.AccountName == "" || azcfg.AccountKey == "" {
		return nil, errors.New("provide azblob->account_name and azblob->account_key in config, RESTORE ... FROM AzureBlobStorage(...) requires explicit credentials")
	}
	azClient := &storage.AzureBlob{Config: &azcfg}
	if err := azClient.Connect(ctx); err != nil {
		return nil, errors.Wrap(err, "can't connect to azblob")
	}
	blobEndpoint := strings.TrimSuffix(opts.AzblobRestoreURL, "/")
	protocol := azcfg.EndpointSchema
	if blobEndpoint == "" {
		endpointURL := url.URL{Scheme: azcfg.EndpointSchema}
		// https://github.com/Altinity/clickhouse-backup/issues/1031
		if azcfg.EndpointSuffix == "core.windows.net" {
			endpointURL.Host = azcfg.AccountName + ".blob." + azcfg.EndpointSuffix
		} else {
			endpointURL.Host = azcfg.EndpointSuffix
			endpointURL.Path = azcfg.AccountName
		}
		blobEndpoint = endpointURL.String()
	} else if scheme, _, found := strings.Cut(blobEndpoint, "://"); found {
		protocol = scheme
	}
	connectionString := fmt.Sprintf("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;", protocol, azcfg.AccountName, azcfg.AccountKey, blobEndpoint)
	return &cloudSource{
		reader: azClient,
		isNotFound: func(err error) bool {
			return bloberror.HasCode(err, bloberror.BlobNotFound)
		},
		restoreLocation: fmt.Sprintf("AzureBlobStorage('%s', '%s', '%s/')", connectionString, azcfg.Container, prefix),
		secrets:         []string{azcfg.AccountKey},
		label:           fmt.Sprintf("azblob://%s", azcfg.Container),
		close: func(ctx context.Context) {
			if closeErr := azClient.Close(ctx); closeErr != nil {
				log.Warn().Msgf("can't close AzureBlob connection: %v", closeErr)
			}
		},
	}, nil
}

func (b *Backuper) RestoreCloud(opts RestoreCloudOptions, commandId int) error {
	if opts.Prefix == "" {
		return errors.New("backup prefix must be defined")
	}
	if opts.ReplicatedZkPath == "" {
		opts.ReplicatedZkPath = "'/clickhouse/tables/{uuid}/{shard}'"
	}
	if opts.ReplicatedReplica == "" {
		opts.ReplicatedReplica = "'{replica}'"
	}
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return errors.Wrap(err, "status.Current.GetContextWithCancel")
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startRestoreCloud := time.Now()
	if err = b.ch.Connect(); err != nil {
		return errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	prefix := strings.Trim(opts.Prefix, "/")
	basePrefix := strings.Trim(opts.BasePrefix, "/")
	var source *cloudSource
	if opts.Container != "" || opts.AzblobRestoreURL != "" || b.cfg.General.RemoteStorage == "azblob" {
		source, err = b.connectCloudSourceAzblob(ctx, &opts, prefix)
	} else {
		source, err = b.connectCloudSourceS3(ctx, &opts, prefix)
	}
	if err != nil {
		return err
	}
	defer source.close(ctx)

	manifestKey := path.Join(prefix, ".backup")
	log.Info().Msgf("read %s/%s", source.label, manifestKey)
	manifestReader, err := source.reader.GetFileReaderAbsolute(ctx, manifestKey)
	if err != nil {
		return errors.Wrapf(err, "can't read %s/%s", source.label, manifestKey)
	}
	manifest, err := parseCloudManifest(manifestReader)
	_ = manifestReader.Close()
	if err != nil {
		return err
	}
	log.Info().Msgf("manifest generator=%s prefix_length=%d files=%d", manifest.DataFileNameGenerator, manifest.DataFileNamePrefixLength, len(manifest.Files))

	// group manifest entries: database DDL, table DDL and logical data bytes per table,
	// `BACKUP ... ON CLUSTER` prefixes every name with shards/<shard_num>/replicas/<replica_num>/
	dbDDLFiles := map[string]*cloudManifestFile{}
	tableDDLFiles := map[string][]*cloudManifestFile{}
	seenTableDDL := map[[2]string]struct{}{}
	dataBytes := map[[2]string]int64{}
	dataBytesShard := map[[3]string]int64{}
	backupShards := map[string]struct{}{}
	for i := range manifest.Files {
		f := &manifest.Files[i]
		name, shard := f.Name, "1"
		if m := cloudShardPrefixRE.FindStringSubmatch(name); m != nil {
			shard = m[1]
			name = name[len(m[0]):]
			backupShards[shard] = struct{}{}
		}
		if strings.HasPrefix(name, "data/") {
			parts := strings.Split(name, "/")
			if len(parts) < 3 {
				continue
			}
			db, table := parts[1], parts[2]
			if decoded, decodeErr := url.PathUnescape(db); decodeErr == nil {
				db = decoded
			}
			if decoded, decodeErr := url.PathUnescape(table); decodeErr == nil {
				table = decoded
			}
			dataBytes[[2]string{db, table}] += f.Size
			dataBytesShard[[3]string{db, table, shard}] += f.Size
			continue
		}
		if !strings.HasPrefix(name, "metadata/") || !strings.HasSuffix(name, ".sql") {
			continue
		}
		db, table := cloudLogicalNames(name)
		if table == "" {
			dbDDLFiles[db] = f
		} else if _, seen := seenTableDDL[[2]string{db, table}]; !seen {
			// each shard carries its own copy of the DDL, restore the first one on the whole cluster
			seenTableDDL[[2]string{db, table}] = struct{}{}
			tableDDLFiles[db] = append(tableDDLFiles[db], f)
		}
	}

	onClusterSQL, localShard, err := b.checkCloudClusterTopology(ctx, opts.RestoreOnCluster, len(backupShards))
	if err != nil {
		return err
	}

	databases := make([]string, 0, len(tableDDLFiles)+len(dbDDLFiles))
	for db := range tableDDLFiles {
		databases = append(databases, db)
	}
	for db := range dbDDLFiles {
		if _, exists := tableDDLFiles[db]; !exists {
			databases = append(databases, db)
		}
	}
	sort.Strings(databases)

	errorsCount := 0
	handleError := func(what string, handledErr error) error {
		errorsCount++
		redacted := errors.Errorf("%s: %s", what, restoreCloudRedact(handledErr.Error(), source.secrets...))
		if opts.ContinueOnError {
			log.Error().Msgf("%v", redacted)
			return nil
		}
		return redacted
	}

	for _, database := range databases {
		if _, skip := cloudSkipDatabases[database]; skip {
			continue
		}
		// collect table DDLs first, so a database whose tables are all filtered out is not created
		type tableDDL struct {
			table         string
			sql           string
			partitionsSQL string // " PARTITIONS ..." clause of RESTORE TABLE, empty when no --partitions filter applies
		}
		tableDDLs := make([]tableDDL, 0, len(tableDDLFiles[database]))
		for _, f := range tableDDLFiles[database] {
			_, table := cloudLogicalNames(f.Name)
			if !matchCloudTablePattern(opts.TablePattern, database, table) {
				continue
			}
			if b.shouldSkipByTableName(fmt.Sprintf("%s.%s", database, table)) {
				log.Info().Msgf("skip %s.%s by clickhouse->skip_tables", database, table)
				continue
			}
			if opts.SkipEmptyTables && dataBytes[[2]string{database, table}] == 0 {
				log.Info().Msgf("skip empty %s.%s (no data files in backup)", database, table)
				continue
			}
			if f.Size == 0 {
				log.Info().Msgf("skip empty %s", f.Name)
				continue
			}
			ddl, fetchErr := b.fetchCloudBlob(ctx, source, manifest, f, prefix, basePrefix)
			if fetchErr != nil {
				if handledErr := handleError(fmt.Sprintf("fetch %s.%s", database, table), fetchErr); handledErr != nil {
					return handledErr
				}
				continue
			}
			rewrittenDDL := rewriteCloudSchema(ddl, "table", opts.ReplicatedZkPath, opts.ReplicatedReplica)
			partitionsSQL, partitionsMatched := b.cloudRestorePartitionsSQL(ctx, database, table, rewrittenDDL, opts.Partitions)
			if !partitionsMatched {
				log.Info().Msgf("skip %s.%s (no matching --partitions)", database, table)
				continue
			}
			tableDDLs = append(tableDDLs, tableDDL{table: table, sql: rewrittenDDL, partitionsSQL: partitionsSQL})
		}
		if len(tableDDLs) == 0 && (opts.TablePattern != "" || len(opts.Partitions) > 0) {
			continue
		}
		log.Info().Msgf("######## database %s ########", database)
		if f, exists := dbDDLFiles[database]; exists && f.Size > 0 {
			ddl, fetchErr := b.fetchCloudBlob(ctx, source, manifest, f, prefix, basePrefix)
			if fetchErr == nil {
				fetchErr = b.restoreCloudExec(ctx, injectCloudOnCluster(rewriteCloudSchema(ddl, "database", "", ""), onClusterSQL), fmt.Sprintf("database %s", database), source.secrets)
			}
			if fetchErr != nil {
				if handledErr := handleError(fmt.Sprintf("database %s", database), fetchErr); handledErr != nil {
					return handledErr
				}
				continue
			}
		}
		sort.SliceStable(tableDDLs, func(i, j int) bool {
			iOrder, jOrder := cloudApplyOrder(tableDDLs[i].sql), cloudApplyOrder(tableDDLs[j].sql)
			if iOrder != jOrder {
				return iOrder < jOrder
			}
			return tableDDLs[i].table < tableDDLs[j].table
		})
		for _, t := range tableDDLs {
			label := fmt.Sprintf("%s.%s", database, t.table)
			restoreErr := b.restoreCloudExec(ctx, injectCloudOnCluster(t.sql, onClusterSQL), fmt.Sprintf("table %s", label), source.secrets)
			if restoreErr == nil {
				restoreSQL := fmt.Sprintf(
					"RESTORE TABLE %s.%s%s%s FROM %s SETTINGS allow_different_database_def=1, allow_different_table_def=1",
					cloudQuoteIdent(database), cloudQuoteIdent(t.table), t.partitionsSQL, onClusterSQL, source.restoreLocation,
				)
				restoreErr = b.restoreCloudExec(ctx, restoreSQL, fmt.Sprintf("RESTORE TABLE %s", label), source.secrets)
			}
			if restoreErr == nil {
				expectedBytes, checkSize := dataBytes[[2]string{database, t.table}], true
				if t.partitionsSQL != "" {
					// manifest bytes cover all partitions, the restored subset is expected to be smaller
					log.Info().Msgf("size check skipped for %s (--partitions filter)", label)
					checkSize = false
				} else if onClusterSQL != "" {
					if localShard == "" {
						log.Info().Msgf("size check skipped for %s (local host is not a member of the cluster)", label)
						checkSize = false
					} else {
						// system.parts is local, compare against the local shard slice of the backup;
						// data may be restored on another replica of the shard, sync before checking
						expectedBytes = dataBytesShard[[3]string{database, t.table, localShard}]
						b.cloudSyncReplica(ctx, database, t.table, t.sql)
					}
				}
				if checkSize {
					restoreErr = b.checkCloudRestoredSize(ctx, database, t.table, expectedBytes, t.sql)
				}
			}
			if restoreErr != nil {
				if handledErr := handleError(label, restoreErr); handledErr != nil {
					return handledErr
				}
			}
		}
	}
	if errorsCount > 0 {
		return errors.Errorf("restore_cloud finished with %d error(s)", errorsCount)
	}
	log.Info().Fields(map[string]interface{}{
		"prefix":    prefix,
		"source":    source.label,
		"operation": "restore_cloud",
		"duration":  utils.HumanizeDuration(time.Since(startRestoreCloud)),
	}).Msg("done")
	return nil
}

// checkCloudClusterTopology resolves macros in --restore-on-cluster and verifies the cluster has
// the same number of shards as the backup (replica counts may differ, ReplicatedMergeTree
// replicates the restored data); returns the ` ON CLUSTER '...'` clause and the local shard number
// used for the per-shard size check (empty when the local host is not a cluster member)
func (b *Backuper) checkCloudClusterTopology(ctx context.Context, restoreOnCluster string, backupShardsCount int) (string, string, error) {
	if backupShardsCount == 0 {
		backupShardsCount = 1
	}
	if restoreOnCluster == "" {
		if backupShardsCount > 1 {
			return "", "", errors.Errorf("backup was created with ON CLUSTER and contains %d shards, pass --restore-on-cluster", backupShardsCount)
		}
		return "", "", nil
	}
	cluster, err := b.ch.ApplyMacros(ctx, restoreOnCluster)
	if err != nil {
		return "", "", errors.Wrapf(err, "can't resolve macros in --restore-on-cluster=%s", restoreOnCluster)
	}
	topology := make([]struct {
		Shards     uint64 `ch:"shards"`
		LocalShard string `ch:"local_shard"`
	}, 0)
	query := "SELECT uniqExact(shard_num) AS shards, coalesce(min(if(is_local, toString(shard_num), NULL)), '') AS local_shard " +
		"FROM system.clusters WHERE cluster=? SETTINGS empty_result_for_aggregation_by_empty_set=0"
	if err = b.ch.SelectContext(ctx, &topology, query, cluster); err != nil {
		return "", "", errors.Wrap(err, "can't get cluster topology from system.clusters")
	}
	if len(topology) == 0 || topology[0].Shards == 0 {
		return "", "", errors.Errorf("cluster '%s' not found in system.clusters", cluster)
	}
	if int(topology[0].Shards) != backupShardsCount {
		return "", "", errors.Errorf("backup contains %d shard(s) but cluster '%s' has %d shard(s), topology must match by shards (replica counts may differ)", backupShardsCount, cluster, topology[0].Shards)
	}
	log.Info().Msgf("restore on cluster '%s': %d shard(s) match the backup, local shard=%s", cluster, topology[0].Shards, topology[0].LocalShard)
	return fmt.Sprintf(" ON CLUSTER '%s'", strings.ReplaceAll(cluster, "'", "\\'")), topology[0].LocalShard, nil
}

// injectCloudOnCluster inserts the ` ON CLUSTER '...'` clause after
// `CREATE <kind> [IF NOT EXISTS] <name> [UUID '...']`
func injectCloudOnCluster(sql, onClusterSQL string) string {
	if onClusterSQL == "" {
		return sql
	}
	loc := cloudCreateHeaderRE.FindStringIndex(sql)
	if loc == nil {
		return sql
	}
	return sql[:loc[1]] + onClusterSQL + sql[loc[1]:]
}

// cloudSyncReplica waits for the local replica before the per-shard size check, the RESTORE
// ON CLUSTER data may land on another replica of the shard; failure is not fatal
func (b *Backuper) cloudSyncReplica(ctx context.Context, database, table, createSQL string) {
	if b.DryRun || !strings.Contains(createSQL, "Replicated") {
		return
	}
	if err := b.ch.QueryContext(ctx, fmt.Sprintf("SYSTEM SYNC REPLICA %s.%s", cloudQuoteIdent(database), cloudQuoteIdent(table))); err != nil {
		log.Warn().Msgf("SYSTEM SYNC REPLICA %s.%s: %v", database, table, err)
	}
}

// cloudRestorePartitionsSQL builds the ` PARTITIONS ...` clause of RESTORE TABLE from --partitions,
// same formats and semantics as the regular restore (see restoreEmbedded);
// the second result is false when the table has no matching partitions and must be skipped
func (b *Backuper) cloudRestorePartitionsSQL(ctx context.Context, database, table, createSQL string, partitions []string) (string, bool) {
	if len(partitions) == 0 {
		return "", true
	}
	_, partitionsNameList := partition.ConvertPartitionsToIdsMapAndNamesList(ctx, b.ch, nil, ListOfTables{&metadata.TableMetadata{Database: database, Table: table, Query: createSQL}}, partitions)
	tablePartitions := partitionsNameList[metadata.TableTitle{Database: database, Table: table}]
	if len(tablePartitions) == 0 {
		return "", false
	}
	// `*` restores all partitions, views and dictionaries don't accept a PARTITIONS clause
	if tablePartitions[0] == "*" || cloudViewOrDictRE.MatchString(createSQL) {
		return "", true
	}
	partitionsSQL := fmt.Sprintf("ID '%s'", strings.Join(tablePartitions, "',ID '"))
	if strings.HasPrefix(partitionsSQL, "ID '(") {
		partitionsSQL = strings.Join(tablePartitions, ",")
	}
	return " PARTITIONS " + partitionsSQL, true
}

// fetchCloudBlob downloads a manifest entry, resolving its blob key and falling back between the
// backup prefix and the base backup prefix (incremental backups store unchanged files in the base)
func (b *Backuper) fetchCloudBlob(ctx context.Context, source *cloudSource, manifest *cloudBackupManifest, f *cloudManifestFile, prefix, basePrefix string) (string, error) {
	blob := manifest.blobKey(f)
	prefixes := []string{prefix}
	if basePrefix != "" {
		if f.UseBase {
			prefixes = []string{basePrefix, prefix}
		} else {
			prefixes = []string{prefix, basePrefix}
		}
	}
	var lastErr error
	for _, p := range prefixes {
		key := path.Join(p, blob)
		reader, err := source.reader.GetFileReaderAbsolute(ctx, key)
		if err != nil {
			if source.isNotFound(err) {
				lastErr = err
				continue
			}
			return "", err
		}
		content, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr != nil {
			return "", errors.Wrapf(readErr, "can't read %s", key)
		}
		log.Debug().Msgf("fetched %s -> %s (%d bytes)", f.Name, key, len(content))
		return string(content), nil
	}
	return "", errors.Wrapf(lastErr, "blob %s for %s not found under %v", blob, f.Name, prefixes)
}

// restoreCloudExec executes one DDL / RESTORE statement, honoring --dry-run
func (b *Backuper) restoreCloudExec(ctx context.Context, sql, what string, secrets []string) error {
	logSQL := restoreCloudRedact(sql, secrets...)
	if b.DryRun {
		log.Info().Msgf("DRY-RUN %s: %s", what, logSQL)
		return nil
	}
	log.Info().Msgf("=== %s ===\n%s", what, logSQL)
	if err := b.ch.QueryContext(ctx, sql); err != nil {
		return errors.Errorf("%s", restoreCloudRedact(err.Error(), secrets...))
	}
	return nil
}

// checkCloudRestoredSize compares restored system.parts size/rows against the byte sum of
// data/<db>/<table>/ files in the backup manifest
func (b *Backuper) checkCloudRestoredSize(ctx context.Context, database, table string, backupBytes int64, createSQL string) error {
	if b.DryRun {
		log.Info().Msgf("size check skipped: backup data/%s/%s = %d bytes", database, table, backupBytes)
		return nil
	}
	if cloudViewOrDictRE.MatchString(createSQL) {
		log.Info().Msgf("size check skipped for %s.%s (view/dictionary)", database, table)
		return nil
	}
	restored := make([]struct {
		Bytes uint64 `ch:"bytes"`
		Rows  uint64 `ch:"rows"`
		Parts uint64 `ch:"parts"`
	}, 0)
	query := "SELECT coalesce(sum(bytes_on_disk), 0) AS bytes, coalesce(sum(rows), 0) AS rows, count() AS parts " +
		"FROM system.parts WHERE database=? AND table=? AND active " +
		"SETTINGS empty_result_for_aggregation_by_empty_set=0"
	if err := b.ch.SelectContext(ctx, &restored, query, database, table); err != nil {
		return errors.Wrap(err, "can't get restored table stats from system.parts")
	}
	var restoredBytes, restoredRows, restoredParts uint64
	if len(restored) > 0 {
		restoredBytes, restoredRows, restoredParts = restored[0].Bytes, restored[0].Rows, restored[0].Parts
	}
	log.Info().Msgf("size check %s.%s: backup_bytes=%d restored_bytes=%d rows=%d parts=%d", database, table, backupBytes, restoredBytes, restoredRows, restoredParts)
	if backupBytes > 0 && restoredBytes == 0 {
		return errors.Errorf("%s.%s: backup has %d bytes of data files but restored table is empty (0 bytes, %d rows, %d parts)", database, table, backupBytes, restoredRows, restoredParts)
	}
	if backupBytes > 0 && restoredRows == 0 {
		return errors.Errorf("%s.%s: backup has %d bytes of data files but restored table has 0 rows (%d bytes_on_disk, %d parts)", database, table, backupBytes, restoredBytes, restoredParts)
	}
	// bytes_on_disk is usually close to the sum of part files in the backup, not identical
	if backupBytes > 0 && restoredBytes > 0 {
		delta := backupBytes - int64(restoredBytes)
		if delta < 0 {
			delta = -delta
		}
		limit := backupBytes * 5 / 100
		if limit < 4096 {
			limit = 4096
		}
		if delta > limit {
			return errors.Errorf("%s.%s: size mismatch backup_bytes=%d restored_bytes=%d delta=%d (limit %d)", database, table, backupBytes, restoredBytes, delta, limit)
		}
	}
	return nil
}
