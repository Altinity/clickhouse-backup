package config

import (
	"crypto/tls"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/log_helper"
	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v3"
)

const (
	DefaultConfigPath = "/etc/clickhouse-backup/config.yml"
)

// Config - config file format
type Config struct {
	General    GeneralConfig    `yaml:"general" envconfig:"_"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse" envconfig:"_"`
	S3         S3Config         `yaml:"s3" envconfig:"_"`
	GCS        GCSConfig        `yaml:"gcs" envconfig:"_"`
	COS        COSConfig        `yaml:"cos" envconfig:"_"`
	API        APIConfig        `yaml:"api" envconfig:"_"`
	FTP        FTPConfig        `yaml:"ftp" envconfig:"_"`
	SFTP       SFTPConfig       `yaml:"sftp" envconfig:"_"`
	AzureBlob  AzureBlobConfig  `yaml:"azblob" envconfig:"_"`
	Custom     CustomConfig     `yaml:"custom" envconfig:"_"`
}

// BatchDeletionConfig - batch deletion optimization settings
type BatchDeletionConfig struct {
	Enabled          bool    `yaml:"enabled" envconfig:"BATCH_DELETE_ENABLED" default:"true"`
	Workers          int     `yaml:"workers" envconfig:"BATCH_DELETE_WORKERS" default:"0"` // 0 = auto-detect
	BatchSize        int     `yaml:"batch_size" envconfig:"BATCH_DELETE_BATCH_SIZE" default:"1000"`
	RetryAttempts    int     `yaml:"retry_attempts" envconfig:"BATCH_DELETE_RETRY_ATTEMPTS" default:"3"`
	ErrorStrategy    string  `yaml:"error_strategy" envconfig:"BATCH_DELETE_ERROR_STRATEGY" default:"continue"` // fail_fast, continue, retry_batch
	FailureThreshold float64 `yaml:"failure_threshold" envconfig:"BATCH_DELETE_FAILURE_THRESHOLD" default:"0.1"`
}

// GeneralConfig - general setting section
type GeneralConfig struct {
	RemoteStorage                       string              `yaml:"remote_storage" envconfig:"REMOTE_STORAGE"`
	MaxFileSize                         int64               `yaml:"max_file_size" envconfig:"MAX_FILE_SIZE"`
	BackupsToKeepLocal                  int                 `yaml:"backups_to_keep_local" envconfig:"BACKUPS_TO_KEEP_LOCAL"`
	BackupsToKeepRemote                 int                 `yaml:"backups_to_keep_remote" envconfig:"BACKUPS_TO_KEEP_REMOTE"`
	LogLevel                            string              `yaml:"log_level" envconfig:"LOG_LEVEL"`
	AllowEmptyBackups                   bool                `yaml:"allow_empty_backups" envconfig:"ALLOW_EMPTY_BACKUPS"`
	DownloadConcurrency                 uint8               `yaml:"download_concurrency" envconfig:"DOWNLOAD_CONCURRENCY"`
	UploadConcurrency                   uint8               `yaml:"upload_concurrency" envconfig:"UPLOAD_CONCURRENCY"`
	UploadMaxBytesPerSecond             uint64              `yaml:"upload_max_bytes_per_second" envconfig:"UPLOAD_MAX_BYTES_PER_SECOND"`
	DownloadMaxBytesPerSecond           uint64              `yaml:"download_max_bytes_per_second" envconfig:"DOWNLOAD_MAX_BYTES_PER_SECOND"`
	ObjectDiskServerSideCopyConcurrency uint8               `yaml:"object_disk_server_side_copy_concurrency" envconfig:"OBJECT_DISK_SERVER_SIDE_COPY_CONCURRENCY"`
	AllowObjectDiskStreaming            bool                `yaml:"allow_object_disk_streaming" envconfig:"ALLOW_OBJECT_DISK_STREAMING"`
	UseResumableState                   bool                `yaml:"use_resumable_state" envconfig:"USE_RESUMABLE_STATE"`
	RestoreSchemaOnCluster              string              `yaml:"restore_schema_on_cluster" envconfig:"RESTORE_SCHEMA_ON_CLUSTER"`
	UploadByPart                        bool                `yaml:"upload_by_part" envconfig:"UPLOAD_BY_PART"`
	DownloadByPart                      bool                `yaml:"download_by_part" envconfig:"DOWNLOAD_BY_PART"`
	RestoreDatabaseMapping              map[string]string   `yaml:"restore_database_mapping" envconfig:"RESTORE_DATABASE_MAPPING"`
	RestoreTableMapping                 map[string]string   `yaml:"restore_table_mapping" envconfig:"RESTORE_TABLE_MAPPING"`
	RetriesOnFailure                    int                 `yaml:"retries_on_failure" envconfig:"RETRIES_ON_FAILURE"`
	RetriesPause                        string              `yaml:"retries_pause" envconfig:"RETRIES_PAUSE"`
	RetriesJitter                       int8                `yaml:"retries_jitter" envconfig:"RETRIES_JITTER"`
	WatchInterval                       string              `yaml:"watch_interval" envconfig:"WATCH_INTERVAL"`
	FullInterval                        string              `yaml:"full_interval" envconfig:"FULL_INTERVAL"`
	WatchBackupNameTemplate             string              `yaml:"watch_backup_name_template" envconfig:"WATCH_BACKUP_NAME_TEMPLATE"`
	ShardedOperationMode                string              `yaml:"sharded_operation_mode" envconfig:"SHARDED_OPERATION_MODE"`
	CPUNicePriority                     int                 `yaml:"cpu_nice_priority" envconfig:"CPU_NICE_PRIORITY"`
	IONicePriority                      string              `yaml:"io_nice_priority" envconfig:"IO_NICE_PRIORITY"`
	RBACBackupAlways                    bool                `yaml:"rbac_backup_always" envconfig:"RBAC_BACKUP_ALWAYS"`
	RBACConflictResolution              string              `yaml:"rbac_conflict_resolution" envconfig:"RBAC_CONFLICT_RESOLUTION"`
	ConfigBackupAlways                  bool                `yaml:"config_backup_always" envconfig:"CONFIG_BACKUP_ALWAYS"`
	NamedCollectionsBackupAlways        bool                `yaml:"named_collections_backup_always" envconfig:"NAMED_COLLECTIONS_BACKUP_ALWAYS"`
	BatchDeletion                       BatchDeletionConfig `yaml:"batch_deletion" envconfig:"_"`
	RetriesDuration                     time.Duration
	WatchDuration                       time.Duration
	FullDuration                        time.Duration
}

// GCSBatchConfig - GCS batch deletion specific settings
type GCSBatchConfig struct {
	MaxWorkers    int  `yaml:"max_workers" envconfig:"GCS_MAX_WORKERS" default:"50"`
	UseClientPool bool `yaml:"use_client_pool" envconfig:"GCS_USE_CLIENT_POOL" default:"true"`
	UseBatchAPI   bool `yaml:"use_batch_api" envconfig:"GCS_USE_BATCH_API" default:"true"`
}

// GCSConfig - GCS settings section
type GCSConfig struct {
	CredentialsFile        string            `yaml:"credentials_file" envconfig:"GCS_CREDENTIALS_FILE"`
	CredentialsJSON        string            `yaml:"credentials_json" envconfig:"GCS_CREDENTIALS_JSON"`
	CredentialsJSONEncoded string            `yaml:"credentials_json_encoded" envconfig:"GCS_CREDENTIALS_JSON_ENCODED"`
	EmbeddedAccessKey      string            `yaml:"embedded_access_key" envconfig:"GCS_EMBEDDED_ACCESS_KEY"`
	EmbeddedSecretKey      string            `yaml:"embedded_secret_key" envconfig:"GCS_EMBEDDED_SECRET_KEY"`
	SkipCredentials        bool              `yaml:"skip_credentials" envconfig:"GCS_SKIP_CREDENTIALS"`
	Bucket                 string            `yaml:"bucket" envconfig:"GCS_BUCKET"`
	Path                   string            `yaml:"path" envconfig:"GCS_PATH"`
	ObjectDiskPath         string            `yaml:"object_disk_path" envconfig:"GCS_OBJECT_DISK_PATH"`
	CompressionLevel       int               `yaml:"compression_level" envconfig:"GCS_COMPRESSION_LEVEL"`
	CompressionFormat      string            `yaml:"compression_format" envconfig:"GCS_COMPRESSION_FORMAT"`
	Debug                  bool              `yaml:"debug" envconfig:"GCS_DEBUG"`
	ForceHttp              bool              `yaml:"force_http" envconfig:"GCS_FORCE_HTTP"`
	Endpoint               string            `yaml:"endpoint" envconfig:"GCS_ENDPOINT"`
	StorageClass           string            `yaml:"storage_class" envconfig:"GCS_STORAGE_CLASS"`
	ObjectLabels           map[string]string `yaml:"object_labels" envconfig:"GCS_OBJECT_LABELS"`
	CustomStorageClassMap  map[string]string `yaml:"custom_storage_class_map" envconfig:"GCS_CUSTOM_STORAGE_CLASS_MAP"`
	// NOTE: ClientPoolSize should be at least 2 times bigger than
	// 			UploadConcurrency or DownloadConcurrency in each upload and download case
	ClientPoolSize int            `yaml:"client_pool_size" envconfig:"GCS_CLIENT_POOL_SIZE"`
	ChunkSize      int            `yaml:"chunk_size" envconfig:"GCS_CHUNK_SIZE"`
	BatchDeletion  GCSBatchConfig `yaml:"batch_deletion" envconfig:"_"`
}

// AzureBatchConfig - Azure Blob batch deletion specific settings
type AzureBatchConfig struct {
	UseBatchAPI bool `yaml:"use_batch_api" envconfig:"AZURE_USE_BATCH_API" default:"true"`
	MaxWorkers  int  `yaml:"max_workers" envconfig:"AZURE_MAX_WORKERS" default:"20"`
}

// AzureBlobConfig - Azure Blob settings section
type AzureBlobConfig struct {
	EndpointSchema        string           `yaml:"endpoint_schema" envconfig:"AZBLOB_ENDPOINT_SCHEMA"`
	EndpointSuffix        string           `yaml:"endpoint_suffix" envconfig:"AZBLOB_ENDPOINT_SUFFIX"`
	AccountName           string           `yaml:"account_name" envconfig:"AZBLOB_ACCOUNT_NAME"`
	AccountKey            string           `yaml:"account_key" envconfig:"AZBLOB_ACCOUNT_KEY"`
	SharedAccessSignature string           `yaml:"sas" envconfig:"AZBLOB_SAS"`
	UseManagedIdentity    bool             `yaml:"use_managed_identity" envconfig:"AZBLOB_USE_MANAGED_IDENTITY"`
	Container             string           `yaml:"container" envconfig:"AZBLOB_CONTAINER"`
	AssumeContainerExists bool             `yaml:"assume_container_exists" envconfig:"AZBLOB_ASSUME_CONTAINER_EXISTS"`
	Path                  string           `yaml:"path" envconfig:"AZBLOB_PATH"`
	ObjectDiskPath        string           `yaml:"object_disk_path" envconfig:"AZBLOB_OBJECT_DISK_PATH"`
	CompressionLevel      int              `yaml:"compression_level" envconfig:"AZBLOB_COMPRESSION_LEVEL"`
	CompressionFormat     string           `yaml:"compression_format" envconfig:"AZBLOB_COMPRESSION_FORMAT"`
	SSEKey                string           `yaml:"sse_key" envconfig:"AZBLOB_SSE_KEY"`
	MaxBuffers            int              `yaml:"buffer_count" envconfig:"AZBLOB_MAX_BUFFERS"`
	MaxPartsCount         int64            `yaml:"max_parts_count" envconfig:"AZBLOB_MAX_PARTS_COUNT"`
	Timeout               string           `yaml:"timeout" envconfig:"AZBLOB_TIMEOUT"`
	Debug                 bool             `yaml:"debug" envconfig:"AZBLOB_DEBUG"`
	BatchDeletion         AzureBatchConfig `yaml:"batch_deletion" envconfig:"_"`
}

// S3BatchConfig - S3 batch deletion specific settings
type S3BatchConfig struct {
	UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"S3_USE_BATCH_API" default:"true"`
	VersionConcurrency int  `yaml:"version_concurrency" envconfig:"S3_VERSION_CONCURRENCY" default:"10"`
	PreloadVersions    bool `yaml:"preload_versions" envconfig:"S3_PRELOAD_VERSIONS" default:"true"`
}

// S3Config - s3 settings section
type S3Config struct {
	AccessKey               string            `yaml:"access_key" envconfig:"S3_ACCESS_KEY"`
	SecretKey               string            `yaml:"secret_key" envconfig:"S3_SECRET_KEY"`
	Bucket                  string            `yaml:"bucket" envconfig:"S3_BUCKET"`
	Endpoint                string            `yaml:"endpoint" envconfig:"S3_ENDPOINT"`
	Region                  string            `yaml:"region" envconfig:"S3_REGION"`
	ACL                     string            `yaml:"acl" envconfig:"S3_ACL"`
	AssumeRoleARN           string            `yaml:"assume_role_arn" envconfig:"S3_ASSUME_ROLE_ARN"`
	ForcePathStyle          bool              `yaml:"force_path_style" envconfig:"S3_FORCE_PATH_STYLE"`
	Path                    string            `yaml:"path" envconfig:"S3_PATH"`
	ObjectDiskPath          string            `yaml:"object_disk_path" envconfig:"S3_OBJECT_DISK_PATH"`
	DisableSSL              bool              `yaml:"disable_ssl" envconfig:"S3_DISABLE_SSL"`
	CompressionLevel        int               `yaml:"compression_level" envconfig:"S3_COMPRESSION_LEVEL"`
	CompressionFormat       string            `yaml:"compression_format" envconfig:"S3_COMPRESSION_FORMAT"`
	SSE                     string            `yaml:"sse" envconfig:"S3_SSE"`
	SSEKMSKeyId             string            `yaml:"sse_kms_key_id" envconfig:"S3_SSE_KMS_KEY_ID"`
	SSECustomerAlgorithm    string            `yaml:"sse_customer_algorithm" envconfig:"S3_SSE_CUSTOMER_ALGORITHM"`
	SSECustomerKey          string            `yaml:"sse_customer_key" envconfig:"S3_SSE_CUSTOMER_KEY"`
	SSECustomerKeyMD5       string            `yaml:"sse_customer_key_md5" envconfig:"S3_SSE_CUSTOMER_KEY_MD5"`
	SSEKMSEncryptionContext string            `yaml:"sse_kms_encryption_context" envconfig:"S3_SSE_KMS_ENCRYPTION_CONTEXT"`
	DisableCertVerification bool              `yaml:"disable_cert_verification" envconfig:"S3_DISABLE_CERT_VERIFICATION"`
	UseCustomStorageClass   bool              `yaml:"use_custom_storage_class" envconfig:"S3_USE_CUSTOM_STORAGE_CLASS"`
	StorageClass            string            `yaml:"storage_class" envconfig:"S3_STORAGE_CLASS"`
	CustomStorageClassMap   map[string]string `yaml:"custom_storage_class_map" envconfig:"S3_CUSTOM_STORAGE_CLASS_MAP"`
	Concurrency             int               `yaml:"concurrency" envconfig:"S3_CONCURRENCY"`
	MaxPartsCount           int64             `yaml:"max_parts_count" envconfig:"S3_MAX_PARTS_COUNT"`
	AllowMultipartDownload  bool              `yaml:"allow_multipart_download" envconfig:"S3_ALLOW_MULTIPART_DOWNLOAD"`
	ObjectLabels            map[string]string `yaml:"object_labels" envconfig:"S3_OBJECT_LABELS"`
	RequestPayer            string            `yaml:"request_payer" envconfig:"S3_REQUEST_PAYER"`
	CheckSumAlgorithm       string            `yaml:"check_sum_algorithm" envconfig:"S3_CHECKSUM_ALGORITHM"`
	RetryMode               string            `yaml:"retry_mode" envconfig:"S3_RETRY_MODE"`
	Debug                   bool              `yaml:"debug" envconfig:"S3_DEBUG"`
	BatchDeletion           S3BatchConfig     `yaml:"batch_deletion" envconfig:"_"`
}

// COSConfig - cos settings section
type COSConfig struct {
	RowURL                 string `yaml:"url" envconfig:"COS_URL"`
	Timeout                string `yaml:"timeout" envconfig:"COS_TIMEOUT"`
	SecretID               string `yaml:"secret_id" envconfig:"COS_SECRET_ID"`
	SecretKey              string `yaml:"secret_key" envconfig:"COS_SECRET_KEY"`
	Path                   string `yaml:"path" envconfig:"COS_PATH"`
	ObjectDiskPath         string `yaml:"object_disk_path" envconfig:"COS_OBJECT_DISK_PATH"`
	CompressionFormat      string `yaml:"compression_format" envconfig:"COS_COMPRESSION_FORMAT"`
	CompressionLevel       int    `yaml:"compression_level" envconfig:"COS_COMPRESSION_LEVEL"`
	Concurrency            int    `yaml:"concurrency" envconfig:"COS_CONCURRENCY"`
	AllowMultipartDownload bool   `yaml:"allow_multipart_download" envconfig:"COS_ALLOW_MULTIPART_DOWNLOAD"`
	MaxPartsCount          int64  `yaml:"max_parts_count" envconfig:"COS_MAX_PARTS_COUNT"`
	Debug                  bool   `yaml:"debug" envconfig:"COS_DEBUG"`
}

// FTPConfig - ftp settings section
type FTPConfig struct {
	Address           string `yaml:"address" envconfig:"FTP_ADDRESS"`
	Timeout           string `yaml:"timeout" envconfig:"FTP_TIMEOUT"`
	Username          string `yaml:"username" envconfig:"FTP_USERNAME"`
	Password          string `yaml:"password" envconfig:"FTP_PASSWORD"`
	TLS               bool   `yaml:"tls" envconfig:"FTP_TLS"`
	SkipTLSVerify     bool   `yaml:"skip_tls_verify" envconfig:"FTP_SKIP_TLS_VERIFY"`
	Path              string `yaml:"path" envconfig:"FTP_PATH"`
	ObjectDiskPath    string `yaml:"object_disk_path" envconfig:"FTP_OBJECT_DISK_PATH"`
	CompressionFormat string `yaml:"compression_format" envconfig:"FTP_COMPRESSION_FORMAT"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"FTP_COMPRESSION_LEVEL"`
	Concurrency       uint8  `yaml:"concurrency" envconfig:"FTP_CONCURRENCY"`
	Debug             bool   `yaml:"debug" envconfig:"FTP_DEBUG"`
}

// SFTPConfig - sftp settings section
type SFTPConfig struct {
	Address           string `yaml:"address" envconfig:"SFTP_ADDRESS"`
	Port              uint   `yaml:"port" envconfig:"SFTP_PORT"`
	Username          string `yaml:"username" envconfig:"SFTP_USERNAME"`
	Password          string `yaml:"password" envconfig:"SFTP_PASSWORD"`
	Key               string `yaml:"key" envconfig:"SFTP_KEY"`
	Path              string `yaml:"path" envconfig:"SFTP_PATH"`
	ObjectDiskPath    string `yaml:"object_disk_path" envconfig:"SFTP_OBJECT_DISK_PATH"`
	CompressionFormat string `yaml:"compression_format" envconfig:"SFTP_COMPRESSION_FORMAT"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"SFTP_COMPRESSION_LEVEL"`
	Concurrency       int    `yaml:"concurrency" envconfig:"SFTP_CONCURRENCY"`
	Debug             bool   `yaml:"debug" envconfig:"SFTP_DEBUG"`
}

// CustomConfig - custom CLI storage settings section
type CustomConfig struct {
	UploadCommand          string `yaml:"upload_command" envconfig:"CUSTOM_UPLOAD_COMMAND"`
	DownloadCommand        string `yaml:"download_command" envconfig:"CUSTOM_DOWNLOAD_COMMAND"`
	ListCommand            string `yaml:"list_command" envconfig:"CUSTOM_LIST_COMMAND"`
	DeleteCommand          string `yaml:"delete_command" envconfig:"CUSTOM_DELETE_COMMAND"`
	CommandTimeout         string `yaml:"command_timeout" envconfig:"CUSTOM_COMMAND_TIMEOUT"`
	CommandTimeoutDuration time.Duration
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username                         string            `yaml:"username" envconfig:"CLICKHOUSE_USERNAME"`
	Password                         string            `yaml:"password" envconfig:"CLICKHOUSE_PASSWORD"`
	Host                             string            `yaml:"host" envconfig:"CLICKHOUSE_HOST"`
	Port                             uint              `yaml:"port" envconfig:"CLICKHOUSE_PORT"`
	DiskMapping                      map[string]string `yaml:"disk_mapping" envconfig:"CLICKHOUSE_DISK_MAPPING"`
	SkipTables                       []string          `yaml:"skip_tables" envconfig:"CLICKHOUSE_SKIP_TABLES"`
	SkipTableEngines                 []string          `yaml:"skip_table_engines" envconfig:"CLICKHOUSE_SKIP_TABLE_ENGINES"`
	SkipDisks                        []string          `yaml:"skip_disks" envconfig:"CLICKHOUSE_SKIP_DISKS"`
	SkipDiskTypes                    []string          `yaml:"skip_disk_types" envconfig:"CLICKHOUSE_SKIP_DISK_TYPES"`
	Timeout                          string            `yaml:"timeout" envconfig:"CLICKHOUSE_TIMEOUT"`
	FreezeByPart                     bool              `yaml:"freeze_by_part" envconfig:"CLICKHOUSE_FREEZE_BY_PART"`
	FreezeByPartWhere                string            `yaml:"freeze_by_part_where" envconfig:"CLICKHOUSE_FREEZE_BY_PART_WHERE"`
	UseEmbeddedBackupRestore         bool              `yaml:"use_embedded_backup_restore" envconfig:"CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE"`
	EmbeddedBackupDisk               string            `yaml:"embedded_backup_disk" envconfig:"CLICKHOUSE_EMBEDDED_BACKUP_DISK"`
	BackupMutations                  bool              `yaml:"backup_mutations" envconfig:"CLICKHOUSE_BACKUP_MUTATIONS"`
	RestoreAsAttach                  bool              `yaml:"restore_as_attach" envconfig:"CLICKHOUSE_RESTORE_AS_ATTACH"`
	CheckPartsColumns                bool              `yaml:"check_parts_columns" envconfig:"CLICKHOUSE_CHECK_PARTS_COLUMNS"`
	Secure                           bool              `yaml:"secure" envconfig:"CLICKHOUSE_SECURE"`
	SkipVerify                       bool              `yaml:"skip_verify" envconfig:"CLICKHOUSE_SKIP_VERIFY"`
	SyncReplicatedTables             bool              `yaml:"sync_replicated_tables" envconfig:"CLICKHOUSE_SYNC_REPLICATED_TABLES"`
	LogSQLQueries                    bool              `yaml:"log_sql_queries" envconfig:"CLICKHOUSE_LOG_SQL_QUERIES"`
	ConfigDir                        string            `yaml:"config_dir" envconfig:"CLICKHOUSE_CONFIG_DIR"`
	RestartCommand                   string            `yaml:"restart_command" envconfig:"CLICKHOUSE_RESTART_COMMAND"`
	IgnoreNotExistsErrorDuringFreeze bool              `yaml:"ignore_not_exists_error_during_freeze" envconfig:"CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE"`
	CheckReplicasBeforeAttach        bool              `yaml:"check_replicas_before_attach" envconfig:"CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH"`
	DefaultReplicaPath               string            `yaml:"default_replica_path" envconfig:"CLICKHOUSE_DEFAULT_REPLICA_PATH"`
	DefaultReplicaName               string            `yaml:"default_replica_name" envconfig:"CLICKHOUSE_DEFAULT_REPLICA_NAME"`
	TLSKey                           string            `yaml:"tls_key" envconfig:"CLICKHOUSE_TLS_KEY"`
	TLSCert                          string            `yaml:"tls_cert" envconfig:"CLICKHOUSE_TLS_CERT"`
	TLSCa                            string            `yaml:"tls_ca" envconfig:"CLICKHOUSE_TLS_CA"`
	MaxConnections                   int               `yaml:"max_connections" envconfig:"CLICKHOUSE_MAX_CONNECTIONS"`
	Debug                            bool              `yaml:"debug" envconfig:"CLICKHOUSE_DEBUG"`
}

type APIConfig struct {
	ListenAddr                    string `yaml:"listen" envconfig:"API_LISTEN"`
	EnableMetrics                 bool   `yaml:"enable_metrics" envconfig:"API_ENABLE_METRICS"`
	EnablePprof                   bool   `yaml:"enable_pprof" envconfig:"API_ENABLE_PPROF"`
	Username                      string `yaml:"username" envconfig:"API_USERNAME"`
	Password                      string `yaml:"password" envconfig:"API_PASSWORD"`
	Secure                        bool   `yaml:"secure" envconfig:"API_SECURE"`
	CertificateFile               string `yaml:"certificate_file" envconfig:"API_CERTIFICATE_FILE"`
	PrivateKeyFile                string `yaml:"private_key_file" envconfig:"API_PRIVATE_KEY_FILE"`
	CAKeyFile                     string `yaml:"ca_cert_file" envconfig:"API_CA_KEY_FILE"`
	CACertFile                    string `yaml:"ca_key_file" envconfig:"API_CA_CERT_FILE"`
	CreateIntegrationTables       bool   `yaml:"create_integration_tables" envconfig:"API_CREATE_INTEGRATION_TABLES"`
	IntegrationTablesHost         string `yaml:"integration_tables_host" envconfig:"API_INTEGRATION_TABLES_HOST"`
	AllowParallel                 bool   `yaml:"allow_parallel" envconfig:"API_ALLOW_PARALLEL"`
	CompleteResumableAfterRestart bool   `yaml:"complete_resumable_after_restart" envconfig:"API_COMPLETE_RESUMABLE_AFTER_RESTART"`
	WatchIsMainProcess            bool   `yaml:"watch_is_main_process" envconfig:"WATCH_IS_MAIN_PROCESS"`
}

// ArchiveExtensions - list of available compression formats and associated file extensions
var ArchiveExtensions = map[string]string{
	"tar":    "tar",
	"lz4":    "tar.lz4",
	"bzip2":  "tar.bz2",
	"gzip":   "tar.gz",
	"sz":     "tar.sz",
	"xz":     "tar.xz",
	"br":     "tar.br",
	"brotli": "tar.br",
	"zstd":   "tar.zstd",
}

func (cfg *Config) GetArchiveExtension() string {
	switch cfg.General.RemoteStorage {
	case "s3":
		return ArchiveExtensions[cfg.S3.CompressionFormat]
	case "gcs":
		return ArchiveExtensions[cfg.GCS.CompressionFormat]
	case "cos":
		return ArchiveExtensions[cfg.COS.CompressionFormat]
	case "ftp":
		return ArchiveExtensions[cfg.FTP.CompressionFormat]
	case "sftp":
		return ArchiveExtensions[cfg.SFTP.CompressionFormat]
	case "azblob":
		return ArchiveExtensions[cfg.AzureBlob.CompressionFormat]
	default:
		return ""
	}
}

func (cfg *Config) GetCompressionFormat() string {
	switch cfg.General.RemoteStorage {
	case "s3":
		return cfg.S3.CompressionFormat
	case "gcs":
		return cfg.GCS.CompressionFormat
	case "cos":
		return cfg.COS.CompressionFormat
	case "ftp":
		return cfg.FTP.CompressionFormat
	case "sftp":
		return cfg.SFTP.CompressionFormat
	case "azblob":
		return cfg.AzureBlob.CompressionFormat
	case "none", "custom":
		return "tar"
	default:
		return "unknown"
	}
}

var freezeByPartBeginAndRE = regexp.MustCompile(`(?im)^\s*AND\s+`)

// LoadConfig - load config from file + environment variables
func LoadConfig(configLocation string) (*Config, error) {
	cfg := DefaultConfig()
	configYaml, err := os.ReadFile(configLocation)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("can't open config file: %v", err)
	}
	if err := yaml.Unmarshal(configYaml, &cfg); err != nil {
		return nil, fmt.Errorf("can't parse config file: %v", err)
	}
	if err := envconfig.Process("", cfg); err != nil {
		return nil, err
	}

	//auto-tuning upload_concurrency for storage types which not have SDK level concurrency, https://github.com/Altinity/clickhouse-backup/issues/658
	cfgWithoutDefault := &Config{}
	if err := yaml.Unmarshal(configYaml, &cfgWithoutDefault); err != nil {
		return nil, fmt.Errorf("can't parse config file: %v", err)
	}
	if err := envconfig.Process("", cfgWithoutDefault); err != nil {
		return nil, err
	}
	if (cfg.General.RemoteStorage == "gcs" || cfg.General.RemoteStorage == "azblob" || cfg.General.RemoteStorage == "cos") && cfgWithoutDefault.General.UploadConcurrency == 0 {
		cfg.General.UploadConcurrency = uint8(runtime.NumCPU() / 2)
	}
	cfg.AzureBlob.Path = strings.Trim(cfg.AzureBlob.Path, "/ \t\r\n")
	cfg.S3.Path = strings.Trim(cfg.S3.Path, "/ \t\r\n")
	cfg.GCS.Path = strings.Trim(cfg.GCS.Path, "/ \t\r\n")
	cfg.COS.Path = strings.Trim(cfg.COS.Path, "/ \t\r\n")
	cfg.FTP.Path = strings.TrimRight(strings.Trim(cfg.FTP.Path, " \t\r\n"), "/")
	cfg.SFTP.Path = strings.TrimRight(strings.Trim(cfg.SFTP.Path, " \t\r\n"), "/")

	cfg.AzureBlob.ObjectDiskPath = strings.Trim(cfg.AzureBlob.ObjectDiskPath, "/ \t\n")
	cfg.S3.ObjectDiskPath = strings.Trim(cfg.S3.ObjectDiskPath, "/ \t\r\n")
	cfg.GCS.ObjectDiskPath = strings.Trim(cfg.GCS.ObjectDiskPath, "/ \t\r\n")
	cfg.COS.ObjectDiskPath = strings.Trim(cfg.COS.ObjectDiskPath, "/ \t\r\n")
	cfg.FTP.ObjectDiskPath = strings.TrimRight(strings.Trim(cfg.FTP.ObjectDiskPath, " \t\r\n"), "/")
	cfg.SFTP.ObjectDiskPath = strings.TrimRight(strings.Trim(cfg.SFTP.ObjectDiskPath, " \t\r\n"), "/")

	// https://github.com/Altinity/clickhouse-backup/issues/855
	if cfg.ClickHouse.FreezeByPart && cfg.ClickHouse.FreezeByPartWhere != "" && !freezeByPartBeginAndRE.MatchString(cfg.ClickHouse.FreezeByPartWhere) {
		cfg.ClickHouse.FreezeByPartWhere = " AND " + cfg.ClickHouse.FreezeByPartWhere
	}

	log_helper.SetLogLevelFromString(cfg.General.LogLevel)

	// https://github.com/Altinity/clickhouse-backup/issues/1086
	if cfg.General.RemoteStorage == "s3" && strings.Contains(cfg.S3.Endpoint, "backblaze") && cfg.S3.StorageClass != string(s3types.StorageClassStandard) {
		log.Warn().Str("endpoint", cfg.S3.Endpoint).Str("storageClass", cfg.S3.StorageClass).Msgf("unsopported STORAGE_CLASS, will use %s", string(s3types.StorageClassStandard))
		cfg.S3.StorageClass = string(s3types.StorageClassStandard)
	}
	if err = ValidateConfig(cfg); err != nil {
		return cfg, err
	}
	if err = cfg.SetPriority(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func ValidateConfig(cfg *Config) error {
	if cfg.General.RemoteStorage == "s3" {
		if _, err := aws.ParseRetryMode(cfg.S3.RetryMode); err != nil {
			return err
		}
	}
	if cfg.GetCompressionFormat() == "unknown" {
		return fmt.Errorf("'%s' is unknown remote storage", cfg.General.RemoteStorage)
	}
	if cfg.General.RemoteStorage == "ftp" && (cfg.FTP.Concurrency < cfg.General.DownloadConcurrency || cfg.FTP.Concurrency < cfg.General.UploadConcurrency) {
		return fmt.Errorf(
			"FTP_CONCURRENCY=%d should be great or equal than DOWNLOAD_CONCURRENCY=%d and UPLOAD_CONCURRENCY=%d",
			cfg.FTP.Concurrency, cfg.General.DownloadConcurrency, cfg.General.UploadConcurrency,
		)
	}
	if cfg.GetCompressionFormat() == "lz4" {
		return fmt.Errorf("clickhouse already compressed data by lz4")
	}
	if _, ok := ArchiveExtensions[cfg.GetCompressionFormat()]; !ok && cfg.GetCompressionFormat() != "none" {
		return fmt.Errorf("'%s' is unsupported compression format", cfg.GetCompressionFormat())
	}
	if timeout, err := time.ParseDuration(cfg.ClickHouse.Timeout); err != nil {
		return fmt.Errorf("invalid clickhouse timeout: %v", err)
	} else {
		if cfg.ClickHouse.UseEmbeddedBackupRestore && timeout < 240*time.Minute {
			return fmt.Errorf("clickhouse `timeout: %v`, not enough for `use_embedded_backup_restore: true`", cfg.ClickHouse.Timeout)
		}
	}
	if cfg.ClickHouse.FreezeByPart && cfg.ClickHouse.UseEmbeddedBackupRestore {
		return fmt.Errorf("`freeze_by_part: %v` is not compatible with `use_embedded_backup_restore: %v`", cfg.ClickHouse.FreezeByPart, cfg.ClickHouse.UseEmbeddedBackupRestore)
	}
	if _, err := time.ParseDuration(cfg.COS.Timeout); err != nil {
		return fmt.Errorf("invalid cos timeout: %v", err)
	}
	if _, err := time.ParseDuration(cfg.FTP.Timeout); err != nil {
		return fmt.Errorf("invalid ftp timeout: %v", err)
	}
	if _, err := time.ParseDuration(cfg.AzureBlob.Timeout); err != nil {
		return fmt.Errorf("invalid azblob timeout: %v", err)
	}
	if _, err := time.ParseDuration(cfg.AzureBlob.Timeout); err != nil {
		return fmt.Errorf("invalid azblob timeout: %v", err)
	}
	storageClassOk := false
	var allStorageClasses s3types.StorageClass
	if cfg.S3.UseCustomStorageClass {
		storageClassOk = true
	} else {
		for _, storageClass := range allStorageClasses.Values() {
			if s3types.StorageClass(strings.ToUpper(cfg.S3.StorageClass)) == storageClass {
				storageClassOk = true
				break
			}
		}
	}
	if !storageClassOk {
		return fmt.Errorf("'%s' is bad S3_STORAGE_CLASS, select one of: %#v",
			cfg.S3.StorageClass, allStorageClasses.Values())
	}
	if cfg.S3.AllowMultipartDownload && cfg.S3.Concurrency == 1 {
		return fmt.Errorf(
			"`allow_multipart_download` require `concurrency` in `s3` section more than 1 (3-4 recommends) current value: %d",
			cfg.S3.Concurrency,
		)
	}
	if cfg.API.Secure {
		if cfg.API.CertificateFile == "" {
			return fmt.Errorf("api.certificate_file must be defined")
		}
		if cfg.API.PrivateKeyFile == "" {
			return fmt.Errorf("api.private_key_file must be defined")
		}
		_, err := tls.LoadX509KeyPair(cfg.API.CertificateFile, cfg.API.PrivateKeyFile)
		if err != nil {
			return err
		}
	}
	if cfg.Custom.CommandTimeout != "" {
		if duration, err := time.ParseDuration(cfg.Custom.CommandTimeout); err != nil {
			return fmt.Errorf("invalid custom command timeout: %v", err)
		} else {
			cfg.Custom.CommandTimeoutDuration = duration
		}
	} else {
		return fmt.Errorf("empty custom command timeout")
	}
	if cfg.General.RetriesPause != "" {
		if duration, err := time.ParseDuration(cfg.General.RetriesPause); err != nil {
			return fmt.Errorf("invalid retries pause: %v", err)
		} else {
			cfg.General.RetriesDuration = duration
		}
	} else {
		return fmt.Errorf("empty retries pause")
	}
	if cfg.General.WatchInterval != "" {
		if duration, err := time.ParseDuration(cfg.General.WatchInterval); err != nil {
			return fmt.Errorf("invalid watch interval: %v", err)
		} else {
			cfg.General.WatchDuration = duration
		}
	}
	if cfg.General.FullInterval != "" {
		if duration, err := time.ParseDuration(cfg.General.FullInterval); err != nil {
			return fmt.Errorf("invalid full interval for watch: %v", err)
		} else {
			cfg.General.FullDuration = duration
		}
	}
	return nil
}

func ValidateObjectDiskConfig(cfg *Config) error {
	if !cfg.ClickHouse.UseEmbeddedBackupRestore {
		if cfg.General.RemoteStorage == "s3" && ((cfg.S3.ObjectDiskPath == "" && cfg.S3.Path == "") || (cfg.S3.ObjectDiskPath != "" && cfg.S3.Path == "") || (cfg.S3.Path != "" && strings.HasPrefix(cfg.S3.Path, cfg.S3.ObjectDiskPath))) {
			return fmt.Errorf("data in objects disks, invalid s3->object_disk_path config section, shall be not empty and shall not be prefix for s3->path, shall not inside s3->path if s3->path empty")
		}
		if cfg.General.RemoteStorage == "gcs" && ((cfg.GCS.ObjectDiskPath == "" && cfg.GCS.Path == "") || (cfg.GCS.ObjectDiskPath != "" && cfg.GCS.Path == "") || (cfg.GCS.Path != "" && strings.HasPrefix(cfg.GCS.Path, cfg.GCS.ObjectDiskPath))) {
			return fmt.Errorf("data in objects disks, invalid gcs->object_disk_path config section, shall be not empty and shall not be prefix for gcs->path, shall not inside gcs->path if gcs->path empty")
		}
		if cfg.General.RemoteStorage == "azblob" && ((cfg.AzureBlob.ObjectDiskPath == "" && cfg.AzureBlob.Path == "") || (cfg.AzureBlob.ObjectDiskPath != "" && cfg.AzureBlob.Path == "") || (cfg.AzureBlob.Path != "" && strings.HasPrefix(cfg.AzureBlob.Path, cfg.AzureBlob.ObjectDiskPath))) {
			return fmt.Errorf("data in objects disks, invalid azblob->object_disk_path config section, shall be not empty and shall not be prefix for azblob->path, shall not inside azblob->path if azblob->path empty")
		}
		if cfg.General.RemoteStorage == "cos" && ((cfg.COS.ObjectDiskPath == "" && cfg.COS.Path == "") || (cfg.COS.ObjectDiskPath != "" && cfg.COS.Path == "") || (cfg.COS.Path != "" && strings.HasPrefix(cfg.COS.Path, cfg.COS.ObjectDiskPath))) {
			return fmt.Errorf("data in objects disks, invalid cos->object_disk_path config section, shall be not empty and shall not be prefix for cos->path, shall not inside cos->path if cos->path empty")
		}
		if cfg.General.RemoteStorage == "ftp" && ((cfg.FTP.ObjectDiskPath == "" && cfg.FTP.Path == "") || (cfg.FTP.ObjectDiskPath != "" && cfg.FTP.Path == "") || (cfg.FTP.Path != "" && strings.HasPrefix(cfg.FTP.Path, cfg.FTP.ObjectDiskPath))) {
			return fmt.Errorf("data in objects disks, invalid ftp->object_disk_path config section, shall be not empty and shall not be prefix for ftp->path, shall not inside ftp->path if ftp->path empty")
		}
		if cfg.General.RemoteStorage == "sftp" && ((cfg.SFTP.ObjectDiskPath == "" && cfg.SFTP.Path == "") || (cfg.SFTP.ObjectDiskPath != "" && cfg.SFTP.Path == "") || (cfg.SFTP.Path != "" && strings.HasPrefix(cfg.SFTP.Path, cfg.SFTP.ObjectDiskPath))) {
			return fmt.Errorf("data in objects disks, invalid sftp->object_disk_path config section, shall be not empty and shall not be prefix for sftp->path, shall not inside sftp->path if sftp->path empty")
		}
	}
	return nil
}

// PrintConfig - print default / current config to stdout
func PrintConfig(ctx *cli.Context) error {
	var cfg *Config
	if ctx == nil {
		cfg = DefaultConfig()
	} else {
		cfg = GetConfigFromCli(ctx)
	}
	yml, _ := yaml.Marshal(&cfg)
	fmt.Print(string(yml))
	return nil
}

func DefaultConfig() *Config {
	uploadConcurrency := uint8(1)
	downloadConcurrency := uint8(1)
	if runtime.NumCPU() > 1 {
		uploadConcurrency = uint8(math.Round(math.Sqrt(float64(runtime.NumCPU() / 2))))
		downloadConcurrency = uint8(runtime.NumCPU() / 2)
	}
	if uploadConcurrency < 1 {
		uploadConcurrency = 1
	}
	if downloadConcurrency < 1 {
		downloadConcurrency = 1
	}
	objectDiskServerSideCopyConcurrency := uint8(32)
	return &Config{
		General: GeneralConfig{
			RemoteStorage:                       "none",
			MaxFileSize:                         0,
			BackupsToKeepLocal:                  0,
			BackupsToKeepRemote:                 0,
			LogLevel:                            "info",
			UploadConcurrency:                   uploadConcurrency,
			DownloadConcurrency:                 downloadConcurrency,
			ObjectDiskServerSideCopyConcurrency: objectDiskServerSideCopyConcurrency,
			RestoreSchemaOnCluster:              "",
			UploadByPart:                        true,
			DownloadByPart:                      true,
			UseResumableState:                   true,
			RetriesOnFailure:                    3,
			RetriesPause:                        "5s",
			RetriesDuration:                     5 * time.Second,
			WatchInterval:                       "1h",
			WatchDuration:                       1 * time.Hour,
			FullInterval:                        "24h",
			FullDuration:                        24 * time.Hour,
			WatchBackupNameTemplate:             "shard{shard}-{type}-{time:20060102150405}",
			RestoreDatabaseMapping:              make(map[string]string),
			RestoreTableMapping:                 make(map[string]string),
			IONicePriority:                      "idle",
			CPUNicePriority:                     15,
			RBACBackupAlways:                    true,
			RBACConflictResolution:              "recreate",
			NamedCollectionsBackupAlways:        false,
			BatchDeletion: BatchDeletionConfig{
				Enabled:          true,
				Workers:          0, // auto-detect
				BatchSize:        1000,
				RetryAttempts:    3,
				ErrorStrategy:    "continue",
				FailureThreshold: 0.1,
			},
		},
		ClickHouse: ClickHouseConfig{
			Username: "default",
			Password: "",
			Host:     "localhost",
			Port:     9000,
			SkipTables: []string{
				"system.*",
				"INFORMATION_SCHEMA.*",
				"information_schema.*",
				"_temporary_and_external_tables.*",
			},
			Timeout:                          "30m",
			SyncReplicatedTables:             false,
			LogSQLQueries:                    true,
			ConfigDir:                        "/etc/clickhouse-server/",
			RestartCommand:                   "exec:systemctl restart clickhouse-server",
			IgnoreNotExistsErrorDuringFreeze: true,
			CheckReplicasBeforeAttach:        true,
			UseEmbeddedBackupRestore:         false,
			BackupMutations:                  true,
			RestoreAsAttach:                  false,
			CheckPartsColumns:                true,
			DefaultReplicaPath:               "/clickhouse/tables/{cluster}/{shard}/{database}/{table}",
			DefaultReplicaName:               "{replica}",
			MaxConnections:                   int(downloadConcurrency),
		},
		AzureBlob: AzureBlobConfig{
			EndpointSchema:    "https",
			EndpointSuffix:    "core.windows.net",
			CompressionLevel:  1,
			CompressionFormat: "tar",
			MaxBuffers:        3,
			MaxPartsCount:     256,
			Timeout:           "4h",
			BatchDeletion: AzureBatchConfig{
				UseBatchAPI: true,
				MaxWorkers:  20,
			},
		},
		S3: S3Config{
			Region:                  "us-east-1",
			DisableSSL:              false,
			ACL:                     "private",
			AssumeRoleARN:           "",
			CompressionLevel:        1,
			CompressionFormat:       "tar",
			DisableCertVerification: false,
			UseCustomStorageClass:   false,
			StorageClass:            string(s3types.StorageClassStandard),
			Concurrency:             int(downloadConcurrency + 1),
			MaxPartsCount:           4000,
			RetryMode:               string(aws.RetryModeStandard),
			BatchDeletion: S3BatchConfig{
				UseBatchAPI:        true,
				VersionConcurrency: 10,
				PreloadVersions:    true,
			},
		},
		GCS: GCSConfig{
			CompressionLevel:  1,
			CompressionFormat: "tar",
			StorageClass:      "STANDARD",
			ClientPoolSize:    int(max(uploadConcurrency*3, downloadConcurrency*3, objectDiskServerSideCopyConcurrency)),
			BatchDeletion: GCSBatchConfig{
				MaxWorkers:    50,
				UseClientPool: true,
				UseBatchAPI:   true,
			},
		},
		COS: COSConfig{
			RowURL:                 "",
			Timeout:                "2m",
			SecretID:               "",
			SecretKey:              "",
			Path:                   "",
			CompressionFormat:      "tar",
			CompressionLevel:       1,
			Concurrency:            int(downloadConcurrency + 1),
			AllowMultipartDownload: false,
			MaxPartsCount:          1000,
		},
		API: APIConfig{
			ListenAddr:                    "localhost:7171",
			EnableMetrics:                 true,
			CompleteResumableAfterRestart: true,
		},
		FTP: FTPConfig{
			Timeout:           "2m",
			Concurrency:       downloadConcurrency * 3,
			CompressionFormat: "tar",
			CompressionLevel:  1,
		},
		SFTP: SFTPConfig{
			Port:              22,
			CompressionFormat: "tar",
			CompressionLevel:  1,
			Concurrency:       int(downloadConcurrency * 3),
		},
		Custom: CustomConfig{
			CommandTimeout:         "4h",
			CommandTimeoutDuration: 4 * time.Hour,
		},
	}
}

func GetConfigFromCli(ctx *cli.Context) *Config {
	oldEnvValues := OverrideEnvVars(ctx)
	configPath := GetConfigPath(ctx)
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatal().Stack().Err(err).Send()
	}
	RestoreEnvVars(oldEnvValues)
	return cfg
}

func GetConfigPath(ctx *cli.Context) string {
	if ctx.String("config") != DefaultConfigPath {
		return ctx.String("config")
	}
	if ctx.GlobalString("config") != DefaultConfigPath {
		return ctx.GlobalString("config")
	}
	if os.Getenv("CLICKHOUSE_BACKUP_CONFIG") != "" {
		return os.Getenv("CLICKHOUSE_BACKUP_CONFIG")
	}
	return DefaultConfigPath
}

type oldEnvValues struct {
	OldValue   string
	WasPresent bool
}

func OverrideEnvVars(ctx *cli.Context) map[string]oldEnvValues {
	env := ctx.StringSlice("env")
	oldValues := map[string]oldEnvValues{}
	logLevel := "info"
	if os.Getenv("LOG_LEVEL") != "" {
		logLevel = os.Getenv("LOG_LEVEL")
	}
	log_helper.SetLogLevelFromString(logLevel)
	if len(env) > 0 {
		processEnvFromCli := func(process func(envVariable []string)) {
			for _, v := range env {
				envVariable := strings.SplitN(v, "=", 2)
				if len(envVariable) < 2 {
					envVariable = append(envVariable, "true")
				}
				process(envVariable)
			}
		}

		processEnvFromCli(func(envVariable []string) {
			if envVariable[0] == "LOG_LEVEL" {
				log_helper.SetLogLevelFromString(envVariable[1])
			}
		})

		processEnvFromCli(func(envVariable []string) {
			log.Info().Msgf("override %s=%s", envVariable[0], envVariable[1])
			oldValue, wasPresent := os.LookupEnv(envVariable[0])
			oldValues[envVariable[0]] = oldEnvValues{
				OldValue:   oldValue,
				WasPresent: wasPresent,
			}
			if err := os.Setenv(envVariable[0], envVariable[1]); err != nil {
				log.Warn().Msgf("can't override %s=%s, error: %v", envVariable[0], envVariable[1], err)
			}
		})
	}
	return oldValues
}

func RestoreEnvVars(envVars map[string]oldEnvValues) {
	for name, oldEnv := range envVars {
		if oldEnv.WasPresent {
			if err := os.Setenv(name, oldEnv.OldValue); err != nil {
				log.Warn().Msgf("RestoreEnvVars can't restore %s=%s, error: %v", name, oldEnv.OldValue, err)
			}
		} else {
			if err := os.Unsetenv(name); err != nil {
				log.Warn().Msgf("RestoreEnvVars can't delete %s, error: %v", name, err)
			}
		}
	}
}
