package config

import (
	"crypto/tls"
	"fmt"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"math"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/kelseyhightower/envconfig"
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

// GeneralConfig - general setting section
type GeneralConfig struct {
	RemoteStorage           string            `yaml:"remote_storage" envconfig:"REMOTE_STORAGE"`
	MaxFileSize             int64             `yaml:"max_file_size" envconfig:"MAX_FILE_SIZE"`
	DisableProgressBar      bool              `yaml:"disable_progress_bar" envconfig:"DISABLE_PROGRESS_BAR"`
	BackupsToKeepLocal      int               `yaml:"backups_to_keep_local" envconfig:"BACKUPS_TO_KEEP_LOCAL"`
	BackupsToKeepRemote     int               `yaml:"backups_to_keep_remote" envconfig:"BACKUPS_TO_KEEP_REMOTE"`
	LogLevel                string            `yaml:"log_level" envconfig:"LOG_LEVEL"`
	AllowEmptyBackups       bool              `yaml:"allow_empty_backups" envconfig:"ALLOW_EMPTY_BACKUPS"`
	DownloadConcurrency     uint8             `yaml:"download_concurrency" envconfig:"DOWNLOAD_CONCURRENCY"`
	UploadConcurrency       uint8             `yaml:"upload_concurrency" envconfig:"UPLOAD_CONCURRENCY"`
	UseResumableState       bool              `yaml:"use_resumable_state" envconfig:"USE_RESUMABLE_STATE"`
	RestoreSchemaOnCluster  string            `yaml:"restore_schema_on_cluster" envconfig:"RESTORE_SCHEMA_ON_CLUSTER"`
	UploadByPart            bool              `yaml:"upload_by_part" envconfig:"UPLOAD_BY_PART"`
	DownloadByPart          bool              `yaml:"download_by_part" envconfig:"DOWNLOAD_BY_PART"`
	RestoreDatabaseMapping  map[string]string `yaml:"restore_database_mapping" envconfig:"RESTORE_DATABASE_MAPPING"`
	RetriesOnFailure        int               `yaml:"retries_on_failure" envconfig:"RETRIES_ON_FAILURE"`
	RetriesPause            string            `yaml:"upload_retries_pause" envconfig:"RETRIES_PAUSE"`
	WatchInterval           string            `yaml:"watch_interval" envconfig:"WATCH_INTERVAL"`
	FullInterval            string            `yaml:"full_interval" envconfig:"FULL_INTERVAL"`
	WatchBackupNameTemplate string            `yaml:"watch_backup_name_template" envconfig:"WATCH_BACKUP_NAME_TEMPLATE"`
	RetriesDuration         time.Duration
	WatchDuration           time.Duration
	FullDuration            time.Duration
}

// GCSConfig - GCS settings section
type GCSConfig struct {
	CredentialsFile        string            `yaml:"credentials_file" envconfig:"GCS_CREDENTIALS_FILE"`
	CredentialsJSON        string            `yaml:"credentials_json" envconfig:"GCS_CREDENTIALS_JSON"`
	CredentialsJSONEncoded string            `yaml:"credentials_json_encoded" envconfig:"GCS_CREDENTIALS_JSON_ENCODED"`
	Bucket                 string            `yaml:"bucket" envconfig:"GCS_BUCKET"`
	Path                   string            `yaml:"path" envconfig:"GCS_PATH"`
	CompressionLevel       int               `yaml:"compression_level" envconfig:"GCS_COMPRESSION_LEVEL"`
	CompressionFormat      string            `yaml:"compression_format" envconfig:"GCS_COMPRESSION_FORMAT"`
	Debug                  bool              `yaml:"debug" envconfig:"GCS_DEBUG"`
	Endpoint               string            `yaml:"endpoint" envconfig:"GCS_ENDPOINT"`
	StorageClass           string            `yaml:"storage_class" envconfig:"GCS_STORAGE_CLASS"`
	ObjectLabels           map[string]string `yaml:"object_labels" envconfig:"S3_OBJECT_LABELS"`
}

// AzureBlobConfig - Azure Blob settings section
type AzureBlobConfig struct {
	EndpointSchema        string `yaml:"endpoint_schema" envconfig:"AZBLOB_ENDPOINT_SCHEMA"`
	EndpointSuffix        string `yaml:"endpoint_suffix" envconfig:"AZBLOB_ENDPOINT_SUFFIX"`
	AccountName           string `yaml:"account_name" envconfig:"AZBLOB_ACCOUNT_NAME"`
	AccountKey            string `yaml:"account_key" envconfig:"AZBLOB_ACCOUNT_KEY"`
	SharedAccessSignature string `yaml:"sas" envconfig:"AZBLOB_SAS"`
	UseManagedIdentity    bool   `yaml:"use_managed_identity" envconfig:"AZBLOB_USE_MANAGED_IDENTITY"`
	Container             string `yaml:"container" envconfig:"AZBLOB_CONTAINER"`
	Path                  string `yaml:"path" envconfig:"AZBLOB_PATH"`
	CompressionLevel      int    `yaml:"compression_level" envconfig:"AZBLOB_COMPRESSION_LEVEL"`
	CompressionFormat     string `yaml:"compression_format" envconfig:"AZBLOB_COMPRESSION_FORMAT"`
	SSEKey                string `yaml:"sse_key" envconfig:"AZBLOB_SSE_KEY"`
	BufferSize            int    `yaml:"buffer_size" envconfig:"AZBLOB_BUFFER_SIZE"`
	MaxBuffers            int    `yaml:"buffer_count" envconfig:"AZBLOB_MAX_BUFFERS"`
	MaxPartsCount         int    `yaml:"max_parts_count" envconfig:"AZBLOB_MAX_PARTS_COUNT"`
	Timeout               string `yaml:"timeout" envconfig:"AZBLOB_TIMEOUT"`
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
	DisableSSL              bool              `yaml:"disable_ssl" envconfig:"S3_DISABLE_SSL"`
	CompressionLevel        int               `yaml:"compression_level" envconfig:"S3_COMPRESSION_LEVEL"`
	CompressionFormat       string            `yaml:"compression_format" envconfig:"S3_COMPRESSION_FORMAT"`
	SSE                     string            `yaml:"sse" envconfig:"S3_SSE"`
	DisableCertVerification bool              `yaml:"disable_cert_verification" envconfig:"S3_DISABLE_CERT_VERIFICATION"`
	UseCustomStorageClass   bool              `yaml:"use_custom_storage_class" envconfig:"S3_USE_CUSTOM_STORAGE_CLASS"`
	StorageClass            string            `yaml:"storage_class" envconfig:"S3_STORAGE_CLASS"`
	Concurrency             int               `yaml:"concurrency" envconfig:"S3_CONCURRENCY"`
	PartSize                int64             `yaml:"part_size" envconfig:"S3_PART_SIZE"`
	MaxPartsCount           int64             `yaml:"max_parts_count" envconfig:"S3_MAX_PARTS_COUNT"`
	AllowMultipartDownload  bool              `yaml:"allow_multipart_download" envconfig:"S3_ALLOW_MULTIPART_DOWNLOAD"`
	ObjectLabels            map[string]string `yaml:"object_labels" envconfig:"S3_OBJECT_LABELS"`
	Debug                   bool              `yaml:"debug" envconfig:"S3_DEBUG"`
}

// COSConfig - cos settings section
type COSConfig struct {
	RowURL            string `yaml:"url" envconfig:"COS_URL"`
	Timeout           string `yaml:"timeout" envconfig:"COS_TIMEOUT"`
	SecretID          string `yaml:"secret_id" envconfig:"COS_SECRET_ID"`
	SecretKey         string `yaml:"secret_key" envconfig:"COS_SECRET_KEY"`
	Path              string `yaml:"path" envconfig:"COS_PATH"`
	CompressionFormat string `yaml:"compression_format" envconfig:"COS_COMPRESSION_FORMAT"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"COS_COMPRESSION_LEVEL"`
	Debug             bool   `yaml:"debug" envconfig:"COS_DEBUG"`
}

// FTPConfig - ftp settings section
type FTPConfig struct {
	Address           string `yaml:"address" envconfig:"FTP_ADDRESS"`
	Timeout           string `yaml:"timeout" envconfig:"FTP_TIMEOUT"`
	Username          string `yaml:"username" envconfig:"FTP_USERNAME"`
	Password          string `yaml:"password" envconfig:"FTP_PASSWORD"`
	TLS               bool   `yaml:"tls" envconfig:"FTP_TLS"`
	Path              string `yaml:"path" envconfig:"FTP_PATH"`
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
	Timeout                          string            `yaml:"timeout" envconfig:"CLICKHOUSE_TIMEOUT"`
	FreezeByPart                     bool              `yaml:"freeze_by_part" envconfig:"CLICKHOUSE_FREEZE_BY_PART"`
	FreezeByPartWhere                string            `yaml:"freeze_by_part_where" envconfig:"CLICKHOUSE_FREEZE_BY_PART_WHERE"`
	UseEmbeddedBackupRestore         bool              `yaml:"use_embedded_backup_restore" envconfig:"CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE"`
	EmbeddedBackupDisk               string            `yaml:"embedded_backup_disk" envconfig:"CLICKHOUSE_EMBEDDED_BACKUP_DISK"`
	Secure                           bool              `yaml:"secure" envconfig:"CLICKHOUSE_SECURE"`
	SkipVerify                       bool              `yaml:"skip_verify" envconfig:"CLICKHOUSE_SKIP_VERIFY"`
	SyncReplicatedTables             bool              `yaml:"sync_replicated_tables" envconfig:"CLICKHOUSE_SYNC_REPLICATED_TABLES"`
	LogSQLQueries                    bool              `yaml:"log_sql_queries" envconfig:"CLICKHOUSE_LOG_SQL_QUERIES"`
	ConfigDir                        string            `yaml:"config_dir" envconfig:"CLICKHOUSE_CONFIG_DIR"`
	RestartCommand                   string            `yaml:"restart_command" envconfig:"CLICKHOUSE_RESTART_COMMAND"`
	IgnoreNotExistsErrorDuringFreeze bool              `yaml:"ignore_not_exists_error_during_freeze" envconfig:"CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE"`
	CheckReplicasBeforeAttach        bool              `yaml:"check_replicas_before_attach" envconfig:"CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH"`
	TLSKey                           string            `yaml:"tls_key" envconfig:"CLICKHOUSE_TLS_KEY"`
	TLSCert                          string            `yaml:"tls_cert" envconfig:"CLICKHOUSE_TLS_CERT"`
	TLSCa                            string            `yaml:"tls_ca" envconfig:"CLICKHOUSE_TLS_CA"`
	Debug                            bool              `yaml:"debug" envconfig:"CLICKHOUSE_DEBUG"`
}

type APIConfig struct {
	ListenAddr              string `yaml:"listen" envconfig:"API_LISTEN"`
	EnableMetrics           bool   `yaml:"enable_metrics" envconfig:"API_ENABLE_METRICS"`
	EnablePprof             bool   `yaml:"enable_pprof" envconfig:"API_ENABLE_PPROF"`
	Username                string `yaml:"username" envconfig:"API_USERNAME"`
	Password                string `yaml:"password" envconfig:"API_PASSWORD"`
	Secure                  bool   `yaml:"secure" envconfig:"API_SECURE"`
	CertificateFile         string `yaml:"certificate_file" envconfig:"API_CERTIFICATE_FILE"`
	PrivateKeyFile          string `yaml:"private_key_file" envconfig:"API_PRIVATE_KEY_FILE"`
	CreateIntegrationTables bool   `yaml:"create_integration_tables" envconfig:"API_CREATE_INTEGRATION_TABLES"`
	IntegrationTablesHost   string `yaml:"integration_tables_host" envconfig:"API_INTEGRATION_TABLES_HOST"`
	AllowParallel           bool   `yaml:"allow_parallel" envconfig:"API_ALLOW_PARALLEL"`
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
	cfg.AzureBlob.Path = strings.TrimPrefix(cfg.AzureBlob.Path, "/")
	cfg.S3.Path = strings.TrimPrefix(cfg.S3.Path, "/")
	cfg.GCS.Path = strings.TrimPrefix(cfg.GCS.Path, "/")
	log.SetLevelFromString(cfg.General.LogLevel)
	return cfg, ValidateConfig(cfg)
}

func ValidateConfig(cfg *Config) error {
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
	availableConcurrency := uint8(1)
	if runtime.NumCPU() > 1 {
		availableConcurrency = uint8(math.Round(math.Sqrt(float64(runtime.NumCPU() / 2))))
	}
	return &Config{
		General: GeneralConfig{
			RemoteStorage:           "none",
			MaxFileSize:             0,
			BackupsToKeepLocal:      0,
			BackupsToKeepRemote:     0,
			LogLevel:                "info",
			DisableProgressBar:      true,
			UploadConcurrency:       availableConcurrency,
			DownloadConcurrency:     availableConcurrency,
			RestoreSchemaOnCluster:  "",
			UploadByPart:            true,
			DownloadByPart:          true,
			UseResumableState:       false,
			RetriesOnFailure:        3,
			RetriesPause:            "30s",
			RetriesDuration:         100 * time.Millisecond,
			WatchInterval:           "1h",
			WatchDuration:           1 * time.Hour,
			FullInterval:            "24h",
			FullDuration:            24 * time.Hour,
			WatchBackupNameTemplate: "shard{shard}-{type}-{time:20060102150405}",
			RestoreDatabaseMapping:  make(map[string]string, 0),
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
			Timeout:                          "5m",
			SyncReplicatedTables:             false,
			LogSQLQueries:                    true,
			ConfigDir:                        "/etc/clickhouse-server/",
			RestartCommand:                   "systemctl restart clickhouse-server",
			IgnoreNotExistsErrorDuringFreeze: true,
			CheckReplicasBeforeAttach:        true,
			UseEmbeddedBackupRestore:         false,
		},
		AzureBlob: AzureBlobConfig{
			EndpointSchema:    "https",
			EndpointSuffix:    "core.windows.net",
			CompressionLevel:  1,
			CompressionFormat: "tar",
			BufferSize:        0,
			MaxBuffers:        3,
			MaxPartsCount:     10000,
			Timeout:           "15m",
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
			Concurrency:             1,
			PartSize:                0,
			MaxPartsCount:           10000,
		},
		GCS: GCSConfig{
			CompressionLevel:  1,
			CompressionFormat: "tar",
			StorageClass:      "STANDARD",
		},
		COS: COSConfig{
			RowURL:            "",
			Timeout:           "2m",
			SecretID:          "",
			SecretKey:         "",
			Path:              "",
			CompressionFormat: "tar",
			CompressionLevel:  1,
		},
		API: APIConfig{
			ListenAddr:    "localhost:7171",
			EnableMetrics: true,
		},
		FTP: FTPConfig{
			Timeout:           "2m",
			Concurrency:       availableConcurrency,
			CompressionFormat: "tar",
			CompressionLevel:  1,
		},
		SFTP: SFTPConfig{
			Port:              22,
			CompressionFormat: "tar",
			CompressionLevel:  1,
			Concurrency:       1,
		},
		Custom: CustomConfig{
			CommandTimeout:         "4h",
			CommandTimeoutDuration: 4 * time.Hour,
		},
	}
}

func GetConfigFromCli(ctx *cli.Context) *Config {
	configPath := GetConfigPath(ctx)
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatal(err.Error())
	}
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
