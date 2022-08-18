package config

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kelseyhightower/envconfig"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

const (
	DefaultConfigPath = "/etc/clickhouse-backup/config.yml"
)

// Config - config file format
type Config struct {
	General    GeneralConfig    `yaml:"general" envconfig:"_" json:"general"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse" envconfig:"_" json:"clickhouse"`
	S3         S3Config         `yaml:"s3" envconfig:"_" json:"s3"`
	GCS        GCSConfig        `yaml:"gcs" envconfig:"_" json:"gcs"`
	COS        COSConfig        `yaml:"cos" envconfig:"_" json:"cos"`
	API        APIConfig        `yaml:"api" envconfig:"_" json:"api"`
	FTP        FTPConfig        `yaml:"ftp" envconfig:"_" json:"ftp"`
	SFTP       SFTPConfig       `yaml:"sftp" envconfig:"_" json:"sftp"`
	AzureBlob  AzureBlobConfig  `yaml:"azblob" envconfig:"_" json:"azblob"`
	Custom     CustomConfig     `yaml:"custom" envconfig:"_" json:"custom"`
}

// GeneralConfig - general setting section
type GeneralConfig struct {
	RemoteStorage          string            `yaml:"remote_storage" envconfig:"REMOTE_STORAGE" json:"remote_storage"`
	MaxFileSize            int64             `yaml:"max_file_size" envconfig:"MAX_FILE_SIZE" json:"max_file_size"`
	DisableProgressBar     bool              `yaml:"disable_progress_bar" envconfig:"DISABLE_PROGRESS_BAR" json:"disable_progress_bar"`
	BackupsToKeepLocal     int               `yaml:"backups_to_keep_local" envconfig:"BACKUPS_TO_KEEP_LOCAL" json:"backups_to_keep_local"`
	BackupsToKeepRemote    int               `yaml:"backups_to_keep_remote" envconfig:"BACKUPS_TO_KEEP_REMOTE" json:"backups_to_keep_remote"`
	LogLevel               string            `yaml:"log_level" envconfig:"LOG_LEVEL" json:"log_level"`
	AllowEmptyBackups      bool              `yaml:"allow_empty_backups" envconfig:"ALLOW_EMPTY_BACKUPS" json:"allow_empty_backups"`
	DownloadConcurrency    uint8             `yaml:"download_concurrency" envconfig:"DOWNLOAD_CONCURRENCY" json:"download_concurrency"`
	UploadConcurrency      uint8             `yaml:"upload_concurrency" envconfig:"UPLOAD_CONCURRENCY" json:"upload_concurrency"`
	RestoreSchemaOnCluster string            `yaml:"restore_schema_on_cluster" envconfig:"RESTORE_SCHEMA_ON_CLUSTER" json:"restore_schema_on_cluster"`
	UploadByPart           bool              `yaml:"upload_by_part" envconfig:"UPLOAD_BY_PART" json:"upload_by_part"`
	DownloadByPart         bool              `yaml:"download_by_part" envconfig:"DOWNLOAD_BY_PART" json:"download_by_part"`
	RestoreDatabaseMapping map[string]string `yaml:"restore_database_mapping" envconfig:"RESTORE_DATABASE_MAPPING" json:"restore_database_mapping"`
}

// GCSConfig - GCS settings section
type GCSConfig struct {
	CredentialsFile   string `yaml:"credentials_file" envconfig:"GCS_CREDENTIALS_FILE" json:"credentials_file"`
	CredentialsJSON   string `yaml:"credentials_json" envconfig:"GCS_CREDENTIALS_JSON" json:"credentials_json"`
	Bucket            string `yaml:"bucket" envconfig:"GCS_BUCKET" json:"bucket"`
	Path              string `yaml:"path" envconfig:"GCS_PATH" json:"path"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"GCS_COMPRESSION_LEVEL" json:"compression_level"`
	CompressionFormat string `yaml:"compression_format" envconfig:"GCS_COMPRESSION_FORMAT" json:"compression_format"`
	Debug             bool   `yaml:"debug" envconfig:"GCS_DEBUG" json:"debug"`
	Endpoint          string `yaml:"endpoint" envconfig:"GCS_ENDPOINT" json:"endpoint"`
}

// AzureBlobConfig - Azure Blob settings section
type AzureBlobConfig struct {
	EndpointSuffix        string `yaml:"endpoint_suffix" envconfig:"AZBLOB_ENDPOINT_SUFFIX" json:"endpoint"`
	AccountName           string `yaml:"account_name" envconfig:"AZBLOB_ACCOUNT_NAME" json:"account_name"`
	AccountKey            string `yaml:"account_key" envconfig:"AZBLOB_ACCOUNT_KEY" json:"account_key"`
	SharedAccessSignature string `yaml:"sas" envconfig:"AZBLOB_SAS" json:"sas"`
	UseManagedIdentity    bool   `yaml:"use_managed_identity" envconfig:"AZBLOB_USE_MANAGED_IDENTITY" json:"use_managed_identity"`
	Container             string `yaml:"container" envconfig:"AZBLOB_CONTAINER" json:"container"`
	Path                  string `yaml:"path" envconfig:"AZBLOB_PATH" json:"path"`
	CompressionLevel      int    `yaml:"compression_level" envconfig:"AZBLOB_COMPRESSION_LEVEL" json:"compression_level"`
	CompressionFormat     string `yaml:"compression_format" envconfig:"AZBLOB_COMPRESSION_FORMAT" json:"compression_format"`
	SSEKey                string `yaml:"sse_key" envconfig:"AZBLOB_SSE_KEY" json:"sse_key"`
	BufferSize            int    `yaml:"buffer_size" envconfig:"AZBLOB_BUFFER_SIZE" json:"buffer_size"`
	MaxBuffers            int    `yaml:"buffer_count" envconfig:"AZBLOB_MAX_BUFFERS" json:"buffer_count"`
	MaxPartsCount         int    `yaml:"max_parts_count" envconfig:"AZBLOB_MAX_PARTS_COUNT" json:"max_parts_count"`
}

// S3Config - s3 settings section
type S3Config struct {
	AccessKey               string `yaml:"access_key" envconfig:"S3_ACCESS_KEY" json:"access_key"`
	SecretKey               string `yaml:"secret_key" envconfig:"S3_SECRET_KEY" json:"secret_key"`
	Bucket                  string `yaml:"bucket" envconfig:"S3_BUCKET" json:"bucket"`
	Endpoint                string `yaml:"endpoint" envconfig:"S3_ENDPOINT" json:"endpoint"`
	Region                  string `yaml:"region" envconfig:"S3_REGION" json:"region"`
	ACL                     string `yaml:"acl" envconfig:"S3_ACL" json:"acl"`
	AssumeRoleARN           string `yaml:"assume_role_arn" envconfig:"S3_ASSUME_ROLE_ARN" json:"assume_role_arn"`
	ForcePathStyle          bool   `yaml:"force_path_style" envconfig:"S3_FORCE_PATH_STYLE" json:"force_path_style"`
	Path                    string `yaml:"path" envconfig:"S3_PATH" json:"path"`
	DisableSSL              bool   `yaml:"disable_ssl" envconfig:"S3_DISABLE_SSL" json:"disable_ssl"`
	CompressionLevel        int    `yaml:"compression_level" envconfig:"S3_COMPRESSION_LEVEL" json:"compression_level"`
	CompressionFormat       string `yaml:"compression_format" envconfig:"S3_COMPRESSION_FORMAT" json:"compression_format"`
	SSE                     string `yaml:"sse" envconfig:"S3_SSE" json:"sse"`
	DisableCertVerification bool   `yaml:"disable_cert_verification" envconfig:"S3_DISABLE_CERT_VERIFICATION" json:"disable_cert_verification"`
	StorageClass            string `yaml:"storage_class" envconfig:"S3_STORAGE_CLASS" json:"storage_class"`
	Concurrency             int    `yaml:"concurrency" envconfig:"S3_CONCURRENCY" json:"concurrency"`
	PartSize                int64  `yaml:"part_size" envconfig:"S3_PART_SIZE" json:"part_size"`
	MaxPartsCount           int64  `yaml:"max_parts_count" envconfig:"S3_MAX_PARTS_COUNT" json:"max_parts_count"`
	AllowMultipartDownload  bool   `yaml:"allow_multipart_download" envconfig:"S3_ALLOW_MULTIPART_DOWNLOAD" json:"allow_multipart_download"`
	Debug                   bool   `yaml:"debug" envconfig:"S3_DEBUG" json:"debug"`
}

// COSConfig - cos settings section
type COSConfig struct {
	RowURL            string `yaml:"url" envconfig:"COS_URL" json:"url"`
	Timeout           string `yaml:"timeout" envconfig:"COS_TIMEOUT" json:"timeout"`
	SecretID          string `yaml:"secret_id" envconfig:"COS_SECRET_ID" json:"secret_id"`
	SecretKey         string `yaml:"secret_key" envconfig:"COS_SECRET_KEY" json:"secret_key"`
	Path              string `yaml:"path" envconfig:"COS_PATH" json:"path"`
	CompressionFormat string `yaml:"compression_format" envconfig:"COS_COMPRESSION_FORMAT" json:"compression_format"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"COS_COMPRESSION_LEVEL" json:"compression_level"`
	Debug             bool   `yaml:"debug" envconfig:"COS_DEBUG" json:"debug"`
}

// FTPConfig - ftp settings section
type FTPConfig struct {
	Address           string `yaml:"address" envconfig:"FTP_ADDRESS" json:"address"`
	Timeout           string `yaml:"timeout" envconfig:"FTP_TIMEOUT" json:"timeout"`
	Username          string `yaml:"username" envconfig:"FTP_USERNAME" json:"username"`
	Password          string `yaml:"password" envconfig:"FTP_PASSWORD" json:"password"`
	TLS               bool   `yaml:"tls" envconfig:"FTP_TLS" json:"tls"`
	Path              string `yaml:"path" envconfig:"FTP_PATH" json:"path"`
	CompressionFormat string `yaml:"compression_format" envconfig:"FTP_COMPRESSION_FORMAT" json:"compression_format"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"FTP_COMPRESSION_LEVEL" json:"compression_level"`
	Concurrency       uint8  `yaml:"concurrency" envconfig:"FTP_CONCURRENCY" json:"concurrency"`
	Debug             bool   `yaml:"debug" envconfig:"FTP_DEBUG" json:"debug"`
}

// SFTPConfig - sftp settings section
type SFTPConfig struct {
	Address           string `yaml:"address" envconfig:"SFTP_ADDRESS" json:"address"`
	Port              uint   `yaml:"port" envconfig:"SFTP_PORT" json:"port"`
	Username          string `yaml:"username" envconfig:"SFTP_USERNAME" json:"username"`
	Password          string `yaml:"password" envconfig:"SFTP_PASSWORD" json:"password"`
	Key               string `yaml:"key" envconfig:"SFTP_KEY" json:"key"`
	Path              string `yaml:"path" envconfig:"SFTP_PATH" json:"path"`
	CompressionFormat string `yaml:"compression_format" envconfig:"SFTP_COMPRESSION_FORMAT" json:"compression_format"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"SFTP_COMPRESSION_LEVEL" json:"compression_level"`
	Concurrency       int    `yaml:"concurrency" envconfig:"SFTP_CONCURRENCY" json:"concurrency"`
	Debug             bool   `yaml:"debug" envconfig:"SFTP_DEBUG" json:"debug"`
}

// CustomConfig - custom CLI storage settings section
type CustomConfig struct {
	UploadCommand          string `yaml:"upload_command" envconfig:"CUSTOM_UPLOAD_COMMAND" json:"upload_command"`
	DownloadCommand        string `yaml:"download_command" envconfig:"CUSTOM_DOWNLOAD_COMMAND" json:"download_command"`
	ListCommand            string `yaml:"list_command" envconfig:"CUSTOM_LIST_COMMAND" json:"list_command"`
	DeleteCommand          string `yaml:"delete_command" envconfig:"CUSTOM_DELETE_COMMAND" json:"delete_command"`
	CommandTimeout         string `yaml:"command_timeout" envconfig:"CUSTOM_COMMAND_TIMEOUT" json:"command_timeout"`
	CommandTimeoutDuration time.Duration
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username                         string            `yaml:"username" envconfig:"CLICKHOUSE_USERNAME" json:"username"`
	Password                         string            `yaml:"password" envconfig:"CLICKHOUSE_PASSWORD" json:"password"`
	Host                             string            `yaml:"host" envconfig:"CLICKHOUSE_HOST" json:"host"`
	Port                             uint              `yaml:"port" envconfig:"CLICKHOUSE_PORT" json:"port"`
	DiskMapping                      map[string]string `yaml:"disk_mapping" envconfig:"CLICKHOUSE_DISK_MAPPING" json:"disk_mapping"`
	SkipTables                       []string          `yaml:"skip_tables" envconfig:"CLICKHOUSE_SKIP_TABLES" json:"skip_tables"`
	Timeout                          string            `yaml:"timeout" envconfig:"CLICKHOUSE_TIMEOUT" json:"timeout"`
	FreezeByPart                     bool              `yaml:"freeze_by_part" envconfig:"CLICKHOUSE_FREEZE_BY_PART" json:"freeze_by_part"`
	FreezeByPartWhere                string            `yaml:"freeze_by_part_where" envconfig:"CLICKHOUSE_FREEZE_BY_PART_WHERE" json:"freeze_by_part_where"`
	Secure                           bool              `yaml:"secure" envconfig:"CLICKHOUSE_SECURE" json:"secure"`
	SkipVerify                       bool              `yaml:"skip_verify" envconfig:"CLICKHOUSE_SKIP_VERIFY" json:"skip_verify"`
	SyncReplicatedTables             bool              `yaml:"sync_replicated_tables" envconfig:"CLICKHOUSE_SYNC_REPLICATED_TABLES" json:"sync_replicated_tables"`
	LogSQLQueries                    bool              `yaml:"log_sql_queries" envconfig:"CLICKHOUSE_LOG_SQL_QUERIES" json:"log_sql_queries"`
	ConfigDir                        string            `yaml:"config_dir" envconfig:"CLICKHOUSE_CONFIG_DIR" json:"config_dir"`
	RestartCommand                   string            `yaml:"restart_command" envconfig:"CLICKHOUSE_RESTART_COMMAND" json:"restart_command"`
	IgnoreNotExistsErrorDuringFreeze bool              `yaml:"ignore_not_exists_error_during_freeze" envconfig:"CLICKHOUSE_IGNORE_NOT_EXISTS_ERROR_DURING_FREEZE" json:"ignore_not_exists_error_during_freeze"`
	TLSKey                           string            `yaml:"tls_key" envconfig:"CLICKHOUSE_TLS_KEY" json:"tls_key"`
	TLSCert                          string            `yaml:"tls_cert" envconfig:"CLICKHOUSE_TLS_CERT" json:"tls_cert"`
	TLSCa                            string            `yaml:"tls_ca" envconfig:"CLICKHOUSE_TLS_CA" json:"tls_ca"`
	Debug                            bool              `yaml:"debug" envconfig:"CLICKHOUSE_DEBUG" json:"debug"`
}

type APIConfig struct {
	ListenAddr              string `yaml:"listen" envconfig:"API_LISTEN" json:"listen"`
	EnableMetrics           bool   `yaml:"enable_metrics" envconfig:"API_ENABLE_METRICS" json:"enable_metrics"`
	EnablePprof             bool   `yaml:"enable_pprof" envconfig:"API_ENABLE_PPROF" json:"enable_pprof"`
	Username                string `yaml:"username" envconfig:"API_USERNAME" json:"username"`
	Password                string `yaml:"password" envconfig:"API_PASSWORD" json:"password"`
	Secure                  bool   `yaml:"secure" envconfig:"API_SECURE" json:"secure"`
	CertificateFile         string `yaml:"certificate_file" envconfig:"API_CERTIFICATE_FILE" json:"certificate_file"`
	PrivateKeyFile          string `yaml:"private_key_file" envconfig:"API_PRIVATE_KEY_FILE" json:"private_key_file"`
	CreateIntegrationTables bool   `yaml:"create_integration_tables" envconfig:"API_CREATE_INTEGRATION_TABLES" json:"create_integration_tables"`
	IntegrationTablesHost   string `yaml:"integration_tables_host" envconfig:"API_INTEGRATION_TABLES_HOST" json:"integration_tables_host"`
	AllowParallel           bool   `yaml:"allow_parallel" envconfig:"API_ALLOW_PARALLEL" json:"allow_parallel"`
}

// ArchiveExtensions - list of availiable compression formats and associated file extensions
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
	configYaml, err := ioutil.ReadFile(configLocation)
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
	if _, err := time.ParseDuration(cfg.ClickHouse.Timeout); err != nil {
		return fmt.Errorf("invalid clickhouse timeout: %v", err)
	}
	if _, err := time.ParseDuration(cfg.COS.Timeout); err != nil {
		return fmt.Errorf("invalid cos timeout: %v", err)
	}
	if _, err := time.ParseDuration(cfg.FTP.Timeout); err != nil {
		return fmt.Errorf("invalid ftp timeout: %v", err)
	}
	storageClassOk := false
	for _, storageClass := range s3.StorageClass_Values() {
		if strings.ToUpper(cfg.S3.StorageClass) == storageClass {
			storageClassOk = true
			break
		}
	}
	if !storageClassOk {
		return fmt.Errorf("'%s' is bad S3_STORAGE_CLASS, select one of: %s",
			cfg.S3.StorageClass, strings.Join(s3.StorageClass_Values(), ", "))
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
	return nil
}

// PrintConfig - print default / current config to stdout
func PrintConfig(ctx *cli.Context) error {
	var cfg *Config
	if ctx == nil {
		cfg = DefaultConfig()
	} else {
		cfg = GetConfig(ctx)
	}
	yml, _ := yaml.Marshal(&cfg)
	fmt.Print(string(yml))
	return nil
}

func DefaultConfig() *Config {
	availableConcurrency := uint8(1)
	if runtime.NumCPU() > 1 {
		availableConcurrency = uint8(math.Min(float64(runtime.NumCPU()/2), 128))
	}
	return &Config{
		General: GeneralConfig{
			RemoteStorage:          "none",
			MaxFileSize:            0,
			BackupsToKeepLocal:     0,
			BackupsToKeepRemote:    0,
			LogLevel:               "info",
			DisableProgressBar:     true,
			UploadConcurrency:      availableConcurrency,
			DownloadConcurrency:    availableConcurrency,
			RestoreSchemaOnCluster: "",
			UploadByPart:           true,
			DownloadByPart:         true,
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
		},
		AzureBlob: AzureBlobConfig{
			EndpointSuffix:    "core.windows.net",
			CompressionLevel:  1,
			CompressionFormat: "tar",
			BufferSize:        0,
			MaxBuffers:        3,
			MaxPartsCount:     10000,
		},
		S3: S3Config{
			Region:                  "us-east-1",
			DisableSSL:              false,
			ACL:                     "private",
			AssumeRoleARN:           "",
			CompressionLevel:        1,
			CompressionFormat:       "tar",
			DisableCertVerification: false,
			StorageClass:            s3.StorageClassStandard,
			Concurrency:             1,
			PartSize:                0,
			MaxPartsCount:           10000,
		},
		GCS: GCSConfig{
			CompressionLevel:  1,
			CompressionFormat: "tar",
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

func GetConfig(ctx *cli.Context) *Config {
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

// SaveConfig - save config to file and reload config to environment variables
func SaveConfig(cfg *Config, configLocation string) error {

	configYaml, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(configLocation, configYaml, 0644)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("can't write config file: %v", err)
	}
	if err = envconfig.Process("", cfg); err != nil {
		return err
	}
	cfg.AzureBlob.Path = strings.TrimPrefix(cfg.AzureBlob.Path, "/")
	cfg.S3.Path = strings.TrimPrefix(cfg.S3.Path, "/")
	cfg.GCS.Path = strings.TrimPrefix(cfg.GCS.Path, "/")
	log.SetLevelFromString(cfg.General.LogLevel)
	return nil
}