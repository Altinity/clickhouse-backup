package chbackup

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	yaml "gopkg.in/yaml.v2"
)

// Config - config file format
type Config struct {
	General    GeneralConfig    `yaml:"general"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	S3         S3Config         `yaml:"s3"`
	GCS        GCSConfig        `yaml:"gcs"`
	COS        COSConfig        `yaml:"cos"`
	FTP        FTPConfig        `yaml:"ftp"`
}

// GeneralConfig - general setting section
type GeneralConfig struct {
	RemoteStorage       string `yaml:"remote_storage" envconfig:"REMOTE_STORAGE"`
	DisableProgressBar  bool   `yaml:"disable_progress_bar" envconfig:"DISABLE_PROGRESS_BAR"`
	BackupsToKeepLocal  int    `yaml:"backups_to_keep_local" envconfig:"BACKUPS_TO_KEEP_LOCAL"`
	BackupsToKeepRemote int    `yaml:"backups_to_keep_remote" envconfig:"BACKUPS_TO_KEEP_REMOTE"`
}

// GCSConfig - GCS settings section
type GCSConfig struct {
	CredentialsFile   string `yaml:"credentials_file" envconfig:"GCS_CREDENTIALS_FILE"`
	CredentialsJSON   string `yaml:"credentials_json" envconfig:"GCS_CREDENTIALS_JSON"`
	Bucket            string `yaml:"bucket" envconfig:"GCS_BUCKET"`
	Path              string `yaml:"path" envconfig:"GCS_PATH"`
	CompressionLevel  int    `yaml:"compression_level" envconfig:"GCS_COMPRESSION_LEVEL"`
	CompressionFormat string `yaml:"compression_format" envconfig:"GCS_COMPRESSION_FORMAT"`
}

// S3Config - s3 settings section
type S3Config struct {
	AccessKey               string `yaml:"access_key" envconfig:"S3_ACCESS_KEY"`
	SecretKey               string `yaml:"secret_key" envconfig:"S3_SECRET_KEY"`
	Bucket                  string `yaml:"bucket" envconfig:"S3_BUCKET"`
	Endpoint                string `yaml:"endpoint" envconfig:"S3_ENDPOINT"`
	Region                  string `yaml:"region" envconfig:"S3_REGION"`
	ACL                     string `yaml:"acl" envconfig:"S3_ACL"`
	ForcePathStyle          bool   `yaml:"force_path_style" envconfig:"S3_FORCE_PATH_STYLE"`
	Path                    string `yaml:"path" envconfig:"S3_PATH"`
	DisableSSL              bool   `yaml:"disable_ssl" envconfig:"S3_DISABLE_SSL"`
	PartSize                int64  `yaml:"part_size" envconfig:"S3_PART_SIZE"`
	CompressionLevel        int    `yaml:"compression_level" envconfig:"S3_COMPRESSION_LEVEL"`
	CompressionFormat       string `yaml:"compression_format" envconfig:"S3_COMPRESSION_FORMAT"`
	SSE                     string `yaml:"sse" envconfig:"S3_SSE"`
	DisableCertVerification bool   `yaml:"disable_cert_verification" envconfig:"S3_DISABLE_CERT_VERIFICATION"`
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
	Debug             bool   `yaml:"debug" envconfig:"FTP_DEBUG"`
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username     string   `yaml:"username" envconfig:"CLICKHOUSE_USERNAME"`
	Password     string   `yaml:"password" envconfig:"CLICKHOUSE_PASSWORD"`
	Host         string   `yaml:"host" envconfig:"CLICKHOUSE_HOST"`
	Port         uint     `yaml:"port" envconfig:"CLICKHOUSE_PORT"`
	DataPath     string   `yaml:"data_path" envconfig:"CLICKHOUSE_DATA_PATH"`
	SkipTables   []string `yaml:"skip_tables" envconfig:"CLICKHOUSE_SKIP_TABLES"`
	Timeout      string   `yaml:"timeout" envconfig:"CLICKHOUSE_TIMEOUT"`
	FreezeByPart bool     `yaml:"freeze_by_part" envconfig:"CLICKHOUSE_FREEZE_BY_PART"`
}

// LoadConfig - load config from file
func LoadConfig(configLocation string) (*Config, error) {
	config := DefaultConfig()
	configYaml, err := ioutil.ReadFile(configLocation)
	if os.IsNotExist(err) {
		err := envconfig.Process("", config)
		return config, err
	}
	if err != nil {
		return nil, fmt.Errorf("can't open with %v", err)
	}
	if err := yaml.Unmarshal(configYaml, &config); err != nil {
		return nil, fmt.Errorf("can't parse with %v", err)
	}
	if err := envconfig.Process("", config); err != nil {
		return nil, err
	}
	return config, validateConfig(config)
}

func validateConfig(config *Config) error {
	if _, err := getArchiveWriter(config.S3.CompressionFormat, config.S3.CompressionLevel); err != nil {
		return err
	}
	if _, err := getArchiveWriter(config.GCS.CompressionFormat, config.GCS.CompressionLevel); err != nil {
		return err
	}
	if _, err := time.ParseDuration(config.ClickHouse.Timeout); err != nil {
		return err
	}
	if _, err := time.ParseDuration(config.COS.Timeout); err != nil {
		return err
	}
	if _, err := time.ParseDuration(config.FTP.Timeout); err != nil {
		return err
	}
	return nil
}

// PrintDefaultConfig - print default config to stdout
func PrintDefaultConfig() {
	c := DefaultConfig()
	d, _ := yaml.Marshal(&c)
	fmt.Print(string(d))
}

func DefaultConfig() *Config {
	return &Config{
		General: GeneralConfig{
			RemoteStorage:       "s3",
			BackupsToKeepLocal:  0,
			BackupsToKeepRemote: 0,
		},
		ClickHouse: ClickHouseConfig{
			Username: "default",
			Password: "",
			Host:     "localhost",
			Port:     9000,
			SkipTables: []string{
				"system.*",
			},
			Timeout: "5m",
		},
		S3: S3Config{
			Region:                  "us-east-1",
			DisableSSL:              false,
			ACL:                     "private",
			PartSize:                100 * 1024 * 1024,
			CompressionLevel:        1,
			CompressionFormat:       "gzip",
			DisableCertVerification: false,
		},
		GCS: GCSConfig{
			CompressionLevel:  1,
			CompressionFormat: "gzip",
		},
		COS: COSConfig{
			RowURL:            "",
			Timeout:           "2m",
			SecretID:          "",
			SecretKey:         "",
			Path:              "",
			CompressionFormat: "gzip",
			CompressionLevel:  1,
			Debug:             false,
		},
		FTP: FTPConfig{
			Address:           "",
			Timeout:           "2m",
			Username:          "",
			Password:          "",
			TLS:               false,
			CompressionFormat: "gzip",
			CompressionLevel:  1,
			Debug:             false,
		},
	}
}
