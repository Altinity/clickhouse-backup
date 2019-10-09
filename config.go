package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/kelseyhightower/envconfig"
	yaml "gopkg.in/yaml.v2"
)

// Config - config file format
type Config struct {
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	S3         S3Config         `yaml:"s3"`
}

// S3Config - s3 settings section
type S3Config struct {
	AccessKey          string `yaml:"access_key" envconfig:"S3_ACCESS_KEY"`
	SecretKey          string `yaml:"secret_key" envconfig:"S3_SECRET_KEY"`
	Bucket             string `yaml:"bucket" envconfig:"S3_BUCKET"`
	Endpoint           string `yaml:"endpoint" envconfig:"S3_ENDPOINT"`
	Region             string `yaml:"region" envconfig:"S3_REGION"`
	ACL                string `yaml:"acl" envconfig:"S3_ACL"`
	ForcePathStyle     bool   `yaml:"force_path_style" envconfig:"S3_FORCE_PATH_STYLE"`
	Path               string `yaml:"path" envconfig:"S3_PATH"`
	DisableSSL         bool   `yaml:"disable_ssl" envconfig:"S3_DISABLE_SSL"`
	DisableProgressBar bool   `yaml:"disable_progress_bar" envconfig:"DISABLE_PROGRESS_BAR"`
	PartSize           int64  `yaml:"part_size" envconfig:"S3_PART_SIZE"`
	Strategy           string `yaml:"strategy"`
	BackupsToKeepLocal int    `yaml:"backups_to_keep_local" envconfig:"BACKUPS_TO_KEEP_LOCAL"`
	BackupsToKeepS3    int    `yaml:"backups_to_keep_s3" envconfig:"BACKUPS_TO_KEEP_S3"`
	CompressionLevel   int    `yaml:"compression_level" envconfig:"S3_COMPRESSION_LEVEL"`
	CompressionFormat  string `yaml:"compression_format" envconfig:"S3_COMPRESSION_FORMAT"`
	SSE                string `yaml:"sse" envconfig:"S3_SSE"`
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username   string   `yaml:"username" envconfig:"CLICKHOUSE_USERNAME"`
	Password   string   `yaml:"password" envconfig:"CLICKHOUSE_PASSWORD"`
	Host       string   `yaml:"host" envconfig:"CLICKHOUSE_HOST"`
	Port       uint     `yaml:"port" envconfig:"CLICKHOUSE_PORT"`
	DataPath   string   `yaml:"data_path" envconfig:"CLICKHOUSE_DATA_PATH"`
	SkipTables []string `yaml:"skip_tables" envconfig:"CLICKHOUSE_SKIP_TABLES"`
}

// LoadConfig - load config from file
func LoadConfig(configLocation string) (*Config, error) {
	config := defaultConfig()
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
	if config.S3.Strategy == "tree" {
		return fmt.Errorf("The 'tree' strategy support has been removed in v0.4.0")
	}
	_, err := getArchiveWriter(config.S3.CompressionFormat, config.S3.CompressionLevel)
	return err
}

// PrintDefaultConfig - print default config to stdout
func PrintDefaultConfig() {
	c := defaultConfig()
	d, _ := yaml.Marshal(&c)
	fmt.Print(string(d))
}

func defaultConfig() *Config {
	return &Config{
		ClickHouse: ClickHouseConfig{
			Username: "default",
			Password: "",
			Host:     "localhost",
			Port:     9000,
			SkipTables: []string{
				"system.*",
			},
		},
		S3: S3Config{
			Region:             "us-east-1",
			DisableSSL:         false,
			ACL:                "private",
			PartSize:           100 * 1024 * 1024,
			BackupsToKeepLocal: 0,
			BackupsToKeepS3:    0,
			CompressionLevel:   1,
			CompressionFormat:  "gzip",
			SSE:                "",
		},
	}
}
