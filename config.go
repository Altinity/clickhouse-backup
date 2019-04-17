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
	AccessKey          string `yaml:"access_key"`
	SecretKey          string `yaml:"secret_key"`
	Bucket             string `yaml:"bucket"`
	Endpoint           string `yaml:"endpoint"`
	Region             string `yaml:"region"`
	ACL                string `yaml:"acl"`
	ForcePathStyle     bool   `yaml:"force_path_style"`
	Path               string `yaml:"path"`
	DisableSSL         bool   `yaml:"disable_ssl"`
	DisableProgressBar bool   `yaml:"disable_progress_bar"`
	OverwriteStrategy  string `yaml:"overwrite_strategy"`
	PartSize           int64  `yaml:"part_size"`
	DeleteExtraFiles   bool   `yaml:"delete_extra_files"`
	Strategy           string `yaml:"strategy"`
	BackupsToKeepLocal int    `yaml:"backups_to_keep_local"`
	BackupsToKeepS3    int    `yaml:"backups_to_keep_s3"`
	CompressionLevel   int    `yaml:"compression_level"`
	CompressionFormat  string `yaml:"compression_format"`
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username   string   `yaml:"username"`
	Password   string   `yaml:"password"`
	Host       string   `yaml:"host"`
	Port       uint     `yaml:"port"`
	DataPath   string   `yaml:"data_path"`
	SkipTables []string `yaml:"skip_tables"`
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
	switch config.S3.OverwriteStrategy {
	case
		"skip",
		"etag",
		"always":
		break
	default:
		return fmt.Errorf("unknown s3.overwrite_strategy it can be 'skip', 'etag', 'always'")
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
			OverwriteStrategy:  "always",
			PartSize:           5 * 1024 * 1024,
			DeleteExtraFiles:   true,
			Strategy:           "archive",
			BackupsToKeepLocal: 0,
			BackupsToKeepS3:    0,
			CompressionLevel:   1,
			CompressionFormat:  "lz4",
		},
	}
}
