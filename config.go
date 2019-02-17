package main

import (
	"fmt"
	"io/ioutil"
	"os"

	yaml "gopkg.in/yaml.v2"
)

// Config - config file format
type Config struct {
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	S3         S3Config         `yaml:"s3"`
	Backup     BackupConfig     `yaml:"backup"`
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
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     uint   `yaml:"port"`
	DataPath string `yaml:"data_path"`
}

// BackupConfig - backup specific settings
type BackupConfig struct {
	Strategy      string `yaml:"strategy"`
	BackupsToKeep int    `yaml:"backups_to_keep"`
}

// LoadConfig - load config from file
func LoadConfig(configLocation string) (*Config, error) {
	config := defaultConfig()
	configYaml, err := ioutil.ReadFile(configLocation)
	if os.IsNotExist(err) {
		return config, nil
	}
	if err != nil {
		return nil, fmt.Errorf("can't read with: %v", err)
	}
	err = yaml.Unmarshal(configYaml, &config)
	if err != nil {
		return nil, fmt.Errorf("can't parse with: %v", err)
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
	return nil
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
		},
		S3: S3Config{
			Region:            "us-east-1",
			DisableSSL:        false,
			ACL:               "private",
			OverwriteStrategy: "always",
		},
		Backup: BackupConfig{
			Strategy:      "tree",
			BackupsToKeep: 10,
		},
	}
}
