package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/enhanced"
	"github.com/stretchr/testify/assert"
)

// TestBatchDeletionConfig tests batch deletion configuration
func TestBatchDeletionConfig(t *testing.T) {
	t.Run("Valid Configuration", func(t *testing.T) {
		testValidBatchDeletionConfig(t)
	})

	t.Run("Invalid Configuration", func(t *testing.T) {
		testInvalidBatchDeletionConfig(t)
	})

	t.Run("Configuration Defaults", func(t *testing.T) {
		testBatchDeletionConfigDefaults(t)
	})

	t.Run("Configuration Integration", func(t *testing.T) {
		testBatchDeletionConfigIntegration(t)
	})
}

// testValidBatchDeletionConfig tests valid configuration scenarios
func testValidBatchDeletionConfig(t *testing.T) {
	validConfigs := []struct {
		name   string
		config config.BatchDeletionConfig
	}{
		{
			name: "Minimal valid configuration",
			config: config.BatchDeletionConfig{
				Enabled:   true,
				Workers:   1,
				BatchSize: 1,
			},
		},
		{
			name: "Full valid configuration",
			config: config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        1000,
				Workers:          50,
				RetryAttempts:    3,
				ErrorStrategy:    "continue",
				FailureThreshold: 0.1,
			},
		},
		{
			name: "Conservative configuration",
			config: config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        100,
				Workers:          10,
				RetryAttempts:    5,
				ErrorStrategy:    "fail_fast",
				FailureThreshold: 0.05,
			},
		},
		{
			name: "Aggressive configuration",
			config: config.BatchDeletionConfig{
				Enabled:          true,
				BatchSize:        5000,
				Workers:          200,
				RetryAttempts:    1,
				ErrorStrategy:    "continue",
				FailureThreshold: 0.25,
			},
		},
	}

	for _, tc := range validConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBatchDeletionConfig(&tc.config)
			assert.NoError(t, err, "Valid configuration should not produce errors")

			// Test configuration normalization
			normalizedConfig := normalizeBatchDeletionConfig(&tc.config)
			assert.NotNil(t, normalizedConfig)

			// Verify enabled state is preserved
			assert.Equal(t, tc.config.Enabled, normalizedConfig.Enabled)

			// Verify values are within reasonable ranges
			if normalizedConfig.BatchSize > 0 {
				assert.GreaterOrEqual(t, normalizedConfig.BatchSize, 1)
				assert.LessOrEqual(t, normalizedConfig.BatchSize, 10000)
			}

			if normalizedConfig.Workers > 0 {
				assert.GreaterOrEqual(t, normalizedConfig.Workers, 1)
				assert.LessOrEqual(t, normalizedConfig.Workers, 500)
			}
		})
	}
}

// testInvalidBatchDeletionConfig tests invalid configuration scenarios
func testInvalidBatchDeletionConfig(t *testing.T) {
	invalidConfigs := []struct {
		name        string
		config      config.BatchDeletionConfig
		expectedErr string
	}{
		{
			name: "Negative batch size",
			config: config.BatchDeletionConfig{
				Enabled:   true,
				BatchSize: -100,
			},
			expectedErr: "batch size cannot be negative",
		},
		{
			name: "Negative workers",
			config: config.BatchDeletionConfig{
				Enabled: true,
				Workers: -5,
			},
			expectedErr: "worker count cannot be negative",
		},
		{
			name: "Invalid failure threshold - negative",
			config: config.BatchDeletionConfig{
				Enabled:          true,
				Workers:          1,
				BatchSize:        1,
				FailureThreshold: -0.1,
			},
			expectedErr: "failure threshold cannot be negative",
		},
		{
			name: "Invalid failure threshold - too high",
			config: config.BatchDeletionConfig{
				Enabled:          true,
				Workers:          1,
				BatchSize:        1,
				FailureThreshold: 1.5,
			},
			expectedErr: "failure threshold cannot exceed 1.0",
		},
		{
			name: "Invalid retry attempts",
			config: config.BatchDeletionConfig{
				Enabled:       true,
				Workers:       1,
				BatchSize:     1,
				RetryAttempts: -1,
			},
			expectedErr: "retry attempts cannot be negative",
		},
		{
			name: "Invalid error strategy",
			config: config.BatchDeletionConfig{
				Enabled:       true,
				Workers:       1,
				BatchSize:     1,
				ErrorStrategy: "invalid_strategy",
			},
			expectedErr: "invalid error strategy",
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBatchDeletionConfig(&tc.config)
			assert.Error(t, err, "Invalid configuration should produce an error")
			if tc.expectedErr != "" {
				assert.Contains(t, err.Error(), tc.expectedErr, "Error should contain expected message")
			}
		})
	}
}

// testBatchDeletionConfigDefaults tests configuration defaults
func testBatchDeletionConfigDefaults(t *testing.T) {
	// Test default configuration from config.DefaultConfig()
	defaultConfig := config.DefaultConfig()
	assert.True(t, defaultConfig.General.BatchDeletion.Enabled, "Batch deletion should be enabled by default")
	assert.Equal(t, 1000, defaultConfig.General.BatchDeletion.BatchSize, "Default batch size should be 1000")
	assert.Equal(t, 0, defaultConfig.General.BatchDeletion.Workers, "Default workers should be 0 (auto-detect)")
	assert.Equal(t, 3, defaultConfig.General.BatchDeletion.RetryAttempts, "Default retry attempts should be 3")
	assert.Equal(t, "continue", defaultConfig.General.BatchDeletion.ErrorStrategy, "Default error strategy should be 'continue'")
	assert.Equal(t, 0.1, defaultConfig.General.BatchDeletion.FailureThreshold, "Default failure threshold should be 0.1")

	// Test storage-specific defaults
	assert.True(t, defaultConfig.S3.BatchDeletion.UseBatchAPI, "S3 should use batch API by default")
	assert.Equal(t, 10, defaultConfig.S3.BatchDeletion.VersionConcurrency, "S3 version concurrency should be 10")
	assert.True(t, defaultConfig.S3.BatchDeletion.PreloadVersions, "S3 should preload versions by default")

	assert.Equal(t, 50, defaultConfig.GCS.BatchDeletion.MaxWorkers, "GCS max workers should be 50")
	assert.True(t, defaultConfig.GCS.BatchDeletion.UseClientPool, "GCS should use client pool by default")
	assert.True(t, defaultConfig.GCS.BatchDeletion.UseBatchAPI, "GCS should use batch API by default")

	assert.True(t, defaultConfig.AzureBlob.BatchDeletion.UseBatchAPI, "Azure should use batch API by default")
	assert.Equal(t, 20, defaultConfig.AzureBlob.BatchDeletion.MaxWorkers, "Azure max workers should be 20")
}

// testBatchDeletionConfigIntegration tests configuration integration with storage configs
func testBatchDeletionConfigIntegration(t *testing.T) {
	integrationTests := []struct {
		name        string
		config      *config.Config
		storageType string
		shouldWork  bool
		description string
	}{
		{
			name: "Complete S3 configuration",
			config: &config.Config{
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled:   true,
						BatchSize: 1000,
						Workers:   50,
					},
				},
				S3: config.S3Config{
					Bucket: "integration-test-s3",
					Region: "us-east-1",
					BatchDeletion: config.S3BatchConfig{
						UseBatchAPI:        true,
						VersionConcurrency: 10,
						PreloadVersions:    true,
					},
				},
			},
			storageType: "s3",
			shouldWork:  true,
			description: "Complete S3 configuration should work with batch deletion",
		},
		{
			name: "Complete GCS configuration",
			config: &config.Config{
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled: true,
						Workers: 25,
					},
				},
				GCS: config.GCSConfig{
					Bucket:         "integration-test-gcs",
					ClientPoolSize: 30,
					BatchDeletion: config.GCSBatchConfig{
						MaxWorkers:    50,
						UseClientPool: true,
						UseBatchAPI:   true,
					},
				},
			},
			storageType: "gcs",
			shouldWork:  true,
			description: "Complete GCS configuration should work with batch deletion",
		},
		{
			name: "Complete Azure configuration",
			config: &config.Config{
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled: true,
						Workers: 20,
					},
				},
				AzureBlob: config.AzureBlobConfig{
					Container: "integration-test-azure",
					BatchDeletion: config.AzureBatchConfig{
						UseBatchAPI: true,
						MaxWorkers:  20,
					},
				},
			},
			storageType: "azure",
			shouldWork:  true,
			description: "Complete Azure configuration should work with batch deletion",
		},
		{
			name: "Disabled batch deletion",
			config: &config.Config{
				General: config.GeneralConfig{
					BatchDeletion: config.BatchDeletionConfig{
						Enabled: false,
					},
				},
				S3: config.S3Config{
					Bucket: "integration-test-s3",
					Region: "us-east-1",
				},
			},
			storageType: "s3",
			shouldWork:  true,
			description: "Disabled batch deletion should still work (fallback to standard delete)",
		},
	}

	for _, tc := range integrationTests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Validate general batch deletion configuration
			err := validateBatchDeletionConfig(&tc.config.General.BatchDeletion)

			if tc.shouldWork {
				assert.NoError(t, err, tc.description)

				// Test that configuration can be normalized
				normalizedConfig := normalizeBatchDeletionConfig(&tc.config.General.BatchDeletion)
				assert.NotNil(t, normalizedConfig)

				// Test storage-specific validation
				switch tc.storageType {
				case "s3":
					err = validateS3BatchConfig(&tc.config.S3.BatchDeletion)
					assert.NoError(t, err, "S3 batch config should be valid")
				case "gcs":
					err = validateGCSBatchConfig(&tc.config.GCS.BatchDeletion)
					assert.NoError(t, err, "GCS batch config should be valid")
				case "azure":
					err = validateAzureBatchConfig(&tc.config.AzureBlob.BatchDeletion)
					assert.NoError(t, err, "Azure batch config should be valid")
				}
			} else {
				assert.Error(t, err, tc.description)
			}

			_ = ctx // Avoid unused variable warning
		})
	}
}

// TestStorageSpecificBatchConfig tests storage-specific batch configuration
func TestStorageSpecificBatchConfig(t *testing.T) {
	t.Run("S3 Batch Configuration", func(t *testing.T) {
		testS3BatchConfig(t)
	})

	t.Run("GCS Batch Configuration", func(t *testing.T) {
		testGCSBatchConfig(t)
	})

	t.Run("Azure Batch Configuration", func(t *testing.T) {
		testAzureBatchConfig(t)
	})
}

// testS3BatchConfig tests S3-specific batch configuration
func testS3BatchConfig(t *testing.T) {
	validConfigs := []config.S3BatchConfig{
		{
			UseBatchAPI:        true,
			VersionConcurrency: 10,
			PreloadVersions:    true,
		},
		{
			UseBatchAPI:        false,
			VersionConcurrency: 20,
			PreloadVersions:    false,
		},
	}

	for i, cfg := range validConfigs {
		t.Run(fmt.Sprintf("Valid S3 config %d", i+1), func(t *testing.T) {
			err := validateS3BatchConfig(&cfg)
			assert.NoError(t, err, "Valid S3 batch config should not produce errors")
		})
	}

	invalidConfigs := []struct {
		name        string
		config      config.S3BatchConfig
		expectedErr string
	}{
		{
			name: "Negative version concurrency",
			config: config.S3BatchConfig{
				VersionConcurrency: -1,
			},
			expectedErr: "version concurrency cannot be negative",
		},
		{
			name: "Excessive version concurrency",
			config: config.S3BatchConfig{
				VersionConcurrency: 200,
			},
			expectedErr: "version concurrency should not exceed 100",
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateS3BatchConfig(&tc.config)
			assert.Error(t, err, "Invalid S3 batch config should produce an error")
			if tc.expectedErr != "" {
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

// testGCSBatchConfig tests GCS-specific batch configuration
func testGCSBatchConfig(t *testing.T) {
	validConfigs := []config.GCSBatchConfig{
		{
			MaxWorkers:    50,
			UseClientPool: true,
			UseBatchAPI:   true,
		},
		{
			MaxWorkers:    25,
			UseClientPool: false,
			UseBatchAPI:   false,
		},
	}

	for i, cfg := range validConfigs {
		t.Run(fmt.Sprintf("Valid GCS config %d", i+1), func(t *testing.T) {
			err := validateGCSBatchConfig(&cfg)
			assert.NoError(t, err, "Valid GCS batch config should not produce errors")
		})
	}

	invalidConfigs := []struct {
		name        string
		config      config.GCSBatchConfig
		expectedErr string
	}{
		{
			name: "Negative max workers",
			config: config.GCSBatchConfig{
				MaxWorkers: -1,
			},
			expectedErr: "max workers cannot be negative",
		},
		{
			name: "Excessive max workers",
			config: config.GCSBatchConfig{
				MaxWorkers: 200,
			},
			expectedErr: "max workers should not exceed 100",
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateGCSBatchConfig(&tc.config)
			assert.Error(t, err, "Invalid GCS batch config should produce an error")
			if tc.expectedErr != "" {
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

// testAzureBatchConfig tests Azure-specific batch configuration
func testAzureBatchConfig(t *testing.T) {
	validConfigs := []config.AzureBatchConfig{
		{
			UseBatchAPI: true,
			MaxWorkers:  20,
		},
		{
			UseBatchAPI: false,
			MaxWorkers:  15,
		},
	}

	for i, cfg := range validConfigs {
		t.Run(fmt.Sprintf("Valid Azure config %d", i+1), func(t *testing.T) {
			err := validateAzureBatchConfig(&cfg)
			assert.NoError(t, err, "Valid Azure batch config should not produce errors")
		})
	}

	invalidConfigs := []struct {
		name        string
		config      config.AzureBatchConfig
		expectedErr string
	}{
		{
			name: "Negative max workers",
			config: config.AzureBatchConfig{
				MaxWorkers: -1,
			},
			expectedErr: "max workers cannot be negative",
		},
		{
			name: "Excessive max workers",
			config: config.AzureBatchConfig{
				MaxWorkers: 100,
			},
			expectedErr: "max workers should not exceed 50",
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAzureBatchConfig(&tc.config)
			assert.Error(t, err, "Invalid Azure batch config should produce an error")
			if tc.expectedErr != "" {
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

// Helper functions for configuration validation and normalization

func validateBatchDeletionConfig(config *config.BatchDeletionConfig) error {
	if config.BatchSize < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "BatchSize",
			Value:   config.BatchSize,
			Message: "batch size cannot be negative",
		}
	}

	if config.Workers < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "Workers",
			Value:   config.Workers,
			Message: "worker count cannot be negative",
		}
	}

	if config.FailureThreshold < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "FailureThreshold",
			Value:   config.FailureThreshold,
			Message: "failure threshold cannot be negative",
		}
	}

	if config.FailureThreshold > 1.0 {
		return &enhanced.OptimizationConfigError{
			Field:   "FailureThreshold",
			Value:   config.FailureThreshold,
			Message: "failure threshold cannot exceed 1.0",
		}
	}

	if config.RetryAttempts < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "RetryAttempts",
			Value:   config.RetryAttempts,
			Message: "retry attempts cannot be negative",
		}
	}

	// Validate error strategy
	validStrategies := []string{"fail_fast", "continue", "retry_batch"}
	validStrategy := false
	for _, strategy := range validStrategies {
		if config.ErrorStrategy == strategy {
			validStrategy = true
			break
		}
	}
	if !validStrategy && config.ErrorStrategy != "" {
		return &enhanced.OptimizationConfigError{
			Field:   "ErrorStrategy",
			Value:   config.ErrorStrategy,
			Message: "invalid error strategy, must be one of: fail_fast, continue, retry_batch",
		}
	}

	return nil
}

func normalizeBatchDeletionConfig(config *config.BatchDeletionConfig) *config.BatchDeletionConfig {
	normalized := *config

	// Apply defaults for zero values
	if normalized.BatchSize == 0 {
		normalized.BatchSize = 1000
	}

	if normalized.Workers == 0 {
		normalized.Workers = 50
	}

	if normalized.RetryAttempts == 0 {
		normalized.RetryAttempts = 3
	}

	if normalized.ErrorStrategy == "" {
		normalized.ErrorStrategy = "continue"
	}

	if normalized.FailureThreshold == 0 {
		normalized.FailureThreshold = 0.1
	}

	return &normalized
}

func validateS3BatchConfig(config *config.S3BatchConfig) error {
	if config.VersionConcurrency < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "VersionConcurrency",
			Value:   config.VersionConcurrency,
			Message: "version concurrency cannot be negative",
		}
	}

	if config.VersionConcurrency > 100 {
		return &enhanced.OptimizationConfigError{
			Field:   "VersionConcurrency",
			Value:   config.VersionConcurrency,
			Message: "version concurrency should not exceed 100 to avoid overwhelming S3",
		}
	}

	return nil
}

func validateGCSBatchConfig(config *config.GCSBatchConfig) error {
	if config.MaxWorkers < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "MaxWorkers",
			Value:   config.MaxWorkers,
			Message: "max workers cannot be negative",
		}
	}

	if config.MaxWorkers > 100 {
		return &enhanced.OptimizationConfigError{
			Field:   "MaxWorkers",
			Value:   config.MaxWorkers,
			Message: "max workers should not exceed 100 to avoid rate limiting",
		}
	}

	return nil
}

func validateAzureBatchConfig(config *config.AzureBatchConfig) error {
	if config.MaxWorkers < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "MaxWorkers",
			Value:   config.MaxWorkers,
			Message: "max workers cannot be negative",
		}
	}

	if config.MaxWorkers > 50 {
		return &enhanced.OptimizationConfigError{
			Field:   "MaxWorkers",
			Value:   config.MaxWorkers,
			Message: "max workers should not exceed 50 for Azure Blob to avoid throttling",
		}
	}

	return nil
}
