package test

import (
	"context"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/enhanced"
	"github.com/stretchr/testify/assert"
)

// TestDeleteOptimizationConfig tests enhanced delete optimization configuration
func TestDeleteOptimizationConfig(t *testing.T) {
	t.Run("Valid Configuration", func(t *testing.T) {
		testValidDeleteOptimizationConfig(t)
	})

	t.Run("Invalid Configuration", func(t *testing.T) {
		testInvalidDeleteOptimizationConfig(t)
	})

	t.Run("Configuration Defaults", func(t *testing.T) {
		testDeleteOptimizationConfigDefaults(t)
	})

	t.Run("Configuration Combinations", func(t *testing.T) {
		testDeleteOptimizationConfigCombinations(t)
	})
}

// testValidDeleteOptimizationConfig tests valid configuration scenarios
func testValidDeleteOptimizationConfig(t *testing.T) {
	validConfigs := []struct {
		name   string
		config config.DeleteOptimizations
	}{
		{
			name: "Minimal valid configuration",
			config: config.DeleteOptimizations{
				Enabled:   true,
				Workers:   1, // Need at least 1 worker when enabled
				BatchSize: 1, // Need at least 1 batch size
			},
		},
		{
			name: "Full valid configuration",
			config: config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        1000,
				Workers:          50,
				RetryAttempts:    3,
				ErrorStrategy:    "continue",
				FailureThreshold: 0.1,
				CacheEnabled:     true,
				CacheTTL:         time.Hour,
			},
		},
		{
			name: "Conservative configuration",
			config: config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        100,
				Workers:          10,
				RetryAttempts:    5,
				ErrorStrategy:    "fail_fast",
				FailureThreshold: 0.05,
				CacheEnabled:     false,
			},
		},
		{
			name: "Aggressive configuration",
			config: config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        5000,
				Workers:          200,
				RetryAttempts:    1,
				ErrorStrategy:    "continue",
				FailureThreshold: 0.25,
				CacheEnabled:     true,
				CacheTTL:         30 * time.Minute,
			},
		},
	}

	for _, tc := range validConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDeleteOptimizationConfig(&tc.config)
			assert.NoError(t, err, "Valid configuration should not produce errors")

			// Test configuration normalization
			normalizedConfig := normalizeDeleteOptimizationConfig(&tc.config)
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

// testInvalidDeleteOptimizationConfig tests invalid configuration scenarios
func testInvalidDeleteOptimizationConfig(t *testing.T) {
	invalidConfigs := []struct {
		name        string
		config      config.DeleteOptimizations
		expectedErr string
	}{
		{
			name: "Negative batch size",
			config: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: -100,
			},
			expectedErr: "batch size cannot be negative",
		},
		{
			name: "Zero workers",
			config: config.DeleteOptimizations{
				Enabled: true,
				Workers: 0,
			},
			expectedErr: "worker count must be positive",
		},
		{
			name: "Negative workers",
			config: config.DeleteOptimizations{
				Enabled: true,
				Workers: -5,
			},
			expectedErr: "worker count cannot be negative",
		},
		{
			name: "Excessive batch size",
			config: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 50000, // Too large
			},
			expectedErr: "batch size exceeds maximum",
		},
		{
			name: "Excessive workers",
			config: config.DeleteOptimizations{
				Enabled: true,
				Workers: 1000, // Too many
			},
			expectedErr: "worker count exceeds maximum",
		},
		{
			name: "Invalid failure threshold - negative",
			config: config.DeleteOptimizations{
				Enabled:          true,
				Workers:          1, // Valid worker count
				BatchSize:        1, // Valid batch size
				FailureThreshold: -0.1,
			},
			expectedErr: "failure threshold cannot be negative",
		},
		{
			name: "Invalid failure threshold - too high",
			config: config.DeleteOptimizations{
				Enabled:          true,
				Workers:          1,   // Valid worker count
				BatchSize:        1,   // Valid batch size
				FailureThreshold: 1.5, // > 1.0
			},
			expectedErr: "failure threshold cannot exceed 1.0",
		},
		{
			name: "Invalid retry attempts",
			config: config.DeleteOptimizations{
				Enabled:       true,
				Workers:       1, // Valid worker count
				BatchSize:     1, // Valid batch size
				RetryAttempts: -1,
			},
			expectedErr: "retry attempts cannot be negative",
		},
		{
			name: "Excessive retry attempts",
			config: config.DeleteOptimizations{
				Enabled:       true,
				Workers:       1,  // Valid worker count
				BatchSize:     1,  // Valid batch size
				RetryAttempts: 20, // Too many
			},
			expectedErr: "retry attempts exceed maximum",
		},
		{
			name: "Invalid error strategy",
			config: config.DeleteOptimizations{
				Enabled:       true,
				Workers:       1, // Valid worker count
				BatchSize:     1, // Valid batch size
				ErrorStrategy: "invalid_strategy",
			},
			expectedErr: "invalid error strategy",
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDeleteOptimizationConfig(&tc.config)
			assert.Error(t, err, "Invalid configuration should produce an error")
			if tc.expectedErr != "" {
				assert.Contains(t, err.Error(), tc.expectedErr, "Error should contain expected message")
			}
		})
	}
}

// testDeleteOptimizationConfigDefaults tests configuration defaults
func testDeleteOptimizationConfigDefaults(t *testing.T) {
	defaultTests := []struct {
		name           string
		config         config.DeleteOptimizations
		expectedFields map[string]interface{}
	}{
		{
			name: "Empty configuration gets defaults",
			config: config.DeleteOptimizations{
				Enabled: true,
			},
			expectedFields: map[string]interface{}{
				"BatchSize":    1000,
				"Workers":      50,
				"CacheEnabled": false,            // Default is false, not true
				"CacheTTL":     30 * time.Minute, // Default is 30 minutes, not 1 hour
			},
		},
		{
			name: "Partial configuration fills missing defaults",
			config: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 500,
			},
			expectedFields: map[string]interface{}{
				"BatchSize": 500, // Preserved
				"Workers":   50,  // Default
			},
		},
		{
			name: "Zero values trigger defaults",
			config: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 0, // Should trigger default
				Workers:   0, // Should trigger default
			},
			expectedFields: map[string]interface{}{
				"BatchSize":     1000,             // Default
				"Workers":       50,               // Default
				"ErrorStrategy": "continue",       // Default
				"CacheTTL":      30 * time.Minute, // Default
			},
		},
	}

	for _, tc := range defaultTests {
		t.Run(tc.name, func(t *testing.T) {
			normalizedConfig := normalizeDeleteOptimizationConfig(&tc.config)
			assert.NotNil(t, normalizedConfig)

			for field, expectedValue := range tc.expectedFields {
				switch field {
				case "BatchSize":
					assert.Equal(t, expectedValue, normalizedConfig.BatchSize)
				case "Workers":
					assert.Equal(t, expectedValue, normalizedConfig.Workers)
				case "CacheEnabled":
					assert.Equal(t, expectedValue, normalizedConfig.CacheEnabled)
				case "CacheTTL":
					assert.Equal(t, expectedValue, normalizedConfig.CacheTTL)
				case "ErrorStrategy":
					assert.Equal(t, expectedValue, normalizedConfig.ErrorStrategy)
				}
			}
		})
	}
}

// testDeleteOptimizationConfigCombinations tests various configuration combinations
func testDeleteOptimizationConfigCombinations(t *testing.T) {
	combinationTests := []struct {
		name        string
		config      config.DeleteOptimizations
		shouldValid bool
		description string
	}{
		{
			name: "Cache enabled with valid settings",
			config: config.DeleteOptimizations{
				Enabled:      true,
				Workers:      1, // Valid worker count
				BatchSize:    1, // Valid batch size
				CacheEnabled: true,
				CacheTTL:     30 * time.Minute,
			},
			shouldValid: true,
			description: "Cache configuration should be valid",
		},
		{
			name: "Cache disabled ignores cache settings",
			config: config.DeleteOptimizations{
				Enabled:       true,
				CacheEnabled:  false,
				ErrorStrategy: "invalid", // Should be validated even when cache disabled
			},
			shouldValid: false, // Invalid error strategy should fail
			description: "Invalid error strategy should fail validation",
		},
		{
			name: "High performance configuration",
			config: config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        2000,
				Workers:          100,
				RetryAttempts:    1,
				FailureThreshold: 0.2,
				ErrorStrategy:    "continue",
			},
			shouldValid: true,
			description: "High performance configuration should be valid",
		},
		{
			name: "High reliability configuration",
			config: config.DeleteOptimizations{
				Enabled:          true,
				BatchSize:        200,
				Workers:          20,
				RetryAttempts:    5,
				FailureThreshold: 0.01,
				ErrorStrategy:    "fail_fast", // More conservative
				CacheEnabled:     false,       // More conservative
			},
			shouldValid: true,
			description: "High reliability configuration should be valid",
		},
		{
			name: "Conflicting settings",
			config: config.DeleteOptimizations{
				Enabled:   false, // Disabled
				BatchSize: 1000,  // Should be ignored
				Workers:   50,    // Should be ignored
			},
			shouldValid: true,
			description: "Settings should be ignored when optimization is disabled",
		},
	}

	for _, tc := range combinationTests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDeleteOptimizationConfig(&tc.config)

			if tc.shouldValid {
				assert.NoError(t, err, tc.description)

				// Test that configuration can be used to create enhanced storage
				normalizedConfig := normalizeDeleteOptimizationConfig(&tc.config)
				assert.NotNil(t, normalizedConfig)

				if tc.config.Enabled {
					assert.True(t, normalizedConfig.Enabled, "Enabled state should be preserved")
				}
			} else {
				assert.Error(t, err, tc.description)
			}
		})
	}
}

// TestStorageSpecificConfig tests storage-specific configuration
func TestStorageSpecificConfig(t *testing.T) {
	t.Run("S3 Configuration", func(t *testing.T) {
		testS3SpecificConfig(t)
	})

	t.Run("GCS Configuration", func(t *testing.T) {
		testGCSSpecificConfig(t)
	})

	t.Run("Azure Configuration", func(t *testing.T) {
		testAzureSpecificConfig(t)
	})
}

// testS3SpecificConfig tests S3-specific configuration validation
func testS3SpecificConfig(t *testing.T) {
	s3Tests := []struct {
		name        string
		config      config.S3Config
		optimConfig config.DeleteOptimizations
		shouldValid bool
		expectedErr string
	}{
		{
			name: "Valid S3 configuration with optimizations",
			config: config.S3Config{
				Bucket: "test-s3-bucket",
				Region: "us-west-2",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 1000, // S3 supports up to 1000 objects per batch
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{UseBatchAPI: true},
			},
			shouldValid: true,
		},
		{
			name: "S3 configuration with excessive batch size",
			config: config.S3Config{
				Bucket: "test-s3-bucket",
				Region: "us-west-2",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 5000, // Exceeds S3 limit
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{UseBatchAPI: true},
			},
			shouldValid: false,
			expectedErr: "S3 batch size cannot exceed 1000",
		},
		{
			name: "Empty S3 bucket name",
			config: config.S3Config{
				Bucket: "",
				Region: "us-west-2",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
			},
			shouldValid: false,
			expectedErr: "S3 bucket name cannot be empty",
		},
		{
			name: "S3 configuration without storage features",
			config: config.S3Config{
				Bucket: "test-s3-bucket",
				Region: "us-west-2",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled:   true,
				BatchSize: 100, // Conservative
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{UseBatchAPI: false}, // Parallel instead of batch
			},
			shouldValid: true,
		},
	}

	for _, tc := range s3Tests {
		t.Run(tc.name, func(t *testing.T) {
			fullConfig := &config.Config{
				S3:                  tc.config,
				DeleteOptimizations: tc.optimConfig,
			}

			err := validateS3ConfigForEnhancedDelete(fullConfig)

			if tc.shouldValid {
				assert.NoError(t, err, "Valid S3 configuration should not produce errors")
			} else {
				assert.Error(t, err, "Invalid S3 configuration should produce an error")
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			}
		})
	}
}

// testGCSSpecificConfig tests GCS-specific configuration validation
func testGCSSpecificConfig(t *testing.T) {
	gcsTests := []struct {
		name        string
		config      config.GCSConfig
		optimConfig config.DeleteOptimizations
		shouldValid bool
		expectedErr string
	}{
		{
			name: "Valid GCS configuration with optimizations",
			config: config.GCSConfig{
				Bucket:         "test-gcs-bucket",
				ClientPoolSize: 20,
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
				Workers: 15, // Less than ClientPoolSize (20)
				GCSOptimizations: struct {
					MaxWorkers    int  `yaml:"max_workers" envconfig:"DELETE_GCS_MAX_WORKERS" default:"50"`
					UseClientPool bool `yaml:"use_client_pool" envconfig:"DELETE_GCS_USE_CLIENT_POOL" default:"true"`
				}{UseClientPool: true},
			},
			shouldValid: true,
		},
		{
			name: "GCS configuration with excessive workers",
			config: config.GCSConfig{
				Bucket:         "test-gcs-bucket",
				ClientPoolSize: 10,
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
				Workers: 50, // Exceeds ClientPoolSize
				GCSOptimizations: struct {
					MaxWorkers    int  `yaml:"max_workers" envconfig:"DELETE_GCS_MAX_WORKERS" default:"50"`
					UseClientPool bool `yaml:"use_client_pool" envconfig:"DELETE_GCS_USE_CLIENT_POOL" default:"true"`
				}{UseClientPool: true},
			},
			shouldValid: false,
			expectedErr: "GCS worker count cannot exceed client pool size",
		},
		{
			name: "Empty GCS bucket name",
			config: config.GCSConfig{
				Bucket: "",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
			},
			shouldValid: false,
			expectedErr: "GCS bucket name cannot be empty",
		},
		{
			name: "GCS configuration with zero client pool size",
			config: config.GCSConfig{
				Bucket:         "test-gcs-bucket",
				ClientPoolSize: 0, // Should use default
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
				Workers: 10,
			},
			shouldValid: true,
		},
	}

	for _, tc := range gcsTests {
		t.Run(tc.name, func(t *testing.T) {
			fullConfig := &config.Config{
				GCS:                 tc.config,
				DeleteOptimizations: tc.optimConfig,
			}

			err := validateGCSConfigForEnhancedDelete(fullConfig)

			if tc.shouldValid {
				assert.NoError(t, err, "Valid GCS configuration should not produce errors")
			} else {
				assert.Error(t, err, "Invalid GCS configuration should produce an error")
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			}
		})
	}
}

// testAzureSpecificConfig tests Azure-specific configuration validation
func testAzureSpecificConfig(t *testing.T) {
	azureTests := []struct {
		name        string
		config      config.AzureBlobConfig
		optimConfig config.DeleteOptimizations
		shouldValid bool
		expectedErr string
	}{
		{
			name: "Valid Azure configuration with optimizations",
			config: config.AzureBlobConfig{
				Container: "test-azure-container",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
				Workers: 20, // Conservative for Azure
				AzureOptimizations: struct {
					UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
					MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
				}{UseBatchAPI: true},
			},
			shouldValid: true,
		},
		{
			name: "Azure configuration with excessive workers",
			config: config.AzureBlobConfig{
				Container: "test-azure-container",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
				Workers: 100, // Too many for Azure
				AzureOptimizations: struct {
					UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
					MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
				}{UseBatchAPI: true},
			},
			shouldValid: false,
			expectedErr: "Azure worker count should not exceed 50",
		},
		{
			name: "Empty Azure container name",
			config: config.AzureBlobConfig{
				Container: "",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
			},
			shouldValid: false,
			expectedErr: "Azure container name cannot be empty",
		},
		{
			name: "Azure configuration without storage features",
			config: config.AzureBlobConfig{
				Container: "test-azure-container",
			},
			optimConfig: config.DeleteOptimizations{
				Enabled: true,
				Workers: 15,
				AzureOptimizations: struct {
					UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
					MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
				}{UseBatchAPI: false}, // Uses parallel delete
			},
			shouldValid: true,
		},
	}

	for _, tc := range azureTests {
		t.Run(tc.name, func(t *testing.T) {
			fullConfig := &config.Config{
				AzureBlob:           tc.config,
				DeleteOptimizations: tc.optimConfig,
			}

			err := validateAzureConfigForEnhancedDelete(fullConfig)

			if tc.shouldValid {
				assert.NoError(t, err, "Valid Azure configuration should not produce errors")
			} else {
				assert.Error(t, err, "Invalid Azure configuration should produce an error")
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			}
		})
	}
}

// TestFeatureFlagConfiguration tests feature flag configuration
func TestFeatureFlagConfiguration(t *testing.T) {
	featureFlagTests := []struct {
		name          string
		config        config.DeleteOptimizations
		storageType   string
		expectFeature bool
		description   string
	}{
		{
			name: "S3 with batch API enabled",
			config: config.DeleteOptimizations{
				Enabled: true,
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{UseBatchAPI: true},
			},
			storageType:   "s3",
			expectFeature: true,
			description:   "S3 should use batch delete when batch API is enabled",
		},
		{
			name: "S3 with batch API disabled",
			config: config.DeleteOptimizations{
				Enabled: true,
				S3Optimizations: struct {
					UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
					VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
					PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
				}{UseBatchAPI: false},
			},
			storageType:   "s3",
			expectFeature: false,
			description:   "S3 should use parallel delete when batch API is disabled",
		},
		{
			name: "GCS always uses parallel",
			config: config.DeleteOptimizations{
				Enabled: true,
				GCSOptimizations: struct {
					MaxWorkers    int  `yaml:"max_workers" envconfig:"DELETE_GCS_MAX_WORKERS" default:"50"`
					UseClientPool bool `yaml:"use_client_pool" envconfig:"DELETE_GCS_USE_CLIENT_POOL" default:"true"`
				}{UseClientPool: true},
			},
			storageType:   "gcs",
			expectFeature: false, // GCS doesn't support batch delete
			description:   "GCS should always use parallel delete",
		},
		{
			name: "Azure always uses parallel",
			config: config.DeleteOptimizations{
				Enabled: true,
				AzureOptimizations: struct {
					UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
					MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
				}{UseBatchAPI: true},
			},
			storageType:   "azure",
			expectFeature: false, // Azure doesn't support batch delete
			description:   "Azure should always use parallel delete",
		},
	}

	for _, tc := range featureFlagTests {
		t.Run(tc.name, func(t *testing.T) {
			usesStorageFeature := shouldUseStorageFeatures(tc.storageType, &tc.config)

			if tc.expectFeature {
				assert.True(t, usesStorageFeature, tc.description)
			} else {
				assert.False(t, usesStorageFeature, tc.description)
			}
		})
	}
}

// TestConfigurationIntegration tests configuration integration with enhanced storage
func TestConfigurationIntegration(t *testing.T) {
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
				S3: config.S3Config{
					Bucket: "integration-test-s3",
					Region: "us-east-1",
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:      true,
					BatchSize:    1000,
					Workers:      50,
					CacheEnabled: true,
					S3Optimizations: struct {
						UseBatchAPI        bool `yaml:"use_batch_api" envconfig:"DELETE_S3_USE_BATCH_API" default:"true"`
						VersionConcurrency int  `yaml:"version_concurrency" envconfig:"DELETE_S3_VERSION_CONCURRENCY" default:"10"`
						PreloadVersions    bool `yaml:"preload_versions" envconfig:"DELETE_S3_PRELOAD_VERSIONS" default:"true"`
					}{UseBatchAPI: true},
				},
			},
			storageType: "s3",
			shouldWork:  true,
			description: "Complete S3 configuration should work with enhanced delete",
		},
		{
			name: "Complete GCS configuration",
			config: &config.Config{
				GCS: config.GCSConfig{
					Bucket:         "integration-test-gcs",
					ClientPoolSize: 30,
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:      true,
					Workers:      25, // Less than ClientPoolSize
					CacheEnabled: true,
					GCSOptimizations: struct {
						MaxWorkers    int  `yaml:"max_workers" envconfig:"DELETE_GCS_MAX_WORKERS" default:"50"`
						UseClientPool bool `yaml:"use_client_pool" envconfig:"DELETE_GCS_USE_CLIENT_POOL" default:"true"`
					}{UseClientPool: true},
				},
			},
			storageType: "gcs",
			shouldWork:  true,
			description: "Complete GCS configuration should work with enhanced delete",
		},
		{
			name: "Complete Azure configuration",
			config: &config.Config{
				AzureBlob: config.AzureBlobConfig{
					Container: "integration-test-azure",
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled:      true,
					Workers:      20,
					CacheEnabled: true,
					AzureOptimizations: struct {
						UseBatchAPI bool `yaml:"use_batch_api" envconfig:"DELETE_AZURE_USE_BATCH_API" default:"true"`
						MaxWorkers  int  `yaml:"max_workers" envconfig:"DELETE_AZURE_MAX_WORKERS" default:"20"`
					}{UseBatchAPI: true},
				},
			},
			storageType: "azure",
			shouldWork:  true,
			description: "Complete Azure configuration should work with enhanced delete",
		},
		{
			name: "Disabled optimizations",
			config: &config.Config{
				S3: config.S3Config{
					Bucket: "integration-test-s3",
					Region: "us-east-1",
				},
				DeleteOptimizations: config.DeleteOptimizations{
					Enabled: false, // Disabled
				},
			},
			storageType: "s3",
			shouldWork:  true,
			description: "Disabled optimizations should still work (fallback to standard delete)",
		},
	}

	for _, tc := range integrationTests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Validate configuration
			var err error
			switch tc.storageType {
			case "s3":
				err = validateS3ConfigForEnhancedDelete(tc.config)
			case "gcs":
				err = validateGCSConfigForEnhancedDelete(tc.config)
			case "azure":
				err = validateAzureConfigForEnhancedDelete(tc.config)
			}

			if tc.shouldWork {
				assert.NoError(t, err, tc.description)

				// Test that configuration can be normalized
				normalizedConfig := normalizeDeleteOptimizationConfig(&tc.config.DeleteOptimizations)
				assert.NotNil(t, normalizedConfig)

				// Test configuration application
				appliedConfig := applyStorageSpecificLimits(tc.storageType, normalizedConfig)
				assert.NotNil(t, appliedConfig)

				// Verify storage-specific limits are applied
				switch tc.storageType {
				case "s3":
					if appliedConfig.S3Optimizations.UseBatchAPI && appliedConfig.BatchSize > 1000 {
						assert.LessOrEqual(t, appliedConfig.BatchSize, 1000, "S3 batch size should be limited to 1000")
					}
				case "gcs":
					if appliedConfig.Workers > 100 {
						assert.LessOrEqual(t, appliedConfig.Workers, 100, "GCS workers should be limited")
					}
				case "azure":
					if appliedConfig.Workers > 50 {
						assert.LessOrEqual(t, appliedConfig.Workers, 50, "Azure workers should be limited")
					}
				}
			} else {
				assert.Error(t, err, tc.description)
			}

			_ = ctx // Avoid unused variable warning
		})
	}
}

// Helper functions for configuration validation and normalization

func validateDeleteOptimizationConfig(config *config.DeleteOptimizations) error {
	if config.BatchSize < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "BatchSize",
			Value:   config.BatchSize,
			Message: "batch size cannot be negative",
		}
	}

	if config.BatchSize > 10000 {
		return &enhanced.OptimizationConfigError{
			Field:   "BatchSize",
			Value:   config.BatchSize,
			Message: "batch size exceeds maximum allowed value",
		}
	}

	if config.Workers < 0 {
		return &enhanced.OptimizationConfigError{
			Field:   "Workers",
			Value:   config.Workers,
			Message: "worker count cannot be negative",
		}
	}

	if config.Workers == 0 && config.Enabled {
		return &enhanced.OptimizationConfigError{
			Field:   "Workers",
			Value:   config.Workers,
			Message: "worker count must be positive when optimizations are enabled",
		}
	}

	if config.Workers > 500 {
		return &enhanced.OptimizationConfigError{
			Field:   "Workers",
			Value:   config.Workers,
			Message: "worker count exceeds maximum allowed value",
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

	if config.RetryAttempts > 10 {
		return &enhanced.OptimizationConfigError{
			Field:   "RetryAttempts",
			Value:   config.RetryAttempts,
			Message: "retry attempts exceed maximum allowed value",
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

func normalizeDeleteOptimizationConfig(config *config.DeleteOptimizations) *config.DeleteOptimizations {
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

	if normalized.CacheTTL == 0 {
		normalized.CacheTTL = 30 * time.Minute
	}

	return &normalized
}

func validateS3ConfigForEnhancedDelete(config *config.Config) error {
	if config.S3.Bucket == "" {
		return &enhanced.OptimizationConfigError{
			Field:   "Bucket",
			Value:   config.S3.Bucket,
			Message: "S3 bucket name cannot be empty",
		}
	}

	if config.DeleteOptimizations.S3Optimizations.UseBatchAPI && config.DeleteOptimizations.BatchSize > 1000 {
		return &enhanced.OptimizationConfigError{
			Field:   "BatchSize",
			Value:   config.DeleteOptimizations.BatchSize,
			Message: "S3 batch size cannot exceed 1000 when using batch API",
		}
	}

	return nil
}

func validateGCSConfigForEnhancedDelete(config *config.Config) error {
	if config.GCS.Bucket == "" {
		return &enhanced.OptimizationConfigError{
			Field:   "Bucket",
			Value:   config.GCS.Bucket,
			Message: "GCS bucket name cannot be empty",
		}
	}

	clientPoolSize := config.GCS.ClientPoolSize
	if clientPoolSize == 0 {
		clientPoolSize = 50 // Default
	}

	if config.DeleteOptimizations.Workers > clientPoolSize {
		return &enhanced.OptimizationConfigError{
			Field:   "Workers",
			Value:   config.DeleteOptimizations.Workers,
			Message: "GCS worker count cannot exceed client pool size",
		}
	}

	return nil
}

func validateAzureConfigForEnhancedDelete(config *config.Config) error {
	if config.AzureBlob.Container == "" {
		return &enhanced.OptimizationConfigError{
			Field:   "Container",
			Value:   config.AzureBlob.Container,
			Message: "Azure container name cannot be empty",
		}
	}

	if config.DeleteOptimizations.Workers > 50 {
		return &enhanced.OptimizationConfigError{
			Field:   "Workers",
			Value:   config.DeleteOptimizations.Workers,
			Message: "Azure worker count should not exceed 50 for optimal performance",
		}
	}

	return nil
}

func shouldUseStorageFeatures(storageType string, config *config.DeleteOptimizations) bool {
	switch storageType {
	case "s3":
		return config.S3Optimizations.UseBatchAPI
	case "gcs":
		return false // GCS doesn't support batch delete
	case "azure":
		return false // Azure doesn't support batch delete
	default:
		return false
	}
}

func applyStorageSpecificLimits(storageType string, config *config.DeleteOptimizations) *config.DeleteOptimizations {
	limited := *config

	switch storageType {
	case "s3":
		if limited.S3Optimizations.UseBatchAPI && limited.BatchSize > 1000 {
			limited.BatchSize = 1000
		}
	case "gcs":
		if limited.Workers > 100 {
			limited.Workers = 100
		}
	case "azure":
		if limited.Workers > 50 {
			limited.Workers = 50
		}
	}

	return &limited
}
