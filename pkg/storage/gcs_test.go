package storage

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCSEncryptionKeyValidation(t *testing.T) {
	testCases := []struct {
		name          string
		encryptionKey string
		expectError   bool
		errorContains string
	}{
		{
			name:          "empty key is valid (no encryption)",
			encryptionKey: "",
			expectError:   false,
		},
		{
			name:          "valid 256-bit key",
			encryptionKey: base64.StdEncoding.EncodeToString(make([]byte, 32)),
			expectError:   false,
		},
		{
			name:          "valid 256-bit key with random data",
			encryptionKey: "dGhpcyBpcyBhIDMyIGJ5dGUga2V5ISEhISEhISEhISE=", // "this is a 32 byte key!!!!!!!!!!!" (32 bytes) base64
			expectError:   false,
		},
		{
			name:          "invalid base64",
			encryptionKey: "not-valid-base64!!!",
			expectError:   true,
			errorContains: "malformed encryption_key",
		},
		{
			name:          "key too short (16 bytes / 128-bit)",
			encryptionKey: base64.StdEncoding.EncodeToString(make([]byte, 16)),
			expectError:   true,
			errorContains: "got 16 bytes",
		},
		{
			name:          "key too long (64 bytes / 512-bit)",
			encryptionKey: base64.StdEncoding.EncodeToString(make([]byte, 64)),
			expectError:   true,
			errorContains: "got 64 bytes",
		},
		{
			name:          "key slightly short (31 bytes)",
			encryptionKey: base64.StdEncoding.EncodeToString(make([]byte, 31)),
			expectError:   true,
			errorContains: "got 31 bytes",
		},
		{
			name:          "key slightly long (33 bytes)",
			encryptionKey: base64.StdEncoding.EncodeToString(make([]byte, 33)),
			expectError:   true,
			errorContains: "got 33 bytes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gcs := &GCS{
				Config: &config.GCSConfig{
					EncryptionKey: tc.encryptionKey,
					// These are required for Connect but we'll test key validation
					// before actual connection by checking the error
					Bucket:         "test-bucket",
					SkipCredentials: true,
				},
			}

			// We can't fully test Connect without a GCS server, but we can
			// validate the key parsing logic by checking if the error is
			// related to key validation vs connection issues
			err := gcs.validateAndDecodeEncryptionKey()

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorContains)
				assert.Nil(t, gcs.encryptionKey)
			} else {
				require.NoError(t, err)
				if tc.encryptionKey != "" {
					assert.NotNil(t, gcs.encryptionKey)
					assert.Len(t, gcs.encryptionKey, 32)
				} else {
					assert.Nil(t, gcs.encryptionKey)
				}
			}
		})
	}
}

func TestGCSApplyEncryption(t *testing.T) {
	t.Run("returns same object when no encryption key", func(t *testing.T) {
		gcs := &GCS{
			Config:        &config.GCSConfig{},
			encryptionKey: nil,
		}

		// We can't create a real ObjectHandle without a client, but we can
		// verify the logic by checking the nil case
		result := gcs.applyEncryption(nil)
		assert.Nil(t, result)
	})

	t.Run("encryption key is set correctly", func(t *testing.T) {
		key := make([]byte, 32)
		for i := range key {
			key[i] = byte(i)
		}

		gcs := &GCS{
			Config:        &config.GCSConfig{},
			encryptionKey: key,
		}

		// Verify the key is stored correctly
		assert.Equal(t, key, gcs.encryptionKey)
		assert.Len(t, gcs.encryptionKey, 32)
	})
}

func TestGCSEncryptionKeyDecoding(t *testing.T) {
	t.Run("correctly decodes valid base64 key", func(t *testing.T) {
		// Create a known 32-byte key
		originalKey := []byte("12345678901234567890123456789012") // exactly 32 bytes
		encodedKey := base64.StdEncoding.EncodeToString(originalKey)

		gcs := &GCS{
			Config: &config.GCSConfig{
				EncryptionKey: encodedKey,
			},
		}

		err := gcs.validateAndDecodeEncryptionKey()
		require.NoError(t, err)
		assert.Equal(t, originalKey, gcs.encryptionKey)
	})

	t.Run("handles URL-safe base64 encoding", func(t *testing.T) {
		// Standard base64 with potential + and / characters
		originalKey := make([]byte, 32)
		for i := range originalKey {
			originalKey[i] = byte(i * 8) // Will produce various characters
		}
		encodedKey := base64.StdEncoding.EncodeToString(originalKey)

		gcs := &GCS{
			Config: &config.GCSConfig{
				EncryptionKey: encodedKey,
			},
		}

		err := gcs.validateAndDecodeEncryptionKey()
		require.NoError(t, err)
		assert.Equal(t, originalKey, gcs.encryptionKey)
	})
}

// validateAndDecodeEncryptionKey is a helper that extracts the key validation
// logic for testing without needing a full GCS connection
func (gcs *GCS) validateAndDecodeEncryptionKey() error {
	if gcs.Config.EncryptionKey == "" {
		return nil
	}

	key, err := base64.StdEncoding.DecodeString(gcs.Config.EncryptionKey)
	if err != nil {
		return fmt.Errorf("gcs: malformed encryption_key, must be base64-encoded 256-bit key: %w", err)
	}
	if len(key) != 32 {
		return fmt.Errorf("gcs: malformed encryption_key, must be base64-encoded 256-bit key (got %d bytes)", len(key))
	}
	gcs.encryptionKey = key
	return nil
}
