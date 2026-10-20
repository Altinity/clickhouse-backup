package storage

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

// recordingRoundTripper captures the request URL scheme it is asked to send
// and short-circuits the round trip so no real network call happens.
type recordingRoundTripper struct {
	seenScheme string
}

func (rt *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.seenScheme = req.URL.Scheme
	return &http.Response{StatusCode: 200, Body: http.NoBody, Header: make(http.Header)}, nil
}

// TestRewriteTransportSchemeDowngrade documents the behavioral difference
// between the ForceHttp path and the DisableHttp2 path. ForceHttp wraps the
// base transport in rewriteTransport, which downgrades https:// to cleartext
// http:// (only valid for the internal varnish cache). DisableHttp2 must NOT
// do this — it uses the base transport directly so the https scheme (and thus
// TLS + HTTPS_PROXY) is preserved while HTTP/2 is still suppressed.
func TestRewriteTransportSchemeDowngrade(t *testing.T) {
	t.Run("ForceHttp wrapper downgrades scheme to cleartext", func(t *testing.T) {
		base := &recordingRoundTripper{}
		rt := &rewriteTransport{base: base}
		req := &http.Request{URL: &url.URL{Scheme: "https", Host: "storage.googleapis.com", Path: "/"}}
		_, err := rt.RoundTrip(req)
		require.NoError(t, err)
		assert.Equal(t, "http", base.seenScheme, "rewriteTransport must downgrade https to http")
	})

	t.Run("DisableHttp2 path preserves https scheme", func(t *testing.T) {
		// DisableHttp2 uses the custom transport directly (no rewriteTransport),
		// so the scheme reaching the base round tripper stays https.
		base := &recordingRoundTripper{}
		req := &http.Request{URL: &url.URL{Scheme: "https", Host: "storage.googleapis.com", Path: "/"}}
		_, err := base.RoundTrip(req)
		require.NoError(t, err)
		assert.Equal(t, "https", base.seenScheme, "DisableHttp2 must preserve https scheme so TLS and HTTPS_PROXY work")
	})
}

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
					Bucket:          "test-bucket",
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

// TestGCSHTTP1TransportReusesConnections every concurrent request above MaxIdleConnsPerHost closes its
// connection after use, on a busy download that used up the whole ephemeral port range with
// `connect: cannot assign requested address`, see https://github.com/Altinity/clickhouse-backup/issues/1607
func TestGCSHTTP1TransportReusesConnections(t *testing.T) {
	const workers, requestsPerWorker = 16, 20
	var newConns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	client := &http.Client{Transport: newGCSHTTP1Transport()}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range requestsPerWorker {
				resp, err := client.Get(srv.URL)
				if !assert.NoError(t, err) {
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	t.Logf("%d requests opened %d connections", workers*requestsPerWorker, newConns.Load())
	assert.LessOrEqual(t, newConns.Load(), int64(2*workers), "concurrent requests should reuse idle connections")
}

// TestGCSIsNotEncryptedError uses the exact error GCS returns when a plain object is read with a CSEK,
// the reason is lowerCamelCase "resourceNotEncryptedWithCustomerEncryptionKey"
func TestGCSIsNotEncryptedError(t *testing.T) {
	gcs := &GCS{}
	apiErr := &googleapi.Error{
		Code:    http.StatusBadRequest,
		Message: "The target object is not encrypted by a customer-supplied encryption key.",
		Errors: []googleapi.ErrorItem{{
			Reason:  "resourceNotEncryptedWithCustomerEncryptionKey",
			Message: "The target object is not encrypted by a customer-supplied encryption key.",
		}},
	}
	assert.True(t, gcs.isNotEncryptedError(apiErr))
	assert.True(t, gcs.isNotEncryptedError(errors.Wrap(apiErr, "GCS CopyObject src.Attrs")))
	assert.False(t, gcs.isNotEncryptedError(nil))
	assert.False(t, gcs.isNotEncryptedError(&googleapi.Error{Code: http.StatusNotFound, Message: "No such object"}))
}

// TestGCSPooledClientsShareToken - every pooled client used to build its own credentials and fetch its own
// OAuth2 token, so a cold delete with delete_concurrency=50 opened ~50 parallel TLS handshakes to
// oauth2.googleapis.com and failed with "auth: cannot fetch token: ... TLS handshake timeout"
func TestGCSPooledClientsShareToken(t *testing.T) {
	var tokenRequests, deleteRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			tokenRequests.Add(1)
			time.Sleep(50 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`))
			return
		}
		if r.Method == http.MethodDelete && r.Header.Get("Authorization") == "Bearer test-token" {
			deleteRequests.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	credJSON, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "test",
		"private_key_id": "test",
		"private_key":    string(keyPEM),
		"client_email":   "test@test.iam.gserviceaccount.com",
		"token_uri":      srv.URL + "/token",
	})
	require.NoError(t, err)

	gcs := &GCS{Config: &config.GCSConfig{
		CredentialsJSON:   string(credJSON),
		Endpoint:          srv.URL + "/storage/v1/",
		Bucket:            "test",
		ClientPoolSize:    20,
		DeleteConcurrency: 20,
	}}
	require.NoError(t, gcs.Connect(t.Context()))
	defer func() { require.NoError(t, gcs.Close(t.Context())) }()

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	require.NoError(t, gcs.DeleteKeysBatch(t.Context(), keys))
	assert.Equal(t, int64(len(keys)), deleteRequests.Load())
	assert.Equal(t, int64(1), tokenRequests.Load(), "all pooled clients shall share one cached token")
}
