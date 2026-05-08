package storage

// Tests that each backend maps its "object not found" errors to the public
// ErrNotFound sentinel. The goal is to lock the intent so that accidentally
// removing or changing the not-found check causes a test failure.
//
// Backends where the classification is buried inside an exported method that
// requires a live connection use t.Skip with a pointer to the integration test
// that provides the load-bearing coverage.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	cos "github.com/tencentyun/cos-go-sdk-v5"
)

func TestStorage_NotFoundClassification(t *testing.T) {

	// ── S3 ────────────────────────────────────────────────────────────────────
	// Spin up a minimal httptest server that always returns HTTP 404, wire a
	// real aws-sdk-go-v2 s3.Client at it, and exercise StatFileAbsolute. This
	// calls the actual production code path (pkg/storage/s3.go:786-806).
	t.Run("s3", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		s3Client := s3.New(s3.Options{
			Region: "us-east-1",
			Credentials: credentials.NewStaticCredentialsProvider(
				"test-key", "test-secret", "",
			),
			HTTPClient:   srv.Client(),
			BaseEndpoint: aws.String(srv.URL),
			// Path-style so the bucket name goes in the URL path, not the host,
			// which works correctly against a single-host test server.
			UsePathStyle: true,
		})

		backend := &S3{
			client: s3Client,
			Config: &config.S3Config{
				Bucket: "test-bucket",
				Region: "us-east-1",
			},
		}

		_, err := backend.StatFileAbsolute(context.Background(), "does/not/exist")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("S3 StatFileAbsolute with 404 response: got %v, want ErrNotFound", err)
		}
	})

	// ── Azure Blob ────────────────────────────────────────────────────────────
	// The azure-storage-blob-go SDK wraps the not-found condition in a private
	// *storageError struct whose constructor reads live HTTP response headers;
	// there is no public constructor that accepts an arbitrary service code.
	// The classification (pkg/storage/azblob.go:317,361) is therefore only
	// testable end-to-end.
	// Integration coverage: TestIntegrationAzureBlob / TestAzureBlob_StatFile
	// in test/integration/.
	t.Run("azblob", func(t *testing.T) {
		t.Skip("azblob: storageError is a private type with no public constructor; " +
			"not-found mapping is covered by integration tests " +
			"(TestIntegrationAzureBlob / TestAzureBlob_StatFile)")
	})

	// ── GCS ───────────────────────────────────────────────────────────────────
	// The GCS path (pkg/storage/gcs.go) maps cloud.google.com/go/storage
	// ErrObjectNotExist → ErrNotFound via the production helper gcsIsNotFound.
	// The GCS client pools require live auth, so we verify the sentinel identity
	// directly by calling the production helper rather than StatFileAbsolute.
	//
	// Integration coverage: TestIntegrationGCS / TestGCS_StatFile in
	// test/integration/.
	t.Run("gcs", func(t *testing.T) {
		// Import-path note: "cloud.google.com/go/storage" is imported as
		// "storage" in gcs.go but we access it here via the alias defined
		// in gcs_testhelper_test.go (see gcsErrObjectNotExist below).
		syntheticErr := gcsErrObjectNotExist() // sentinel from helper below
		if !gcsIsNotFound(syntheticErr) {
			t.Fatalf("GCS not-found classification: gcsIsNotFound(%v) = false, want true", syntheticErr)
		}
	})

	// ── COS ───────────────────────────────────────────────────────────────────
	// The COS path (pkg/storage/cos.go) checks cosErr.Code == "NoSuchKey" via
	// the production helper cosIsNotFound. cos.ErrorResponse is a public struct,
	// so we can construct a synthetic one and feed it directly to the production
	// helper.
	t.Run("cos", func(t *testing.T) {
		syntheticErr := &cos.ErrorResponse{
			Response: &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    &http.Request{},
			},
			Code:    "NoSuchKey",
			Message: "The specified key does not exist.",
		}

		if !cosIsNotFound(syntheticErr) {
			t.Fatalf("COS not-found classification: cosIsNotFound(%v) = false, want true", syntheticErr)
		}
	})

	// ── SFTP ──────────────────────────────────────────────────────────────────
	// The SFTP path (pkg/storage/sftp.go:111) calls sftp.sftpClient.Stat which
	// requires a live SFTP connection. The not-found check is a string match
	// (strings.Contains(err.Error(), "not exist")) applied to errors returned
	// by the SSH/SFTP library; there is no way to inject an error without
	// dialling a server.
	//
	// Integration coverage: TestIntegrationSFTP / TestSFTP_StatFile in
	// test/integration/.
	t.Run("sftp", func(t *testing.T) {
		t.Skip("sftp: StatFileAbsolute calls sftpClient.Stat which requires a live " +
			"SFTP connection; covered by integration tests " +
			"(TestIntegrationSFTP / TestSFTP_StatFile)")
	})

	// ── FTP ───────────────────────────────────────────────────────────────────
	// The FTP path (pkg/storage/ftp.go) uses the production helper ftpIsNotFound
	// which checks strings.HasPrefix(err.Error(), "550") for List/Delete errors.
	// Both classification and the "file not found in entries list" path happen
	// inside StatFileAbsolute after getConnectionFromPool (which dials a live
	// FTP server). We exercise the production helper directly using a synthetic
	// textproto.Error (the exact type returned by github.com/jlaffaye/ftp).
	t.Run("ftp", func(t *testing.T) {
		// Verify the production helper classifies a 550 error as not-found.
		err550 := &textproto.Error{Code: 550, Msg: "No such file or directory"}
		if !ftpIsNotFound(err550) {
			t.Fatalf("FTP not-found classification (550): ftpIsNotFound(%v) = false, want true", err550)
		}

		// Verify that a non-550 error is NOT classified as not-found.
		err530 := &textproto.Error{Code: 530, Msg: "Not logged in"}
		if ftpIsNotFound(err530) {
			t.Fatal("FTP non-550 error was incorrectly classified as not-found")
		}
	})
}

// gcsErrObjectNotExist returns the GCS sentinel that the production code
// compares against in gcsIsNotFound (gcs.go). It lives in a separate file
// (gcs_testhelper_test.go) so that the cloud.google.com/go/storage import
// does not collide with the package-level "storage" identifier here.
func gcsErrObjectNotExist() error {
	return gcsGetErrObjectNotExist()
}
