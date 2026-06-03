package storage

import "testing"

func TestBackupDestinationKeepsSharedLimitersPerDirection(t *testing.T) {
	bd := &BackupDestination{}

	uploadLimiter1 := bd.UploadLimiter(1024)
	uploadLimiter2 := bd.UploadLimiter(1024)
	downloadLimiter := bd.DownloadLimiter(1024)

	if uploadLimiter1 == nil {
		t.Fatal("UploadLimiter() returned nil")
	}
	if uploadLimiter1 != uploadLimiter2 {
		t.Fatal("UploadLimiter() did not reuse the shared limiter")
	}
	if uploadLimiter1 == downloadLimiter {
		t.Fatal("upload and download limiters should be independent")
	}
	if bd.UploadLimiter(0) != nil {
		t.Fatal("UploadLimiter(0) should return nil when throttling is disabled")
	}
	if reused := bd.UploadLimiter(2048); reused == uploadLimiter1 {
		t.Fatal("UploadLimiter() should rebuild the limiter when the rate changes")
	}
}
