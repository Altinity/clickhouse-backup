package storage

// gcsGetErrObjectNotExist returns the cloud.google.com/go/storage.ErrObjectNotExist
// sentinel. It lives in this file so that the gcs-storage import alias does not
// conflict with the package-level "storage" name in errors_test.go.

import (
	gcsStorage "cloud.google.com/go/storage"
)

func gcsGetErrObjectNotExist() error {
	return gcsStorage.ErrObjectNotExist
}
