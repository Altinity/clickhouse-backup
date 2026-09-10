package storage

import (
	"errors"
	"io/fs"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/api/googleapi"
)

// IsNotFoundErr reports whether err means the remote object is permanently missing
// (S3 NoSuchKey/404, GCS ErrObjectNotExist/404, Azure BlobNotFound/404, FTP 550, SFTP/local fs.ErrNotExist),
// so retrying can never succeed, see https://github.com/Altinity/clickhouse-backup/issues/1456
func IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, storage.ErrObjectNotExist) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	var httpErr *smithyhttp.ResponseError
	if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var gcpErr *googleapi.Error
	if errors.As(err, &gcpErr) && gcpErr.Code == http.StatusNotFound {
		return true
	}
	var azErr *azcore.ResponseError
	if errors.As(err, &azErr) && (azErr.StatusCode == http.StatusNotFound || azErr.ErrorCode == string(bloberror.BlobNotFound)) {
		return true
	}
	// every backend phrases "object is missing" differently and some wrap it as a plain string,
	// so fall back to the known permanent-not-found markers across S3/GCS/Azure/FTP/SFTP/FS
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"doesn't exist",             // GCS
		"does not exist",            // SFTP ("file does not exist"), Azure ("the specified blob does not exist")
		"no such file or directory", // FTP (550), local filesystem
		"key not found",
		"nosuchkey",      // S3
		"blobnotfound",   // Azure Blob (x-ms-error-code)
		"statuscode 404", // S3 SDK v2
		"statuscode: 404",
		"status: 404", // Azure ("RESPONSE Status: 404")
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
