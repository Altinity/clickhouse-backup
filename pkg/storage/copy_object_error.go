package storage

import (
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

// ErrCopyObjectUnsupported marks storages which can't perform server-side CopyObject at all.
var ErrCopyObjectUnsupported = errors.New("server-side CopyObject not supported")

// IsPermanentCopyObjectError reports whether a server-side CopyObject error can't be
// resolved by retrying (unsupported operation or missing permissions), so the caller
// should fall back to streaming copy instead of wasting the retry backoff budget.
func IsPermanentCopyObjectError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrCopyObjectUnsupported) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "NotImplemented", "InvalidRequest",
			// object disks can live on a different endpoint/provider than the backup destination
			// (e.g. disk_gcs/disk_cos/disk_azblob vs minio), then the destination can't see the
			// source bucket at all and retrying can never succeed
			"NoSuchBucket", "NoSuchKey", "NotFound", "PermanentRedirect":
			return true
		}
	}
	var gcpErr *googleapi.Error
	if errors.As(err, &gcpErr) {
		switch gcpErr.Code {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotImplemented, http.StatusNotFound:
			return true
		}
	}
	var azErr *azcore.ResponseError
	if errors.As(err, &azErr) {
		if azErr.ErrorCode == string(bloberror.CannotVerifyCopySource) {
			return true
		}
		switch azErr.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotImplemented, http.StatusNotFound:
			return true
		}
	}
	return false
}
