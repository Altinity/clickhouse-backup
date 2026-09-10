package storage

import (
	"errors"
	"io/fs"
	"net/http"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestIsNotFoundErr(t *testing.T) {
	notFoundMessages := []string{
		"object doesn't exist",
		"key not found: metadata/default/test.json",
		"NoSuchKey: The specified key does not exist",
		"operation error S3: GetObject, https response error StatusCode: 404",
		"StatusCode 404",
		// real backend phrasings observed in test/integration TestMetadataNotFound*
		"550 /backup/metadata/default/test.json: No such file or directory", // FTP
		"file does not exist", // SFTP
		"AzureBlob GetFileReaderAbsolute Download: RESPONSE ERROR (ServiceCode=BlobNotFound) RESPONSE Status: 404 The specified blob does not exist.", // Azure
	}
	for _, msg := range notFoundMessages {
		assert.True(t, IsNotFoundErr(errors.New(msg)), msg)
	}

	// typed errors from SDKs and wrapped sentinels
	assert.True(t, IsNotFoundErr(NewErrNotFound("metadata/default/test.json")))
	assert.True(t, IsNotFoundErr(pkgerrors.Wrap(NewErrNotFound("k"), "DownloadCompressedStream StatFile")))
	assert.True(t, IsNotFoundErr(pkgerrors.Wrap(fs.ErrNotExist, "sftp")))
	assert.True(t, IsNotFoundErr(gcs.ErrObjectNotExist))
	assert.True(t, IsNotFoundErr(&smithy.GenericAPIError{Code: "NoSuchKey", Message: "x"}))
	assert.True(t, IsNotFoundErr(&smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 404}}, Err: errors.New("x")}))
	assert.True(t, IsNotFoundErr(&googleapi.Error{Code: 404}))
	assert.True(t, IsNotFoundErr(&azcore.ResponseError{StatusCode: 404}))
	assert.True(t, IsNotFoundErr(&azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}))

	assert.False(t, IsNotFoundErr(nil))
	assert.False(t, IsNotFoundErr(errors.New("temporary network timeout")))
	assert.False(t, IsNotFoundErr(&smithy.GenericAPIError{Code: "SlowDown"}))
	assert.False(t, IsNotFoundErr(&smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 503}}, Err: errors.New("x")}))
	assert.False(t, IsNotFoundErr(&googleapi.Error{Code: 503}))
	assert.False(t, IsNotFoundErr(&azcore.ResponseError{StatusCode: 500}))
}
