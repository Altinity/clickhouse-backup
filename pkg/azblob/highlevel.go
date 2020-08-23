// forked from github.com/Azure/azure-storage-blob-go/azblob/blob/feature/clientprovidedkey because UploadStreamToBlockBlob does not expose CPK
package azblob

import (
	"context"
	"io"

	. "github.com/Azure/azure-storage-blob-go/azblob"
)

// UploadStreamToBlockBlob copies the file held in io.Reader to the Blob at blockBlobURL.
// A Context deadline or cancellation will cause this to error.
func UploadStreamToBlockBlob(ctx context.Context, reader io.Reader, blockBlobURL BlockBlobURL,
	o UploadStreamToBlockBlobOptions, cpk ClientProvidedKeyOptions) (CommonResponse, error) {

	result, err := copyFromReader(ctx, reader, blockBlobURL, o, cpk)
	if err != nil {
		return nil, err
	}

	return result, nil
}
