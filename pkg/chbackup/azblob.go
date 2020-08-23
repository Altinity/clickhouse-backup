package chbackup

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"fmt"
	"net/url"
	"time"

	. "github.com/Azure/azure-storage-blob-go/azblob"
	x "github.com/oneiq/clickhouse-backup/pkg/azblob"
	"github.com/pkg/errors"
)

// AzureBlob - presents methods for manipulate data on Azure
type AzureBlob struct {
	Container ContainerURL
	CPK       ClientProvidedKeyOptions
	Config    *AzureBlobConfig
}

// Connect - connect to Azure
func (s *AzureBlob) Connect() error {
	var (
		err        error
		urlString  string
		credential Credential
	)
	switch {
	case s.Config.Container == "":
		return fmt.Errorf("azblob: container name not set")
	case s.Config.AccountName == "":
		return fmt.Errorf("azblob: account name not set")
	case s.Config.AccountKey != "":
		credential, err = NewSharedKeyCredential(s.Config.AccountName, s.Config.AccountKey)
		if err != nil {
	    	return err
		}
        urlString  = fmt.Sprintf("https://%s.blob.%s", s.Config.AccountName, s.Config.EndpointSuffix)
	case s.Config.SharedAccessSignature != "":
		credential = NewAnonymousCredential()
        urlString  = fmt.Sprintf("https://%s.blob.%s?%s", s.Config.AccountName, s.Config.EndpointSuffix, s.Config.SharedAccessSignature)
	default:
		return fmt.Errorf("azblob: account key or SAS must be set")
	}
	u, err := url.Parse(urlString)
	if err != nil {
		return err
	}

	container := NewServiceURL(*u, NewPipeline(credential, PipelineOptions{})).NewContainerURL(s.Config.Container)
	context   := context.Background()

	if _, err  = container.Create(context, Metadata{}, PublicAccessContainer); err != nil {
		if se, ok := err.(StorageError); !ok || se.ServiceCode() != ServiceCodeContainerAlreadyExists {
			return errors.Wrapf(err, "azblob: failed to create container %s", s.Config.Container)
		}
	}

	if s.Config.SSE != "" {
		key, err := base64.StdEncoding.DecodeString(s.Config.SSE)
		if err != nil {
			return errors.Wrapf(err, "azblob: malformed SSE key, must be base64-encoded 256-bit key")
		} else if len(key) != 32 {
			return fmt.Errorf("azblob: malformed SSE key, must be base64-encoded 256-bit key")
		}

		// grrr
		b64key := s.Config.SSE
		shakey := sha256.Sum256(key)
		b64sha := base64.StdEncoding.EncodeToString(shakey[:])
		s.CPK   = NewClientProvidedKeyOptions(&b64key, &b64sha, nil)
	}

	s.Container = container
	return nil
}

func (s *AzureBlob) Kind() string {
	return "azblob"
}

func (s *AzureBlob) GetFileReader(key string) (io.ReadCloser, error) {
	ctx  := context.Background()
	blob := s.Container.NewBlockBlobURL(key)

	r, err := blob.Download(ctx, 0, CountToEnd, BlobAccessConditions{}, false, s.CPK)
	if err != nil {
		return nil, err
	}

	return r.Body(RetryReaderOptions{}), nil
}

func (s *AzureBlob) PutFile(key string, r io.ReadCloser) error {
	ctx  := context.Background()
	blob := s.Container.NewBlockBlobURL(key)

	bufferSize := 2 * 1024 * 1024 // Configure the size of the rotating buffers that are used when uploading
	maxBuffers := 3               // Configure the number of rotating buffers that are used when uploading
	_, err := x.UploadStreamToBlockBlob(ctx, r, blob, UploadStreamToBlockBlobOptions{ BufferSize: bufferSize, MaxBuffers: maxBuffers }, s.CPK)
	return err
}

func (s *AzureBlob) DeleteFile(key string) error {
	ctx  := context.Background()
	blob := s.Container.NewBlockBlobURL(key)

	_, err := blob.Delete(ctx, DeleteSnapshotsOptionInclude, BlobAccessConditions{})
	return err
}

func (s *AzureBlob) GetFile(key string) (RemoteFile, error) {
	ctx  := context.Background()
	blob := s.Container.NewBlockBlobURL(key)

	r, err := blob.GetProperties(ctx, BlobAccessConditions{}, s.CPK)
	if err != nil {
		if se, ok := err.(StorageError); !ok || se.ServiceCode() != ServiceCodeBlobNotFound {
			return nil, err
		}
		return nil, ErrNotFound
	}

	return &azureBlobFile{
   		name: key,
   		size: r.ContentLength(),
   		lastModified: r.LastModified(),
   	}, nil
}

func (s *AzureBlob) Walk(_ string, process func(r RemoteFile)) error {
	ctx := context.Background()
	opt := ListBlobsSegmentOptions{ Prefix: s.Config.Path }
	mrk := Marker{}

	for mrk.NotDone() {
		r, err := s.Container.ListBlobsFlatSegment(ctx, mrk, opt)
		if err != nil {
			return err
		}
		for _, blob := range r.Segment.BlobItems {
			var size int64
			if blob.Properties.ContentLength != nil {
				size = *blob.Properties.ContentLength
			} else {
				size = 0
			}
			process(&azureBlobFile{
				name: blob.Name,
				size: size,
				lastModified: blob.Properties.LastModified,
			})
		}

		mrk = r.NextMarker
	}

	return nil
}

type azureBlobFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *azureBlobFile) Size() int64 {
	return f.size
}

func (f *azureBlobFile) Name() string {
	return f.name
}

func (f *azureBlobFile) LastModified() time.Time {
	return f.lastModified
}
