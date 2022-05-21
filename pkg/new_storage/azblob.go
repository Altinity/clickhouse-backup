package new_storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	x "github.com/AlexAkulov/clickhouse-backup/pkg/new_storage/azblob"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
)

// AzureBlob - presents methods for manipulate data on Azure
type AzureBlob struct {
	Container azblob.ContainerURL
	CPK       azblob.ClientProvidedKeyOptions
	Config    *config.AzureBlobConfig
}

// Connect - connect to Azure
func (s *AzureBlob) Connect() error {
	if s.Config.EndpointSuffix == "" {
		return fmt.Errorf("endpoint suffix not set")
	}
	if s.Config.Container == "" {
		return fmt.Errorf("container name not set")
	}
	if s.Config.AccountName == "" {
		return fmt.Errorf("account name not set")
	}
	if s.Config.AccountKey == "" && s.Config.SharedAccessSignature == "" && !s.Config.UseManagedIdentity {
		return fmt.Errorf("account key or SAS or use_managed_identity must be set")
	}
	var (
		err        error
		urlString  string
		credential azblob.Credential
	)
	if s.Config.AccountKey != "" {
		credential, err = azblob.NewSharedKeyCredential(s.Config.AccountName, s.Config.AccountKey)
		if err != nil {
			return err
		}
		urlString = fmt.Sprintf("https://%s.blob.%s", s.Config.AccountName, s.Config.EndpointSuffix)
	} else if s.Config.SharedAccessSignature != "" {
		credential = azblob.NewAnonymousCredential()
		urlString = fmt.Sprintf("https://%s.blob.%s?%s", s.Config.AccountName, s.Config.EndpointSuffix, s.Config.SharedAccessSignature)
	} else if s.Config.UseManagedIdentity {
		azureEnv, err := azure.EnvironmentFromName("AZUREPUBLICCLOUD")
		if err != nil {
			return err
		}
		var spToken *adal.ServicePrincipalToken
		msiEndpoint, _ := adal.GetMSIVMEndpoint()
		spToken, err = adal.NewServicePrincipalTokenFromMSI(msiEndpoint, azureEnv.ResourceIdentifiers.Storage)
		if err != nil {
			return err
		}
		tokenRefresher := func(tokenCred azblob.TokenCredential) time.Duration {
			// Refreshing Azure auth token
			err := spToken.Refresh()
			if err != nil {
				// Error refreshing Azure auth token, retry after 1 min.
				return 1 * time.Minute
			}
			token := spToken.Token()
			tokenCred.SetToken(token.AccessToken)
			// Return the expiry time of <response> minus 30 min. so we can retry
			// OAuth token is valid for 1hr.
			// ManagedIdentity one for 24 hrs.
			exp := token.Expires().Sub(time.Now().Add(30 * time.Minute))
			// Received a new Azure auth token, valid for exp
			return exp
		}

		credential = azblob.NewTokenCredential("", tokenRefresher)
		urlString = fmt.Sprintf("https://%s.blob.%s", s.Config.AccountName, s.Config.EndpointSuffix)
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return err
	}
	// don't pollute syslog with expected 404's and other garbage logs
	pipeline.SetForceLogEnabled(false)

	s.Container = azblob.NewServiceURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{})).NewContainerURL(s.Config.Container)
	_, err = s.Container.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil && !isContainerAlreadyExists(err) {
		return err
	}
	test_name := make([]byte, 16)
	if _, err := rand.Read(test_name); err != nil {
		return errors.Wrapf(err, "azblob: failed to generate test blob name")
	}
	test_blob := s.Container.NewBlockBlobURL(base64.URLEncoding.EncodeToString(test_name))
	if _, err = test_blob.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{}); err != nil {
		if se, ok := err.(azblob.StorageError); !ok || se.ServiceCode() != azblob.ServiceCodeBlobNotFound {
			return errors.Wrapf(err, "azblob: failed to access container %s", s.Config.Container)
		}
	}

	if s.Config.SSEKey != "" {
		key, err := base64.StdEncoding.DecodeString(s.Config.SSEKey)
		if err != nil {
			return errors.Wrapf(err, "malformed SSE key, must be base64-encoded 256-bit key")
		}
		if len(key) != 32 {
			return fmt.Errorf("malformed SSE key, must be base64-encoded 256-bit key")
		}
		b64key := s.Config.SSEKey
		shakey := sha256.Sum256(key)
		b64sha := base64.StdEncoding.EncodeToString(shakey[:])
		s.CPK = azblob.NewClientProvidedKeyOptions(&b64key, &b64sha, nil)
	}
	return nil
}

func (s *AzureBlob) Kind() string {
	return "azblob"
}

func (s *AzureBlob) GetFileReader(key string) (io.ReadCloser, error) {
	ctx := context.Background()
	blob := s.Container.NewBlockBlobURL(path.Join(s.Config.Path, key))
	r, err := blob.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, s.CPK)
	if err != nil {
		return nil, err
	}
	return r.Body(azblob.RetryReaderOptions{}), nil
}

func (s *AzureBlob) GetFileReaderWithLocalPath(key, _ string) (io.ReadCloser, error) {
	return s.GetFileReader(key)
}

func (s *AzureBlob) PutFile(key string, r io.ReadCloser) error {
	ctx := context.Background()
	blob := s.Container.NewBlockBlobURL(path.Join(s.Config.Path, key))
	bufferSize := s.Config.BufferSize // Configure the size of the rotating buffers that are used when uploading
	maxBuffers := s.Config.MaxBuffers // Configure the number of rotating buffers that are used when uploading
	_, err := x.UploadStreamToBlockBlob(ctx, r, blob, azblob.UploadStreamToBlockBlobOptions{BufferSize: bufferSize, MaxBuffers: maxBuffers}, s.CPK)
	return err
}

func (s *AzureBlob) DeleteFile(key string) error {
	ctx := context.Background()
	blob := s.Container.NewBlockBlobURL(path.Join(s.Config.Path, key))
	_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}

func (s *AzureBlob) StatFile(key string) (RemoteFile, error) {
	ctx := context.Background()
	blob := s.Container.NewBlockBlobURL(path.Join(s.Config.Path, key))
	r, err := blob.GetProperties(ctx, azblob.BlobAccessConditions{}, s.CPK)
	if err != nil {
		if se, ok := err.(azblob.StorageError); !ok || se.ServiceCode() != azblob.ServiceCodeBlobNotFound {
			return nil, err
		}
		return nil, ErrNotFound
	}
	return &azureBlobFile{
		name:         key,
		size:         r.ContentLength(),
		lastModified: r.LastModified(),
	}, nil
}

func (s *AzureBlob) Walk(azPath string, recursive bool, process func(r RemoteFile) error) error {
	ctx := context.Background()
	prefix := path.Join(s.Config.Path, azPath)
	if prefix == "" || prefix == "/" {
		prefix = ""
	} else {
		prefix += "/"
	}
	opt := azblob.ListBlobsSegmentOptions{
		Prefix: prefix,
	}
	mrk := azblob.Marker{}
	delimiter := ""
	if !recursive {
		delimiter = "/"
	}
	for mrk.NotDone() {
		// r, err := s.Container.ListBlobsFlatSegment(ctx, mrk, opt)
		r, err := s.Container.ListBlobsHierarchySegment(ctx, mrk, delimiter, opt)
		if err != nil {
			return err
		}
		for _, p := range r.Segment.BlobPrefixes {
			if err := process(&azureBlobFile{
				name: strings.TrimPrefix(p.Name, prefix),
			}); err != nil {
				return err
			}
		}
		for _, blob := range r.Segment.BlobItems {
			var size int64
			if blob.Properties.ContentLength != nil {
				size = *blob.Properties.ContentLength
			} else {
				size = 0
			}
			if err := process(&azureBlobFile{
				name:         strings.TrimPrefix(blob.Name, prefix),
				size:         size,
				lastModified: blob.Properties.LastModified,
			}); err != nil {
				return err
			}
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

func isContainerAlreadyExists(err error) bool {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				return true
			}
		}
	}
	return false
}
