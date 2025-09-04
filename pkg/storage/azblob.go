package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"

	x "github.com/Altinity/clickhouse-backup/v2/pkg/storage/azblob"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// AzureBlob - presents methods for manipulate data on Azure
type AzureBlob struct {
	Container azblob.ContainerURL
	Pipeline  pipeline.Pipeline
	CPK       azblob.ClientProvidedKeyOptions
	Config    *config.AzureBlobConfig
}

func (a *AzureBlob) logf(msg string, args ...interface{}) {
	if a.Config.Debug {
		log.Info().Msgf(msg, args...)
	} else {
		log.Debug().Msgf(msg, args...)
	}
}

func (a *AzureBlob) log(level pipeline.LogLevel, msg string) {
	if a.Config.Debug {
		switch level {
		case pipeline.LogNone:
			log.Debug().Msg(msg)
		case pipeline.LogFatal:
			log.Fatal().Msg(msg)
		case pipeline.LogPanic:
			log.Fatal().Msg(msg)
		case pipeline.LogError:
			log.Error().Msg(msg)
		case pipeline.LogWarning:
			log.Warn().Msg(msg)
		case pipeline.LogInfo:
			log.Info().Msg(msg)
		case pipeline.LogDebug:
			log.Debug().Msg(msg)
		}
	}
}

func (a *AzureBlob) Kind() string {
	return "azblob"
}

// Connect - connect to Azure
func (a *AzureBlob) Connect(ctx context.Context) error {
	if a.Config.EndpointSuffix == "" {
		return fmt.Errorf("azblob endpoint suffix not set")
	}
	if a.Config.Container == "" {
		return fmt.Errorf("azblob container name not set")
	}
	if a.Config.AccountName == "" {
		return fmt.Errorf("azblob account name not set")
	}
	if a.Config.AccountKey == "" && a.Config.SharedAccessSignature == "" && !a.Config.UseManagedIdentity {
		return fmt.Errorf("azblob account key or SAS or use_managed_identity must be set")
	}
	var (
		err        error
		urlString  string
		credential azblob.Credential
	)
	timeout, err := time.ParseDuration(a.Config.Timeout)
	if err != nil {
		return err
	}
	if a.Config.AccountKey != "" {
		credential, err = azblob.NewSharedKeyCredential(a.Config.AccountName, a.Config.AccountKey)
		if err != nil {
			return err
		}
		urlString = fmt.Sprintf("%s://%s.blob.%s", a.Config.EndpointSchema, a.Config.AccountName, a.Config.EndpointSuffix)
	} else if a.Config.SharedAccessSignature != "" {
		credential = azblob.NewAnonymousCredential()
		urlString = fmt.Sprintf("%s://%s.blob.%s?%s", a.Config.EndpointSchema, a.Config.AccountName, a.Config.EndpointSuffix, a.Config.SharedAccessSignature)
	} else if a.Config.UseManagedIdentity {
		tokenRefresher := func(tokenCred azblob.TokenCredential) time.Duration {
			cred, err := azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				// Error creating Azure credential, retry after 1 min.
				return 1 * time.Minute
			}
			tokenRequestOptions := policy.TokenRequestOptions{
				Scopes: []string{"https://storage.azure.com/.default"},
			}
			// Get Azure auth token
			token, err := cred.GetToken(ctx, tokenRequestOptions)
			if err != nil {
				// Error refreshing Azure auth token, retry after 1 min.
				return 1 * time.Minute
			}
			tokenCred.SetToken(token.Token)
			// Return the expiry time of <response> minus 30 min. so we can retry
			// OAuth token is valid for 1hr.
			// ManagedIdentity one for 24 hrs.
			exp := token.ExpiresOn.Sub(time.Now()) - 30*time.Minute
			// Received a new Azure auth token, valid for exp
			return exp
		}

		credential = azblob.NewTokenCredential("", tokenRefresher)
		urlString = fmt.Sprintf("%s://%s.blob.%s", a.Config.EndpointSchema, a.Config.AccountName, a.Config.EndpointSuffix)
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return err
	}
	// don't pollute syslog with expected 404'a and other garbage logs
	pipeline.SetForceLogEnabled(false)

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		options := azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
				TryTimeout: timeout,
			},
		}
		if a.Config.Debug {
			options.Log = pipeline.LogOptions{
				Log: a.log,
				ShouldLog: func(level pipeline.LogLevel) bool {
					return true
				},
			}
		}
		a.Pipeline = azblob.NewPipeline(credential, options)
		a.Container = azblob.NewServiceURL(*u, a.Pipeline).NewContainerURL(a.Config.Container)
		if !a.Config.AssumeContainerExists {
			_, err = a.Container.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
			if err != nil && !isContainerAlreadyExists(err) {
				return err
			}
		}
		if a.Config.SSEKey != "" {
			key, err := base64.StdEncoding.DecodeString(a.Config.SSEKey)
			if err != nil {
				return errors.Wrapf(err, "malformed SSE key, must be base64-encoded 256-bit key")
			}
			if len(key) != 32 {
				return fmt.Errorf("malformed SSE key, must be base64-encoded 256-bit key")
			}
			b64key := a.Config.SSEKey
			shakey := sha256.Sum256(key)
			b64sha := base64.StdEncoding.EncodeToString(shakey[:])
			a.CPK = azblob.NewClientProvidedKeyOptions(&b64key, &b64sha, nil)
		}
	}
	return nil
}

func (a *AzureBlob) Close(ctx context.Context) error {
	return nil
}

func (a *AzureBlob) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return a.GetFileReaderAbsolute(ctx, path.Join(a.Config.Path, key))
}

func (a *AzureBlob) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	a.logf("AZBLOB->GetFileReaderAbsolute %s", key)
	blob := a.Container.NewBlockBlobURL(key)
	r, err := blob.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, a.CPK)
	if err != nil {
		return nil, err
	}
	return r.Body(azblob.RetryReaderOptions{}), nil
}

func (a *AzureBlob) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return a.GetFileReader(ctx, key)
}

func (a *AzureBlob) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return a.PutFileAbsolute(ctx, path.Join(a.Config.Path, key), r, localSize)
}

func (a *AzureBlob) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	a.logf("AZBLOB->PutFileAbsolute %s", key)
	blob := a.Container.NewBlockBlobURL(key)
	// https://github.com/Altinity/clickhouse-backup/issues/317

	// Use adaptive buffer sizing based on file size and configured max buffers
	bufferSize := config.CalculateOptimalBufferSize(localSize, a.Config.MaxBuffers)

	// Fallback to part-based calculation if needed
	if bufferSize < 2*1024*1024 {
		bufferSize = int(localSize / a.Config.MaxPartsCount)
		if localSize%a.Config.MaxPartsCount > 0 {
			bufferSize += max(1, int((localSize%a.Config.MaxPartsCount)/a.Config.MaxPartsCount))
		}
	}
	bufferSize = int(AdjustValueByRange(int64(bufferSize), 2*1024*1024, 10*1024*1024))

	_, err := x.UploadStreamToBlockBlob(ctx, r, blob, azblob.UploadStreamToBlockBlobOptions{BufferSize: bufferSize, MaxBuffers: a.Config.MaxBuffers}, a.CPK)
	return err
}

func (a *AzureBlob) DeleteFile(ctx context.Context, key string) error {
	a.logf("AZBLOB->DeleteFile %s", key)
	blob := a.Container.NewBlockBlobURL(path.Join(a.Config.Path, key))
	_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}

func (a *AzureBlob) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	a.logf("AZBLOB->DeleteFileFromObjectDiskBackup %s", key)
	blob := a.Container.NewBlockBlobURL(path.Join(a.Config.ObjectDiskPath, key))
	_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}

func (a *AzureBlob) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return a.StatFileAbsolute(ctx, path.Join(a.Config.Path, key))
}

func (a *AzureBlob) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	a.logf("AZBLOB->StatFileAbsolute %s", key)
	blob := a.Container.NewBlockBlobURL(key)
	r, err := blob.GetProperties(ctx, azblob.BlobAccessConditions{}, a.CPK)
	if err != nil {
		var se azblob.StorageError
		if !errors.As(err, &se) || se.ServiceCode() != azblob.ServiceCodeBlobNotFound {
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

func (a *AzureBlob) Walk(ctx context.Context, azPath string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	prefix := path.Join(a.Config.Path, azPath)
	return a.WalkAbsolute(ctx, prefix, recursive, process)
}

func (a *AzureBlob) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	a.logf("AZBLOB->WalkAbsolute %s", prefix)
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
		if !recursive {
			r, err := a.Container.ListBlobsHierarchySegment(ctx, mrk, delimiter, opt)
			if err != nil {
				return err
			}
			for _, p := range r.Segment.BlobPrefixes {
				if err := process(ctx, &azureBlobFile{
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
				if err := process(ctx, &azureBlobFile{
					name:         strings.TrimPrefix(blob.Name, prefix),
					size:         size,
					lastModified: blob.Properties.LastModified,
				}); err != nil {
					return err
				}
			}
			mrk = r.NextMarker
		} else {
			r, err := a.Container.ListBlobsFlatSegment(ctx, mrk, opt)
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
				if err := process(ctx, &azureBlobFile{
					name:         strings.TrimPrefix(blob.Name, prefix),
					size:         size,
					lastModified: blob.Properties.LastModified,
				}); err != nil {
					return err
				}
			}
			mrk = r.NextMarker
		}
	}
	return nil
}

func (a *AzureBlob) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	dstKey = path.Join(a.Config.ObjectDiskPath, dstKey)
	a.logf("AZBLOB->CopyObject %s/%s -> %s/%s", srcBucket, srcKey, a.Config.Container, dstKey)
	//ugly hack ;(
	endpoint := a.Config.EndpointSuffix
	if strings.HasSuffix(endpoint, "core.windows.net") || !strings.HasPrefix(endpoint, "blob.") {
		endpoint = "blob." + endpoint
	}
	srcURLString := fmt.Sprintf("%s://%s.%s/%s/%s", a.Config.EndpointSchema, a.Config.AccountName, endpoint, strings.Trim(srcBucket, "/"), strings.Trim(srcKey, "/"))
	srcURL, err := url.Parse(srcURLString)
	if err != nil {
		return 0, err
	}

	sourceBlobURL := azblob.NewBlobURL(*srcURL, a.Pipeline)
	destinationBlobURL := a.Container.NewBlobURL(dstKey)

	startCopy, err := destinationBlobURL.StartCopyFromURL(ctx, sourceBlobURL.URL(), nil, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, nil)
	if err != nil {
		return 0, fmt.Errorf("azblob->CopyObject failed to start copy operation: %v", err)
	}
	copyStatus := startCopy.CopyStatus()
	copyStatusDesc := ""
	var size int64
	pollCount := 1
	sleepDuration := time.Millisecond * 50
	for copyStatus == azblob.CopyStatusPending {
		// @TODO think how to avoid polling GetProperties in AZBLOB during CopyObject
		time.Sleep(sleepDuration * time.Duration(pollCount*2))
		dstMeta, err := destinationBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			return 0, fmt.Errorf("azblob->CopyObject failed to destinationBlobURL.GetProperties operation: %v", err)
		}
		copyStatus = dstMeta.CopyStatus()
		copyStatusDesc = dstMeta.CopyStatusDescription()
		size = dstMeta.ContentLength()
		if pollCount < 8 {
			pollCount++
		}
	}
	if copyStatus == azblob.CopyStatusFailed {
		return 0, fmt.Errorf("azblob->CopyObject got CopyStatusFailed %s", copyStatusDesc)
	}
	return size, nil
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
		var storageErr azblob.StorageError
		if errors.As(err, &storageErr) { // This error is a Service-specific
			switch storageErr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				return true
			}
		}
	}
	return false
}
