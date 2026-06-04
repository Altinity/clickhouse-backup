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
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// AzureBlob - presents methods for manipulate data on Azure
type AzureBlob struct {
	Container *container.Client
	CPK       *blob.CPKInfo
	Config    *config.AzureBlobConfig
}

func (a *AzureBlob) logf(msg string, args ...interface{}) {
	if a.Config.Debug {
		log.Info().Msgf(msg, args...)
	} else {
		log.Debug().Msgf(msg, args...)
	}
}

func (a *AzureBlob) Kind() string {
	return "azblob"
}

// Connect - connect to Azure
func (a *AzureBlob) Connect(ctx context.Context) error {
	if a.Config.EndpointSuffix == "" {
		return errors.New("azblob endpoint suffix not set")
	}
	if a.Config.Container == "" {
		return errors.New("azblob container name not set")
	}
	if a.Config.AccountName == "" {
		return errors.New("azblob account name not set")
	}
	if a.Config.AccountKey == "" && a.Config.SharedAccessSignature == "" && !a.Config.UseManagedIdentity {
		return errors.New("azblob account key or SAS or use_managed_identity must be set")
	}
	var (
		err       error
		urlString string
		svc       *service.Client
	)
	timeout, err := time.ParseDuration(a.Config.Timeout)
	if err != nil {
		return errors.Wrap(err, "AzureBlob Connect ParseDuration")
	}

	clientOpts := &service.ClientOptions{}
	clientOpts.Retry = policy.RetryOptions{TryTimeout: timeout}
	// the modern SDK rejects credentials over plain HTTP by default; the legacy SDK did not.
	// allow it for http endpoints (e.g. Azurite) to preserve the previous behavior.
	if strings.EqualFold(a.Config.EndpointSchema, "http") {
		clientOpts.InsecureAllowCredentialWithHTTP = true
	}

	if a.Config.AccountKey != "" {
		credential, err := azblob.NewSharedKeyCredential(a.Config.AccountName, a.Config.AccountKey)
		if err != nil {
			return errors.Wrap(err, "AzureBlob Connect NewSharedKeyCredential")
		}
		urlString = fmt.Sprintf("%s://%s.blob.%s", a.Config.EndpointSchema, a.Config.AccountName, a.Config.EndpointSuffix)
		svc, err = service.NewClientWithSharedKeyCredential(urlString, credential, clientOpts)
		if err != nil {
			return errors.Wrap(err, "AzureBlob Connect NewClientWithSharedKeyCredential")
		}
	} else if a.Config.SharedAccessSignature != "" {
		urlString = fmt.Sprintf("%s://%s.blob.%s?%s", a.Config.EndpointSchema, a.Config.AccountName, a.Config.EndpointSuffix, a.Config.SharedAccessSignature)
		svc, err = service.NewClientWithNoCredential(urlString, clientOpts)
		if err != nil {
			return errors.Wrap(err, "AzureBlob Connect NewClientWithNoCredential")
		}
	} else if a.Config.UseManagedIdentity {
		// the modern SDK refreshes managed identity / OAuth tokens internally,
		// so the previous manual tokenRefresher closure is no longer needed.
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return errors.Wrap(err, "AzureBlob Connect NewDefaultAzureCredential")
		}
		urlString = fmt.Sprintf("%s://%s.blob.%s", a.Config.EndpointSchema, a.Config.AccountName, a.Config.EndpointSuffix)
		svc, err = service.NewClient(urlString, credential, clientOpts)
		if err != nil {
			return errors.Wrap(err, "AzureBlob Connect NewClient")
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if a.Config.Debug {
			// the modern SDK logs through sdk/azcore/log instead of a per-pipeline hook.
			// note: SetEvents/SetListener are process-global, but only the azblob backend uses them.
			azlog.SetEvents(azlog.EventRequest, azlog.EventResponse, azlog.EventResponseError, azlog.EventRetryPolicy, azblob.EventUpload)
			azlog.SetListener(func(cls azlog.Event, msg string) {
				log.Info().Msgf("[azblob][%s] %s", cls, msg)
			})
		}
		a.Container = svc.NewContainerClient(a.Config.Container)
		if !a.Config.AssumeContainerExists {
			_, err = a.Container.Create(ctx, nil)
			if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
				return errors.Wrap(err, "AzureBlob Connect Container.Create")
			}
		}
		if a.Config.SSEKey != "" {
			key, err := base64.StdEncoding.DecodeString(a.Config.SSEKey)
			if err != nil {
				return errors.Wrapf(err, "malformed SSE key, must be base64-encoded 256-bit key")
			}
			if len(key) != 32 {
				return errors.New("malformed SSE key, must be base64-encoded 256-bit key")
			}
			b64key := a.Config.SSEKey
			shakey := sha256.Sum256(key)
			b64sha := base64.StdEncoding.EncodeToString(shakey[:])
			a.CPK = &blob.CPKInfo{
				EncryptionKey:       &b64key,
				EncryptionKeySHA256: &b64sha,
				EncryptionAlgorithm: to.Ptr(blob.EncryptionAlgorithmTypeAES256),
			}
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
	b := a.Container.NewBlobClient(key)
	resp, err := b.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range:   blob.HTTPRange{Offset: 0, Count: blob.CountToEnd},
		CPKInfo: a.CPK,
	})
	if err != nil {
		return nil, errors.Wrap(err, "AzureBlob GetFileReaderAbsolute Download")
	}
	return resp.Body, nil
}

func (a *AzureBlob) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return a.GetFileReader(ctx, key)
}

func (a *AzureBlob) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return a.PutFileAbsolute(ctx, path.Join(a.Config.Path, key), r, localSize)
}

func (a *AzureBlob) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	a.logf("AZBLOB->PutFileAbsolute %s", key)
	b := a.Container.NewBlockBlobClient(key)
	// https://github.com/Altinity/clickhouse-backup/issues/317
	bufferSize := localSize / a.Config.MaxPartsCount
	if localSize%a.Config.MaxPartsCount > 0 {
		bufferSize += max(1, (localSize%a.Config.MaxPartsCount)/a.Config.MaxPartsCount)
	}
	bufferSize = AdjustValueByRange(bufferSize, 2*1024*1024, 10*1024*1024)

	if _, err := b.UploadStream(ctx, r, &blockblob.UploadStreamOptions{BlockSize: bufferSize, Concurrency: a.Config.MaxBuffers, CPKInfo: a.CPK}); err != nil {
		return errors.Wrap(err, "AzureBlob PutFileAbsolute UploadStream")
	}
	return nil
}

func (a *AzureBlob) DeleteFile(ctx context.Context, key string) error {
	a.logf("AZBLOB->DeleteFile %s", key)
	b := a.Container.NewBlobClient(path.Join(a.Config.Path, key))
	if _, err := b.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)}); err != nil {
		return errors.Wrap(err, "AzureBlob DeleteFile")
	}
	return nil
}

func (a *AzureBlob) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	a.logf("AZBLOB->DeleteFileFromObjectDiskBackup %s", key)
	b := a.Container.NewBlobClient(path.Join(a.Config.ObjectDiskPath, key))
	if _, err := b.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)}); err != nil {
		return errors.Wrap(err, "AzureBlob DeleteFileFromObjectDiskBackup")
	}
	return nil
}

// DeleteKeysBatch implements BatchDeleter interface for Azure Blob
// Uses concurrent deletion since Azure SDK doesn't expose batch delete API
func (a *AzureBlob) DeleteKeysBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return a.deleteKeysConcurrent(ctx, keys, a.Config.Path)
}

// DeleteKeysFromObjectDiskBackupBatch implements BatchDeleter interface for Azure Blob
func (a *AzureBlob) DeleteKeysFromObjectDiskBackupBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return a.deleteKeysConcurrent(ctx, keys, a.Config.ObjectDiskPath)
}

// deleteKeysConcurrent performs concurrent deletion of keys
func (a *AzureBlob) deleteKeysConcurrent(ctx context.Context, keys []string, basePath string) error {
	concurrency := a.Config.DeleteConcurrency

	a.logf("AZBLOB->deleteKeysConcurrent: deleting %d keys with concurrency %d", len(keys), concurrency)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var mu sync.Mutex
	var failures []KeyError
	deletedCount := 0

	for _, key := range keys {
		key := key // capture for goroutine
		g.Go(func() error {
			fullKey := path.Join(basePath, key)
			b := a.Container.NewBlobClient(fullKey)
			_, err := b.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)})
			if err != nil {
				// Check if it's a "not found" error - that's OK, key is already deleted
				if bloberror.HasCode(err, bloberror.BlobNotFound) {
					// Already deleted, count as success
					mu.Lock()
					deletedCount++
					mu.Unlock()
					return nil
				}
				mu.Lock()
				failures = append(failures, KeyError{Key: key, Err: err})
				mu.Unlock()
				return nil // Don't fail the entire group
			}
			mu.Lock()
			deletedCount++
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "Azure Blob concurrent delete failed")
	}

	if len(failures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("Azure Blob batch delete: %d keys deleted, %d failed", deletedCount, len(failures)),
			Failures: failures,
		}
	}

	log.Debug().Msgf("Azure Blob batch delete: successfully deleted %d keys", deletedCount)
	return nil
}

func (a *AzureBlob) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return a.StatFileAbsolute(ctx, path.Join(a.Config.Path, key))
}

func (a *AzureBlob) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	a.logf("AZBLOB->StatFileAbsolute %s", key)
	b := a.Container.NewBlobClient(key)
	r, err := b.GetProperties(ctx, &blob.GetPropertiesOptions{CPKInfo: a.CPK})
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, NewErrNotFound(key)
		}
		return nil, errors.Wrap(err, "AzureBlob StatFileAbsolute GetProperties")
	}
	var size int64
	if r.ContentLength != nil {
		size = *r.ContentLength
	}
	var lastModified time.Time
	if r.LastModified != nil {
		lastModified = *r.LastModified
	}
	return &azureBlobFile{
		name:         key,
		size:         size,
		lastModified: lastModified,
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
	if !recursive {
		pager := a.Container.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: &prefix})
		for pager.More() {
			r, err := pager.NextPage(ctx)
			if err != nil {
				return errors.Wrap(err, "AzureBlob WalkAbsolute ListBlobsHierarchySegment")
			}
			for _, p := range r.Segment.BlobPrefixes {
				if err := process(ctx, &azureBlobFile{
					name: strings.TrimPrefix(*p.Name, prefix),
				}); err != nil {
					return errors.Wrap(err, "AzureBlob WalkAbsolute process prefix")
				}
			}
			for _, b := range r.Segment.BlobItems {
				if err := process(ctx, blobItemToFile(b, prefix)); err != nil {
					return errors.Wrap(err, "AzureBlob WalkAbsolute process blob")
				}
			}
		}
	} else {
		pager := a.Container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
		for pager.More() {
			r, err := pager.NextPage(ctx)
			if err != nil {
				return errors.Wrap(err, "AzureBlob WalkAbsolute ListBlobsFlatSegment")
			}
			for _, b := range r.Segment.BlobItems {
				if err := process(ctx, blobItemToFile(b, prefix)); err != nil {
					return errors.Wrap(err, "AzureBlob WalkAbsolute process flat blob")
				}
			}
		}
	}
	return nil
}

func blobItemToFile(b *container.BlobItem, prefix string) *azureBlobFile {
	var size int64
	var lastModified time.Time
	if b.Properties != nil {
		if b.Properties.ContentLength != nil {
			size = *b.Properties.ContentLength
		}
		if b.Properties.LastModified != nil {
			lastModified = *b.Properties.LastModified
		}
	}
	return &azureBlobFile{
		name:         strings.TrimPrefix(*b.Name, prefix),
		size:         size,
		lastModified: lastModified,
	}
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
	if _, err := url.Parse(srcURLString); err != nil {
		return 0, errors.Wrap(err, "AzureBlob CopyObject url.Parse")
	}

	destinationBlob := a.Container.NewBlobClient(dstKey)

	startCopy, err := destinationBlob.StartCopyFromURL(ctx, srcURLString, nil)
	if err != nil {
		return 0, errors.Wrap(err, "azblob->CopyObject failed to start copy operation")
	}
	var copyStatus blob.CopyStatusType
	if startCopy.CopyStatus != nil {
		copyStatus = *startCopy.CopyStatus
	}
	copyStatusDesc := ""
	var size int64
	pollCount := 1
	sleepDuration := time.Millisecond * 50
	for copyStatus == blob.CopyStatusTypePending {
		// @TODO think how to avoid polling GetProperties in AZBLOB during CopyObject
		time.Sleep(sleepDuration * time.Duration(pollCount*2))
		dstMeta, err := destinationBlob.GetProperties(ctx, &blob.GetPropertiesOptions{})
		if err != nil {
			return 0, errors.Wrap(err, "azblob->CopyObject failed to destinationBlobURL.GetProperties operation")
		}
		if dstMeta.CopyStatus != nil {
			copyStatus = *dstMeta.CopyStatus
		}
		if dstMeta.CopyStatusDescription != nil {
			copyStatusDesc = *dstMeta.CopyStatusDescription
		}
		if dstMeta.ContentLength != nil {
			size = *dstMeta.ContentLength
		}
		if pollCount < 8 {
			pollCount++
		}
	}
	if copyStatus == blob.CopyStatusTypeFailed {
		return 0, errors.Errorf("azblob->CopyObject got CopyStatusFailed %s", copyStatusDesc)
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
