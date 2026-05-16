package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/tencentyun/cos-go-sdk-v5/debug"
	"golang.org/x/sync/errgroup"
)

type COS struct {
	client     *cos.Client
	Config     *config.COSConfig
	BufferSize int
}

func (c *COS) Kind() string {
	return "COS"
}

// Connect - connect to cos
func (c *COS) Connect(ctx context.Context) error {
	u, err := url.Parse(c.Config.RowURL)
	if err != nil {
		return errors.WithMessage(err, "COS Connect url.Parse")
	}
	b := &cos.BaseURL{BucketURL: u}
	timeout, err := time.ParseDuration(c.Config.Timeout)
	if err != nil {
		return errors.WithMessage(err, "COS Connect ParseDuration")
	}
	c.client = cos.NewClient(b, &http.Client{
		Timeout: timeout,
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.Config.SecretID,
			SecretKey: c.Config.SecretKey,
			// request debug
			Transport: &debug.DebugRequestTransport{
				RequestHeader:  c.Config.Debug,
				RequestBody:    false,
				ResponseHeader: c.Config.Debug,
				ResponseBody:   false,
			},
		},
	})
	// check bucket exists
	if _, err = c.client.Bucket.Head(ctx); err != nil {
		return errors.WithMessage(err, "COS Connect Bucket.Head")
	}
	return nil
}

func (c *COS) Close(ctx context.Context) error {
	return nil
}

// cosIsNotFound reports whether err is a "NoSuchKey" response from Tencent COS.
func cosIsNotFound(err error) bool {
	var cosErr *cos.ErrorResponse
	return errors.As(err, &cosErr) && cosErr.Code == "NoSuchKey"
}

func (c *COS) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return c.StatFileAbsolute(ctx, path.Join(c.Config.Path, key))
}

func (c *COS) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	// @todo - COS Stat file max size is 5Gb
	resp, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		if cosIsNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, errors.WithMessage(err, "COS StatFileAbsolute Get")
	}
	modifiedTime, _ := parseTime(resp.Response.Header.Get("Date"))
	return &cosFile{
		size:         resp.Response.ContentLength,
		name:         resp.Request.URL.Path,
		lastModified: modifiedTime,
	}, nil
}

func (c *COS) DeleteFile(ctx context.Context, key string) error {
	if _, err := c.client.Object.Delete(ctx, path.Join(c.Config.Path, key)); err != nil {
		return errors.WithMessage(err, "COS DeleteFile")
	}
	return nil
}

func (c *COS) Walk(ctx context.Context, cosPath string, recursive bool, process func(context.Context, RemoteFile) error) error {
	// COS needs prefix ended with "/".
	prefix := path.Join(c.Config.Path, cosPath) + "/"
	return c.WalkAbsolute(ctx, prefix, recursive, process)
}

func (c *COS) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(context.Context, RemoteFile) error) error {

	delimiter := ""
	if !recursive {
		//
		// When delimiter is "/", we only process all backups in the CommonPrefixes field of response.
		// Then we get backupLists.
		//
		delimiter = "/"
	} else {
		//
		// When delimiter is an empty string, we  process the items under specified path.
		// Then we can Delete File object.
		//
		delimiter = ""
	}
	res, _, err := c.client.Bucket.Get(ctx, &cos.BucketGetOptions{
		Delimiter: delimiter,
		Prefix:    prefix,
	})
	if err != nil {
		return errors.WithMessage(err, "COS WalkAbsolute Bucket.Get")
	}
	// When recursive is false, only process all the backups in the CommonPrefixes part.
	for _, dir := range res.CommonPrefixes {
		if err := process(ctx, &cosFile{
			name: strings.TrimPrefix(dir, prefix),
		}); err != nil {
			return errors.WithMessage(err, "COS WalkAbsolute process prefix")
		}
	}
	if recursive {
		for _, v := range res.Contents {
			modifiedTime, _ := parseTime(v.LastModified)
			if err := process(ctx, &cosFile{
				name:         strings.TrimPrefix(v.Key, prefix),
				lastModified: modifiedTime,
				size:         v.Size,
			}); err != nil {
				return errors.WithMessage(err, "COS WalkAbsolute process content")
			}
		}
	}
	return nil
}

func (c *COS) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return c.GetFileReaderAbsolute(ctx, path.Join(c.Config.Path, key))
}

func (c *COS) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, errors.WithMessage(err, "COS GetFileReaderAbsolute Get")
	}
	return resp.Body, nil
}

func (c *COS) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	/* unfortunately, multipart download requires allocating additional disk space
	and doesn't allow us to decompress data directly from stream */
	if c.Config.AllowMultipartDownload {
		writer, err := os.CreateTemp(localPath, strings.ReplaceAll(key, "/", "_"))
		if err != nil {
			return nil, errors.WithMessage(err, "COS GetFileReaderWithLocalPath CreateTemp")
		}

		// Calculate part size based on remote size and max parts count
		partSize := remoteSize / c.Config.MaxPartsCount
		if remoteSize%c.Config.MaxPartsCount > 0 {
			partSize += max(1, (remoteSize%c.Config.MaxPartsCount)/c.Config.MaxPartsCount)
		}
		partSize = AdjustValueByRange(partSize, 5*1024*1024, 5*1024*1024*1024)

		// Prepare download options
		downloadOpts := &cos.MultiDownloadOptions{
			ThreadPoolSize: c.Config.Concurrency,
			PartSize:       partSize,
		}

		// Download the object
		_, err = c.client.Object.Download(
			ctx,
			path.Join(c.Config.Path, key),
			writer.Name(),
			downloadOpts,
		)
		if err != nil {
			return nil, errors.WithMessage(err, "COS GetFileReaderWithLocalPath Download")
		}

		// Reopen the file for reading
		return writer, nil
	} else {
		return c.GetFileReader(ctx, key)
	}
}

func (c *COS) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return c.PutFileAbsolute(ctx, path.Join(c.Config.Path, key), r, localSize)
}

type partUpload struct {
	PartNumber int
	Data       []byte
}

type uploadedPart struct {
	PartNumber int
	ETag       string
}

func (c *COS) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	// For small files or when size is unknown, use simple Put
	if localSize < 5*1024*1024 {
		if _, err := c.client.Object.Put(ctx, key, r, nil); err != nil {
			return errors.WithMessage(err, "COS PutFileAbsolute Put")
		}
		return nil
	}

	// For larger files, use multipart upload
	// Calculate part size based on file size and max parts count
	partSize := localSize / c.Config.MaxPartsCount
	if localSize%c.Config.MaxPartsCount > 0 {
		partSize += max(1, (localSize%c.Config.MaxPartsCount)/c.Config.MaxPartsCount)
	}
	partSize = AdjustValueByRange(partSize, 5*1024*1024, 64*1024*1024)

	// @TODO think about SSE and other options
	resInit, _, initErr := c.client.Object.InitiateMultipartUpload(ctx, key, &cos.InitiateMultipartUploadOptions{
		ACLHeaderOptions: &cos.ACLHeaderOptions{XCosACL: "private"},
	})
	if initErr != nil {
		return errors.Wrap(initErr, "COS->InitiateMultipartUpload return error")
	}

	uploadID := resInit.UploadID

	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			log.Warn().Msgf("COS can't close reader for %s, error: %v", key, closeErr)
		}
	}()

	partsCh := make(chan partUpload)
	uploadedCh := make(chan uploadedPart)
	uploadPartErrGroup, ctx := errgroup.WithContext(ctx)

	// Start worker goroutines
	for i := 0; i < c.Config.Concurrency; i++ {
		uploadPartErrGroup.Go(func() error {
			for part := range partsCh {
				reader := bytes.NewReader(part.Data)
				params := cos.ObjectUploadPartOptions{}
				resp, err := c.client.Object.UploadPart(ctx, key, uploadID, part.PartNumber, reader, &params)
				if err != nil {
					return errors.WithMessage(err, "COS PutFileAbsolute UploadPart")
				}
				uploadedCh <- uploadedPart{
					PartNumber: part.PartNumber,
					ETag:       resp.Header.Get("ETag"),
				}
			}
			return nil
		})
	}

	// Reader goroutine: read and dispatch parts
	uploadPartErrGroup.Go(func() error {
		defer close(partsCh)
		partNum := 1
		for {
			buf := make([]byte, partSize)
			n, readErr := io.ReadFull(r, buf)
			if readErr == io.EOF || errors.Is(readErr, io.ErrUnexpectedEOF) {
				if n > 0 {
					partsCh <- partUpload{PartNumber: partNum, Data: buf[:n]}
				}
				break
			}
			if readErr != nil {
				return errors.WithMessage(readErr, "COS PutFileAbsolute ReadFull")
			}
			partsCh <- partUpload{PartNumber: partNum, Data: buf}
			partNum++
		}
		return nil
	})

	// Collector goroutine: collect uploaded parts
	// NOTE: This runs OUTSIDE the errgroup to avoid deadlock - it must wait for uploadedCh to close,
	// which happens after errgroup.Wait() returns
	var (
		uploadedParts []cos.Object
		mu            sync.Mutex
		collectorWg   sync.WaitGroup
	)
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for up := range uploadedCh {
			mu.Lock()
			uploadedParts = append(uploadedParts, cos.Object{
				PartNumber: up.PartNumber,
				ETag:       up.ETag,
			})
			mu.Unlock()
		}
	}()

	if wgWaitErr := uploadPartErrGroup.Wait(); wgWaitErr != nil {
		close(uploadedCh)
		collectorWg.Wait()
		if _, abortErr := c.client.Object.AbortMultipartUpload(ctx, key, uploadID); abortErr != nil {
			return errors.Wrapf(wgWaitErr, "COS Multipart upload %s abort error: %v, original error was", key, abortErr)
		}
		return errors.Wrapf(wgWaitErr, "COS Multipart upload %s error", key)
	}

	close(uploadedCh)
	collectorWg.Wait()
	sort.Slice(uploadedParts, func(i, j int) bool { return uploadedParts[i].PartNumber < uploadedParts[j].PartNumber })

	// Step 3: Complete Multipart Upload
	_, _, completeErr := c.client.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{Parts: uploadedParts})
	if completeErr != nil {
		return errors.Wrapf(completeErr, "COS Multipart upload complete %s error", key)
	}

	return nil
}

// PutFileAbsoluteIfAbsent atomically creates the object at key only if it
// doesn't already exist, using Tencent COS's If-None-Match: "*" header.
//
// The Tencent Go SDK (github.com/tencentyun/cos-go-sdk-v5 v0.7.73) does not
// expose a typed If-None-Match field on ObjectPutHeaderOptions, but it does
// provide the cos.XOptionalKey / cos.XOptionalValue context mechanism which
// injects arbitrary headers into any SDK call. We use that to send
// "If-None-Match: *" on the PUT request. COS returns HTTP 412 when the object
// already exists; this maps to (false, nil).
func (c *COS) PutFileAbsoluteIfAbsent(ctx context.Context, key string, r io.ReadCloser, localSize int64) (bool, error) {
	ifNoneMatch := make(http.Header)
	ifNoneMatch.Set("If-None-Match", "*")
	ctx = context.WithValue(ctx, cos.XOptionalKey, &cos.XOptionalValue{Header: &ifNoneMatch})

	if _, err := c.client.Object.Put(ctx, key, r, nil); err != nil {
		if isCOSPreconditionFailed(err) {
			return false, nil
		}
		return false, errors.WithMessage(err, "COS PutFileAbsoluteIfAbsent Put")
	}
	return true, nil
}

// PutFileIfAbsent is the path-prefixed variant of PutFileAbsoluteIfAbsent.
// It prepends c.Config.Path to key, matching PutFile semantics.
func (c *COS) PutFileIfAbsent(ctx context.Context, key string, r io.ReadCloser, localSize int64) (bool, error) {
	return c.PutFileAbsoluteIfAbsent(ctx, path.Join(c.Config.Path, key), r, localSize)
}

// isCOSPreconditionFailed returns true when the error is a Tencent COS HTTP 412
// (PreconditionFailed), which is what COS returns for If-None-Match: "*" when
// the object already exists.
func isCOSPreconditionFailed(err error) bool {
	var cosErr *cos.ErrorResponse
	if errors.As(err, &cosErr) && cosErr.Response != nil && cosErr.Response.StatusCode == http.StatusPreconditionFailed {
		return true
	}
	return false
}

func (c *COS) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, errors.Errorf("CopyObject not implemented for %s", c.Kind())
}

func (c *COS) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	if _, err := c.client.Object.Delete(ctx, path.Join(c.Config.ObjectDiskPath, key)); err != nil {
		return errors.WithMessage(err, "COS DeleteFileFromObjectDiskBackup")
	}
	return nil
}

// DeleteKeysBatch implements BatchDeleter interface for COS
// Uses concurrent deletion with configurable concurrency
func (c *COS) DeleteKeysBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Prepend path to all keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = path.Join(c.Config.Path, key)
	}
	return c.deleteKeysConcurrent(ctx, fullKeys)
}

// DeleteKeysFromObjectDiskBackupBatch implements BatchDeleter interface for COS
func (c *COS) DeleteKeysFromObjectDiskBackupBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Prepend object disk path to all keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = path.Join(c.Config.ObjectDiskPath, key)
	}
	return c.deleteKeysConcurrent(ctx, fullKeys)
}

// deleteKeysConcurrent performs concurrent deletion of keys
func (c *COS) deleteKeysConcurrent(ctx context.Context, keys []string) error {
	concurrency := c.Config.DeleteConcurrency

	log.Debug().Msgf("COS batch delete: deleting %d keys with concurrency %d", len(keys), concurrency)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var mu sync.Mutex
	var failures []KeyError
	deletedCount := 0

	for _, key := range keys {
		key := key // capture for goroutine
		g.Go(func() error {
			_, err := c.client.Object.Delete(ctx, key)
			if err != nil {
				// Check if it's a "not found" error - that's OK
				if cosIsNotFound(err) {
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
		return errors.Wrap(err, "COS concurrent delete failed")
	}

	if len(failures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("COS batch delete: %d keys deleted, %d failed", deletedCount, len(failures)),
			Failures: failures,
		}
	}

	log.Debug().Msgf("COS batch delete: successfully deleted %d keys", deletedCount)
	return nil
}

type cosFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *cosFile) Size() int64 {
	return f.size
}

func (f *cosFile) Name() string {
	return f.name
}

func (f *cosFile) LastModified() time.Time {
	return f.lastModified
}

func parseTime(text string) (t time.Time, err error) {
	timeFormats := []string{
		"Mon, 02 Jan 2006 15:04:05 GMT",
		time.RFC850,
		time.ANSIC,
		time.RFC3339,
	}

	for _, layout := range timeFormats {
		t, err = time.Parse(layout, text)
		if err == nil {
			return
		}
	}
	return
}
