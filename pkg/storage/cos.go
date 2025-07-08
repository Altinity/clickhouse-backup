package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

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
		return err
	}
	b := &cos.BaseURL{BucketURL: u}
	timeout, err := time.ParseDuration(c.Config.Timeout)
	if err != nil {
		return err
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
	_, err = c.client.Bucket.Head(ctx)
	return err
}

func (c *COS) Close(ctx context.Context) error {
	return nil
}

func (c *COS) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return c.StatFileAbsolute(ctx, path.Join(c.Config.Path, key))
}

func (c *COS) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	// @todo - COS Stat file max size is 5Gb
	resp, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		var cosErr *cos.ErrorResponse
		ok := errors.As(err, &cosErr)
		if ok && cosErr.Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	modifiedTime, _ := parseTime(resp.Response.Header.Get("Date"))
	return &cosFile{
		size:         resp.Response.ContentLength,
		name:         resp.Request.URL.Path,
		lastModified: modifiedTime,
	}, nil
}

func (c *COS) DeleteFile(ctx context.Context, key string) error {
	_, err := c.client.Object.Delete(ctx, path.Join(c.Config.Path, key))
	return err
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
		return err
	}
	// When recursive is false, only process all the backups in the CommonPrefixes part.
	for _, dir := range res.CommonPrefixes {
		if err := process(ctx, &cosFile{
			name: strings.TrimPrefix(dir, prefix),
		}); err != nil {
			return err
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
				return err
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
		return nil, err
	}
	return resp.Body, nil
}

func (c *COS) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	/* unfortunately, multipart download requires allocating additional disk space
	and doesn't allow us to decompress data directly from stream */
	if c.Config.AllowMultipartDownload {
		writer, err := os.CreateTemp(localPath, strings.ReplaceAll(key, "/", "_"))
		if err != nil {
			return nil, err
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
			return nil, err
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
		_, err := c.client.Object.Put(ctx, key, r, nil)
		return err
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
		return fmt.Errorf("COS->InitiateMultipartUpload return error: %w", initErr)
	}

	uploadID := resInit.UploadID

	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			log.Warn().Msgf("COS can't close reader for %s, error: %v", key, closeErr)
		}
	}()

	var parts []cos.Object // To store parts for completion

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
					return err
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
				return readErr
			}
			partsCh <- partUpload{PartNumber: partNum, Data: buf}
			partNum++
		}
		return nil
	})

	// Collector goroutine: collect uploaded parts
	var (
		uploadedParts []cos.Object
		mu            sync.Mutex
	)
	uploadPartErrGroup.Go(func() error {
		for up := range uploadedCh {
			mu.Lock()
			uploadedParts = append(uploadedParts, cos.Object{
				PartNumber: up.PartNumber,
				ETag:       up.ETag,
			})
			mu.Unlock()
		}
		return nil
	})
	if wgWaitErr := uploadPartErrGroup.Wait(); wgWaitErr != nil {
		if _, abortErr := c.client.Object.AbortMultipartUpload(ctx, key, uploadID); abortErr != nil {
			return fmt.Errorf("COS Multipart upload %s abort error: %v, original error was: %v", key, abortErr, wgWaitErr)
		}
		return fmt.Errorf("COS Multipart upload %s error: %v", key, wgWaitErr)
	}

	close(uploadedCh)
	sort.Slice(uploadedParts, func(i, j int) bool { return uploadedParts[i].PartNumber < uploadedParts[j].PartNumber })

	// Step 3: Complete Multipart Upload
	_, _, completeErr := c.client.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{Parts: parts})
	if completeErr != nil {
		return fmt.Errorf("COS Multipart upload complete %s error: %v", key, completeErr)
	}

	return nil
}

func (c *COS) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, fmt.Errorf("CopyObject not imlemented for %s", c.Kind())
}

func (c *COS) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	_, err := c.client.Object.Delete(ctx, path.Join(c.Config.ObjectDiskPath, key))
	return err
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
