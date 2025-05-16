package storage

import (
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
	client      *cos.Client
	Config      *config.COSConfig
	Concurrency int
	BufferSize  int
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
	// file max size is 5Gb
	resp, err := c.client.Object.Get(ctx, path.Join(c.Config.Path, key), nil)
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
		partSize = AdjustS3PartSize(partSize, 5*1024*1024, 5*1024*1024*1024)

		// Create a context with timeout
		timeout, err := time.ParseDuration(c.Config.Timeout)
		if err != nil {
			return nil, err
		}
		downloadCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Prepare download options
		opt := &cos.MultiDownloadOptions{
			ThreadPoolSize: c.Concurrency,
			PartSize:       partSize,
		}

		// Download the object
		_, err = c.client.Object.Download(
			downloadCtx,
			path.Join(c.Config.Path, key),
			writer.Name(),
			opt,
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

func (c *COS) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	// For small files or when size is unknown, use simple Put
	if localSize <= 0 || localSize < 5*1024*1024 {
		_, err := c.client.Object.Put(ctx, key, r, nil)
		return err
	}

	// For larger files, use multipart upload
	// Calculate part size based on file size and max parts count
	partSize := localSize / c.Config.MaxPartsCount
	if localSize%c.Config.MaxPartsCount > 0 {
		partSize += max(1, (localSize%c.Config.MaxPartsCount)/c.Config.MaxPartsCount)
	}
	partSize = AdjustS3PartSize(partSize, 5*1024*1024, 5*1024*1024*1024)

	// Create a context with timeout
	timeout, err := time.ParseDuration(c.Config.Timeout)
	if err != nil {
		return err
	}
	uploadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Prepare upload options
	opt := &cos.MultiUploadOptions{
		ThreadPoolSize: c.Concurrency,
		PartSize:       int(partSize),
	}

	// Create a temporary file to buffer the input
	tmpFile, err := os.CreateTemp("", "cos-upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy the input to the temporary file
	if _, err = io.Copy(tmpFile, r); err != nil {
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		return err
	}
	if _, err = tmpFile.Seek(0, 0); err != nil {
		return err
	}

	// Upload the file
	_, _, err = c.client.Object.MultiUpload(
		uploadCtx,
		key,
		tmpFile.Name(),
		opt,
	)
	return err
}

func (c *COS) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	dstKey = path.Join(c.Config.ObjectDiskPath, dstKey)
	log.Debug().Msgf("COS->CopyObject %s/%s -> %s/%s", srcBucket, srcKey, c.Config.RowURL, dstKey)

	// Create a context with timeout
	timeout, err := time.ParseDuration(c.Config.Timeout)
	if err != nil {
		return 0, err
	}
	copyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// For small files, use simple Copy
	if srcSize < 5*1024*1024*1024 {
		// Prepare copy source
		sourceURL := fmt.Sprintf("%s/%s", srcBucket, srcKey)
		_, _, err := c.client.Object.Copy(
			copyCtx,
			dstKey,
			sourceURL,
			nil,
		)
		if err != nil {
			return 0, fmt.Errorf("COS->CopyObject %s/%s -> %s/%s return error: %v", srcBucket, srcKey, c.Config.RowURL, dstKey, err)
		}
		return srcSize, nil
	}

	// For larger files, use multipart copy
	// Initialize multipart upload
	v, _, err := c.client.Object.InitiateMultipartUpload(copyCtx, dstKey, nil)
	if err != nil {
		return 0, fmt.Errorf("COS->CopyObject %s/%s -> %s/%s, InitiateMultipartUpload return error: %v", srcBucket, srcKey, c.Config.RowURL, dstKey, err)
	}
	uploadID := v.UploadID

	// Calculate part size based on source size and max parts count
	partSize := srcSize / c.Config.MaxPartsCount
	if srcSize%c.Config.MaxPartsCount > 0 {
		partSize += max(1, (srcSize%c.Config.MaxPartsCount)/c.Config.MaxPartsCount)
	}
	partSize = AdjustS3PartSize(partSize, 5*1024*1024, 5*1024*1024*1024)

	// Calculate the number of parts
	numParts := (srcSize + partSize - 1) / partSize

	copyPartErrGroup, ctx := errgroup.WithContext(ctx)
	copyPartErrGroup.SetLimit(c.Config.Concurrency * c.Config.Concurrency)

	var mu sync.Mutex
	var parts []cos.Object

	// Copy each part of the object
	for partNumber := int64(1); partNumber <= numParts; partNumber++ {
		// Calculate the byte range for the part
		start := (partNumber - 1) * partSize
		end := partNumber * partSize
		if end > srcSize {
			end = srcSize
		}
		currentPartNumber := int(partNumber)

		copyPartErrGroup.Go(func() error {
			// Prepare copy source with range
			sourceURL := fmt.Sprintf("%s/%s", srcBucket, srcKey)
			sourceRange := fmt.Sprintf("bytes=%d-%d", start, end-1)

			// Copy the part
			resp, _, err := c.client.Object.CopyPart(
				ctx,
				dstKey,
				sourceURL,
				uploadID,
				currentPartNumber,
				&cos.ObjectCopyPartOptions{
					Range: sourceRange,
				},
			)
			if err != nil {
				return fmt.Errorf("COS->CopyObject %s/%s -> %s/%s, CopyPart start=%d, end=%d return error: %v", srcBucket, srcKey, c.Config.RowURL, dstKey, start, end-1, err)
			}

			mu.Lock()
			parts = append(parts, cos.Object{
				PartNumber: currentPartNumber,
				ETag:       resp.ETag,
			})
			mu.Unlock()
			return nil
		})
	}

	if wgWaitErr := copyPartErrGroup.Wait(); wgWaitErr != nil {
		// Abort the multipart upload if there was an error
		_, abortErr := c.client.Object.AbortMultipartUpload(context.Background(), dstKey, uploadID)
		if abortErr != nil {
			return 0, fmt.Errorf("aborting CopyObject multipart upload: %v, original error was: %v", abortErr, wgWaitErr)
		}
		return 0, fmt.Errorf("one of CopyObject/Multipart go-routine return error: %v", wgWaitErr)
	}

	// Sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Complete the multipart upload
	_, _, err = c.client.Object.CompleteMultipartUpload(
		context.Background(),
		dstKey,
		uploadID,
		&cos.CompleteMultipartUploadOptions{
			Parts: parts,
		},
	)
	if err != nil {
		return 0, fmt.Errorf("complete CopyObject multipart upload: %v", err)
	}

	return srcSize, nil
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
