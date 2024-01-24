package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/tencentyun/cos-go-sdk-v5/debug"
)

type COS struct {
	client *cos.Client
	Config *config.COSConfig
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
	resp, err := c.client.Object.Get(ctx, path.Join(c.Config.Path, key), nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *COS) GetFileReaderWithLocalPath(ctx context.Context, key, _ string) (io.ReadCloser, error) {
	return c.GetFileReader(ctx, key)
}

func (c *COS) PutFile(ctx context.Context, key string, r io.ReadCloser) error {
	_, err := c.client.Object.Put(ctx, path.Join(c.Config.Path, key), r, nil)
	return err
}

func (c *COS) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, fmt.Errorf("CopyObject not imlemented for %s", c.Kind())
}

func (c *COS) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	return fmt.Errorf("DeleteFileFromObjectDiskBackup not imlemented for %s", c.Kind())
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
