package storage

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	pool "github.com/jolestar/go-commons-pool/v2"
	"github.com/pkg/errors"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	googleHTTPTransport "google.golang.org/api/transport/http"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client        *storage.Client
	Config        *config.GCSConfig
	clientPool    *pool.ObjectPool
	encryptionKey []byte // Customer-Supplied Encryption Key (CSEK)
}

type debugGCSTransport struct {
	base http.RoundTripper
}

type clientObject struct {
	Client *storage.Client
}

func (w debugGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	logMsg := fmt.Sprintf(">>> [GCS_REQUEST] >>> %v %v\n", r.Method, r.URL.String())
	for h, values := range r.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Info().Msg(logMsg)

	resp, err := w.base.RoundTrip(r)
	if err != nil {
		log.Error().Msgf("GCS_ERROR: %v", err)
		return resp, err
	}
	logMsg = fmt.Sprintf("<<< [GCS_RESPONSE: %s] <<< %v %v\n", resp.Status, r.Method, r.URL.String())
	for h, values := range resp.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Info().Msg(logMsg)
	return resp, err
}

func (gcs *GCS) Kind() string {
	return "GCS"
}

type rewriteTransport struct {
	base http.RoundTripper
}

// forces requests to target varnish and use HTTP, required to get uploading
// via varnish working
func (r rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		req.URL.Scheme = "http"
	}
	return r.base.RoundTrip(req)
}

// Connect - connect to GCS
func (gcs *GCS) Connect(ctx context.Context) error {
	var err error
	endpoint := "https://storage.googleapis.com/storage/v1/"
	if gcs.Config.Endpoint != "" {
		endpoint = gcs.Config.Endpoint
	}

	// 1. Build the credential option
	var credOption option.ClientOption
	if gcs.Config.CredentialsJSON != "" {
		credOption = option.WithCredentialsJSON([]byte(gcs.Config.CredentialsJSON))
	} else if gcs.Config.CredentialsJSONEncoded != "" {
		d, _ := base64.StdEncoding.DecodeString(gcs.Config.CredentialsJSONEncoded)
		credOption = option.WithCredentialsJSON(d)
	} else if gcs.Config.CredentialsFile != "" {
		credOption = option.WithCredentialsFile(gcs.Config.CredentialsFile)
	} else if gcs.Config.SAEmail != "" {
		ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
			TargetPrincipal: gcs.Config.SAEmail,
			Scopes: []string{
				"https://www.googleapis.com/auth/cloud-platform",
				"https://www.googleapis.com/auth/devstorage.read_write",
			},
		})
		if err != nil {
			return errors.Wrap(err, "failed to create impersonation token source")
		}
		credOption = option.WithTokenSource(ts)
	} else if gcs.Config.SkipCredentials {
		credOption = option.WithoutAuthentication()
	}

	// 2. Build dial options for the HTTP client
	dialOptions := []option.ClientOption{option.WithTelemetryDisabled()}
	if gcs.Config.Endpoint != "" {
		dialOptions = append(dialOptions, option.WithEndpoint(endpoint))
	}
	if credOption != nil {
		dialOptions = append(dialOptions, credOption)
	}
	// Scopes are required when dialing manually, but conflict with NoAuth in newer library versions
	if !gcs.Config.SkipCredentials {
		dialOptions = append(dialOptions, option.WithScopes(storage.ScopeFullControl))
	}

	// 3. Create the HTTP client
	var httpClient *http.Client
	if gcs.Config.ForceHttp {
		customTransport := &http.Transport{
			WriteBufferSize: 128 * 1024,
			Proxy:           http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          1,
			MaxIdleConnsPerHost:   1,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		// must set ForceAttemptHTTP2 to false so that when a custom TLSClientConfig
		// is provided Golang does not setup HTTP/2 transport
		customTransport.ForceAttemptHTTP2 = false
		customTransport.TLSClientConfig = &tls.Config{
			NextProtos: []string{"http/1.1"},
		}
		customRoundTripper := &rewriteTransport{base: customTransport}
		// Use NewTransport to get an authenticated transport using dialOptions
		transport, err := googleHTTPTransport.NewTransport(ctx, customRoundTripper, dialOptions...)
		if err != nil {
			return errors.Wrap(err, "failed to create GCP transport")
		}
		httpClient = &http.Client{Transport: transport}
	} else {
		// Use NewClient to get an authenticated client using dialOptions
		httpClient, _, err = googleHTTPTransport.NewClient(ctx, dialOptions...)
		if err != nil {
			return errors.Wrap(err, "googleHTTPTransport.NewClient error")
		}
	}

	if gcs.Config.Debug {
		httpClient.Transport = debugGCSTransport{base: httpClient.Transport}
	}

	// 4. Build storage client options using our custom httpClient
	// Credentials and scopes are already handled by the httpClient.
	storageClientOptions := []option.ClientOption{
		option.WithHTTPClient(httpClient),
		option.WithTelemetryDisabled(),
	}
	if gcs.Config.Endpoint != "" {
		storageClientOptions = append(storageClientOptions, option.WithEndpoint(endpoint))
	}

	factory := pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			sClient, err := storage.NewClient(ctx, storageClientOptions...)
			if err != nil {
				return nil, err
			}
			return &clientObject{Client: sClient}, nil
		})
	gcs.clientPool = pool.NewObjectPoolWithDefaultConfig(ctx, factory)
	gcs.clientPool.Config.MaxTotal = gcs.Config.ClientPoolSize * 3
	gcs.client, err = storage.NewClient(ctx, storageClientOptions...)
	if err != nil {
		return err
	}

	// Validate and decode the encryption key if provided
	if gcs.Config.EncryptionKey != "" {
		key, err := base64.StdEncoding.DecodeString(gcs.Config.EncryptionKey)
		if err != nil {
			return errors.Wrap(err, "gcs: malformed encryption_key, must be base64-encoded 256-bit key")
		}
		if len(key) != 32 {
			return fmt.Errorf("gcs: malformed encryption_key, must be base64-encoded 256-bit key (got %d bytes)", len(key))
		}
		gcs.encryptionKey = key
		log.Info().Msg("GCS: Customer-Supplied Encryption Key (CSEK) configured")
	}

	return nil
}

func (gcs *GCS) Close(ctx context.Context) error {
	gcs.clientPool.Close(ctx)
	return gcs.client.Close()
}

// applyEncryption returns an ObjectHandle with encryption key applied if configured
func (gcs *GCS) applyEncryption(obj *storage.ObjectHandle) *storage.ObjectHandle {
	if gcs.encryptionKey != nil {
		return obj.Key(gcs.encryptionKey)
	}
	return obj
}

// isNotEncryptedError checks if the error is "ResourceNotEncryptedWithCustomerEncryptionKey"
func (gcs *GCS) isNotEncryptedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "ResourceNotEncryptedWithCustomerEncryptionKey")
}

func (gcs *GCS) Walk(ctx context.Context, gcsPath string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	rootPath := path.Join(gcs.Config.Path, gcsPath)
	return gcs.WalkAbsolute(ctx, rootPath, recursive, process)
}

func (gcs *GCS) WalkAbsolute(ctx context.Context, rootPath string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	prefix := rootPath + "/"
	if rootPath == "/" {
		prefix = ""
	}
	delimiter := ""
	if !recursive {
		delimiter = "/"
	}
	it := gcs.client.Bucket(gcs.Config.Bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: delimiter,
	})
	for {
		object, err := it.Next()
		switch {
		case err == nil:
			if object.Prefix != "" {
				if err := process(ctx, &gcsFile{
					name: strings.TrimPrefix(object.Prefix, rootPath),
				}); err != nil {
					return err
				}
				continue
			}
			if err := process(ctx, &gcsFile{
				size:         object.Size,
				lastModified: object.Updated,
				name:         strings.TrimPrefix(object.Name, rootPath),
			}); err != nil {
				return err
			}
		case errors.Is(err, iterator.Done):
			return nil
		default:
			return err
		}
	}
}

func (gcs *GCS) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return gcs.GetFileReaderAbsolute(ctx, path.Join(gcs.Config.Path, key))
}

func (gcs *GCS) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	pClientObj, err := gcs.clientPool.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("gcs.GetFileReader: gcs.clientPool.BorrowObject error: %+v", err)
		return nil, err
	}
	pClient := pClientObj.(*clientObject).Client
	obj := pClient.Bucket(gcs.Config.Bucket).Object(key)
	// Do NOT apply encryption for object_disks files - they are not encrypted
	// because ClickHouse needs to read them directly without encryption key
	isObjectDiskPath := gcs.Config.ObjectDiskPath != "" && strings.HasPrefix(key, gcs.Config.ObjectDiskPath)
	if !isObjectDiskPath {
		obj = gcs.applyEncryption(obj)
	}
	reader, err := obj.NewReader(ctx)
	if err != nil {
		// Close reader if it was partially initialized
		if reader != nil {
			_ = reader.Close()
			reader = nil
		}
		// If the object is not encrypted but we tried to read it with encryption key,
		// retry without encryption (for backward compatibility with old backups)
		if !isObjectDiskPath && gcs.isNotEncryptedError(err) && gcs.encryptionKey != nil {
			log.Warn().Msgf("gcs.GetFileReader: object %s not encrypted, retrying without encryption key", key)
			obj = pClient.Bucket(gcs.Config.Bucket).Object(key)
			reader, err = obj.NewReader(ctx)
		}
		if err != nil {
			// Close reader from retry if it failed
			if reader != nil {
				_ = reader.Close()
			}
			if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
				log.Warn().Msgf("gcs.GetFileReader: gcs.clientPool.InvalidateObject error: %v ", pErr)
			}
			return nil, err
		}
	}
	if pErr := gcs.clientPool.ReturnObject(ctx, pClientObj); pErr != nil {
		log.Warn().Msgf("gcs.GetFileReader: gcs.clientPool.ReturnObject error: %v ", pErr)
	}
	return reader, nil
}

func (gcs *GCS) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return gcs.GetFileReader(ctx, key)
}

func (gcs *GCS) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return gcs.PutFileAbsolute(ctx, path.Join(gcs.Config.Path, key), r, localSize)
}

func (gcs *GCS) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	pClientObj, err := gcs.clientPool.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("gcs.PutFile: gcs.clientPool.BorrowObject error: %+v", err)
		return err
	}
	pClient := pClientObj.(*clientObject).Client
	obj := pClient.Bucket(gcs.Config.Bucket).Object(key)
	// Do NOT apply encryption for object_disks files - they must be readable by ClickHouse
	// which doesn't have access to the encryption key
	if gcs.Config.ObjectDiskPath == "" || !strings.HasPrefix(key, gcs.Config.ObjectDiskPath) {
		obj = gcs.applyEncryption(obj)
	}
	// always retry transient errors to mitigate retry logic bugs.
	obj = obj.Retryer(storage.WithPolicy(storage.RetryAlways))
	writer := obj.NewWriter(ctx)
	writer.ChunkSize = gcs.Config.ChunkSize
	writer.StorageClass = gcs.Config.StorageClass
	writer.ChunkRetryDeadline = 60 * time.Minute
	if len(gcs.Config.ObjectLabels) > 0 {
		writer.Metadata = gcs.Config.ObjectLabels
	}
	defer func() {
		if err := gcs.clientPool.ReturnObject(ctx, pClientObj); err != nil {
			log.Warn().Msgf("gcs.PutFile: gcs.clientPool.ReturnObject error: %+v", err)
		}
	}()
	buffer := make([]byte, 128*1024)
	_, err = io.CopyBuffer(writer, r, buffer)
	if err != nil {
		log.Warn().Msgf("gcs.PutFile: can't copy buffer: %+v", err)
		return err
	}
	if err = writer.Close(); err != nil {
		log.Warn().Msgf("gcs.PutFile: can't close writer: %+v", err)
		return err
	}
	return nil
}

func (gcs *GCS) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return gcs.StatFileAbsolute(ctx, path.Join(gcs.Config.Path, key))
}

func (gcs *GCS) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	// Do NOT apply encryption for object_disks files - they are not encrypted
	// because ClickHouse needs to read them directly without encryption key
	isObjectDiskPath := gcs.Config.ObjectDiskPath != "" && strings.HasPrefix(key, gcs.Config.ObjectDiskPath)
	if !isObjectDiskPath {
		obj = gcs.applyEncryption(obj)
	}
	objAttr, err := obj.Attrs(ctx)
	if err != nil {
		// If the object is not encrypted but we tried to read it with encryption key,
		// retry without encryption (for backward compatibility with old backups)
		if !isObjectDiskPath && gcs.isNotEncryptedError(err) && gcs.encryptionKey != nil {
			log.Warn().Msgf("gcs.StatFile: object %s not encrypted, retrying without encryption key", key)
			obj = gcs.client.Bucket(gcs.Config.Bucket).Object(key)
			objAttr, err = obj.Attrs(ctx)
		}
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				return nil, ErrNotFound
			}
			return nil, err
		}
	}
	return &gcsFile{
		size:         objAttr.Size,
		lastModified: objAttr.Updated,
		name:         objAttr.Name,
	}, nil
}

func (gcs *GCS) deleteKey(ctx context.Context, key string) error {
	pClientObj, err := gcs.clientPool.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("gcs.deleteKey: gcs.clientPool.BorrowObject error: %+v", err)
		return err
	}
	pClient := pClientObj.(*clientObject).Client
	object := pClient.Bucket(gcs.Config.Bucket).Object(key)
	err = object.Delete(ctx)
	if err != nil {
		if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
			log.Warn().Msgf("gcs.deleteKey: gcs.clientPool.InvalidateObject error: %+v", pErr)
		}
		return err
	}
	if pErr := gcs.clientPool.ReturnObject(ctx, pClientObj); pErr != nil {
		log.Warn().Msgf("gcs.deleteKey: gcs.clientPool.ReturnObject error: %+v", pErr)
	}
	return nil
}

func (gcs *GCS) DeleteFile(ctx context.Context, key string) error {
	key = path.Join(gcs.Config.Path, key)
	return gcs.deleteKey(ctx, key)
}

func (gcs *GCS) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	key = path.Join(gcs.Config.ObjectDiskPath, key)
	return gcs.deleteKey(ctx, key)
}

// DeleteKeysBatch implements BatchDeleter interface for GCS
// Uses concurrent deletion with connection pool since GCS doesn't have batch delete API
func (gcs *GCS) DeleteKeysBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Prepend path to all keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = path.Join(gcs.Config.Path, key)
	}
	return gcs.deleteKeysConcurrent(ctx, fullKeys)
}

// DeleteKeysFromObjectDiskBackupBatch implements BatchDeleter interface for GCS
func (gcs *GCS) DeleteKeysFromObjectDiskBackupBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Prepend object disk path to all keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = path.Join(gcs.Config.ObjectDiskPath, key)
	}
	return gcs.deleteKeysConcurrent(ctx, fullKeys)
}

// deleteKeysConcurrent performs concurrent deletion using connection pool
func (gcs *GCS) deleteKeysConcurrent(ctx context.Context, keys []string) error {
	concurrency := gcs.Config.DeleteConcurrency

	log.Debug().Msgf("GCS batch delete: deleting %d keys with concurrency %d", len(keys), concurrency)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var mu sync.Mutex
	var failures []KeyError
	deletedCount := 0

	for _, key := range keys {
		key := key // capture for goroutine
		g.Go(func() error {
			pClientObj, err := gcs.clientPool.BorrowObject(ctx)
			if err != nil {
				mu.Lock()
				failures = append(failures, KeyError{Key: key, Err: fmt.Errorf("failed to borrow client: %w", err)})
				mu.Unlock()
				return nil // Don't fail the entire group
			}
			pClient := pClientObj.(*clientObject).Client
			object := pClient.Bucket(gcs.Config.Bucket).Object(key)
			err = object.Delete(ctx)
			if err != nil {
				// Check if it's a "not found" error - that's OK
				if errors.Is(err, storage.ErrObjectNotExist) {
					if pErr := gcs.clientPool.ReturnObject(ctx, pClientObj); pErr != nil {
						log.Warn().Msgf("gcs.deleteKeysConcurrent: gcs.clientPool.ReturnObject error: %+v", pErr)
					}
					mu.Lock()
					deletedCount++
					mu.Unlock()
					return nil
				}
				if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
					log.Warn().Msgf("gcs.deleteKeysConcurrent: gcs.clientPool.InvalidateObject error: %+v", pErr)
				}
				mu.Lock()
				failures = append(failures, KeyError{Key: key, Err: err})
				mu.Unlock()
				return nil
			}
			if pErr := gcs.clientPool.ReturnObject(ctx, pClientObj); pErr != nil {
				log.Warn().Msgf("gcs.deleteKeysConcurrent: gcs.clientPool.ReturnObject error: %+v", pErr)
			}
			mu.Lock()
			deletedCount++
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "GCS concurrent delete failed")
	}

	if len(failures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("GCS batch delete: %d keys deleted, %d failed", deletedCount, len(failures)),
			Failures: failures,
		}
	}

	log.Debug().Msgf("GCS batch delete: successfully deleted %d keys", deletedCount)
	return nil
}

func (gcs *GCS) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	dstKey = path.Join(gcs.Config.ObjectDiskPath, dstKey)
	log.Debug().Msgf("GCS->CopyObject %s/%s -> %s/%s", srcBucket, srcKey, gcs.Config.Bucket, dstKey)
	pClientObj, err := gcs.clientPool.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("gcs.CopyObject: gcs.clientPool.BorrowObject error: %+v", err)
		return 0, err
	}
	pClient := pClientObj.(*clientObject).Client
	src := pClient.Bucket(srcBucket).Object(srcKey)
	// Do NOT apply encryption for object_disks files - they must be readable by ClickHouse
	// which doesn't have access to the encryption key
	dst := pClient.Bucket(gcs.Config.Bucket).Object(dstKey)
	// always retry transient errors to mitigate retry logic bugs.
	dst = dst.Retryer(storage.WithPolicy(storage.RetryAlways))
	attrs, err := src.Attrs(ctx)
	if err != nil {
		if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
			log.Warn().Msgf("gcs.CopyObject: gcs.clientPool.InvalidateObject error: %+v", pErr)
		}
		return 0, err
	}
	copier := dst.CopierFrom(src)
	// Note: source and destination objects for object disks are not encrypted
	// because ClickHouse needs to read them directly without encryption key
	if _, err = copier.Run(ctx); err != nil {
		if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
			log.Warn().Msgf("gcs.CopyObject: gcs.clientPool.InvalidateObject error: %+v", pErr)
		}
		return 0, err
	}
	if pErr := gcs.clientPool.ReturnObject(ctx, pClientObj); pErr != nil {
		log.Warn().Msgf("gcs.CopyObject: gcs.clientPool.ReturnObject error: %+v", pErr)
	}
	return attrs.Size, nil
}

type gcsFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *gcsFile) Size() int64 {
	return f.size
}

func (f *gcsFile) Name() string {
	return f.name
}

func (f *gcsFile) LastModified() time.Time {
	return f.lastModified
}
