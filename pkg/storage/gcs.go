package storage

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	pool "github.com/jolestar/go-commons-pool/v2"
	"github.com/pkg/errors"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/transfermanager"
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
	grpcClient    *storage.Client // used only for parallel composite uploads, see https://github.com/Altinity/clickhouse-backup/issues/1028
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
		return resp, errors.Wrap(err, "GCS debugTransport RoundTrip")
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

// detectGCSCredentialType inspects the `type` field of a Google credentials JSON
// document and maps it to the option.CredentialsType required by WithAuthCredentialsJSON/File.
func detectGCSCredentialType(data []byte) option.CredentialsType {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return option.ServiceAccount
	}
	switch probe.Type {
	case "authorized_user":
		return option.AuthorizedUser
	case "impersonated_service_account":
		return option.ImpersonatedServiceAccount
	case "external_account":
		return option.ExternalAccount
	default:
		return option.ServiceAccount
	}
}

type rewriteTransport struct {
	base http.RoundTripper
}

// RoundTrip forces requests to target varnish and use HTTP, required to get uploading
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
		d := []byte(gcs.Config.CredentialsJSON)
		credOption = option.WithAuthCredentialsJSON(detectGCSCredentialType(d), d)
	} else if gcs.Config.CredentialsJSONEncoded != "" {
		d, _ := base64.StdEncoding.DecodeString(gcs.Config.CredentialsJSONEncoded)
		credOption = option.WithAuthCredentialsJSON(detectGCSCredentialType(d), d)
	} else if gcs.Config.CredentialsFile != "" {
		d, err := os.ReadFile(gcs.Config.CredentialsFile)
		if err != nil {
			return errors.Wrap(err, "GCS Connect failed to read credentials_file")
		}
		credOption = option.WithAuthCredentialsFile(detectGCSCredentialType(d), gcs.Config.CredentialsFile)
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

	// 2. Build base client options (credentials + telemetry)
	clientOptions := []option.ClientOption{option.WithTelemetryDisabled()}
	if gcs.Config.Endpoint != "" {
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
	}
	if credOption != nil {
		clientOptions = append(clientOptions, credOption)
	}

	// 3. For ForceHttp, DisableHttp2, or Debug we need a custom HTTP client;
	//    otherwise let storage.NewClient create its own optimized transport.
	if gcs.Config.ForceHttp || gcs.Config.DisableHttp2 || gcs.Config.Debug {
		// Scopes are required when dialing manually
		if !gcs.Config.SkipCredentials {
			clientOptions = append(clientOptions, option.WithScopes(storage.ScopeFullControl))
		}

		var httpClient *http.Client
		if gcs.Config.ForceHttp || gcs.Config.DisableHttp2 {
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
			if gcs.Config.DisableHttp2 {
				// DisableHttp2 is designed for high-concurrency downloads — raise
				// connection limits so each parallel part gets its own TCP stream.
				customTransport.MaxIdleConns = 0
				customTransport.MaxIdleConnsPerHost = 64
			}
			// must set ForceAttemptHTTP2 to false so that when a custom TLSClientConfig
			// is provided Golang does not setup HTTP/2 transport
			customTransport.ForceAttemptHTTP2 = false
			customTransport.TLSClientConfig = &tls.Config{
				NextProtos: []string{"http/1.1"},
			}
			// ForceHttp downgrades the request scheme to cleartext http:// via
			// rewriteTransport (needed only for internal caches like varnish).
			// DisableHttp2 keeps the original https:// scheme so TLS and
			// HTTPS_PROXY continue to work — it only suppresses HTTP/2 so that
			// concurrent transfers use separate TCP connections instead of being
			// multiplexed onto one. When both are set, ForceHttp wins.
			var roundTripper http.RoundTripper = customTransport
			if gcs.Config.ForceHttp {
				roundTripper = &rewriteTransport{base: customTransport}
			}
			transport, err := googleHTTPTransport.NewTransport(ctx, roundTripper, clientOptions...)
			if err != nil {
				return errors.Wrap(err, "failed to create GCP transport")
			}
			httpClient = &http.Client{Transport: transport}
		} else {
			httpClient, _, err = googleHTTPTransport.NewClient(ctx, clientOptions...)
			if err != nil {
				return errors.Wrap(err, "googleHTTPTransport.NewClient error")
			}
		}

		if gcs.Config.Debug {
			httpClient.Transport = debugGCSTransport{base: httpClient.Transport}
		}

		// Replace clientOptions: credentials are already baked into httpClient
		clientOptions = []option.ClientOption{
			option.WithHTTPClient(httpClient),
			option.WithTelemetryDisabled(),
		}
		if gcs.Config.Endpoint != "" {
			clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
		}
	}

	factory := pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			sClient, err := storage.NewClient(ctx, clientOptions...)
			if err != nil {
				return nil, err
			}
			return &clientObject{Client: sClient}, nil
		})
	gcs.clientPool = pool.NewObjectPoolWithDefaultConfig(ctx, factory)
	gcs.clientPool.Config.MaxTotal = gcs.Config.ClientPoolSize * 3
	gcs.client, err = storage.NewClient(ctx, clientOptions...)
	if err != nil {
		return errors.Wrap(err, "GCS Connect storage.NewClient")
	}

	if gcs.Config.AllowMultipartUpload {
		grpcOptions := []option.ClientOption{option.WithTelemetryDisabled()}
		if credOption != nil {
			grpcOptions = append(grpcOptions, credOption)
		}
		gcs.grpcClient, err = storage.NewGRPCClient(ctx, grpcOptions...)
		if err != nil {
			return errors.Wrap(err, "GCS Connect storage.NewGRPCClient")
		}
		// client-level retry policy, part uploads inside parallel upload create
		// fresh ObjectHandle's from the client, so per-object Retryer doesn't cover them
		gcs.grpcClient.SetRetry(storage.WithPolicy(storage.RetryAlways))
	}

	// Validate and decode the encryption key if provided
	if gcs.Config.EncryptionKey != "" {
		key, err := base64.StdEncoding.DecodeString(gcs.Config.EncryptionKey)
		if err != nil {
			return errors.Wrap(err, "gcs: malformed encryption_key, must be base64-encoded 256-bit key")
		}
		if len(key) != 32 {
			return errors.Errorf("gcs: malformed encryption_key, must be base64-encoded 256-bit key (got %d bytes)", len(key))
		}
		gcs.encryptionKey = key
		log.Info().Msg("GCS: Customer-Supplied Encryption Key (CSEK) configured")
	}

	return nil
}

func (gcs *GCS) Close(ctx context.Context) error {
	gcs.clientPool.Close(ctx)
	if gcs.grpcClient != nil {
		if err := gcs.grpcClient.Close(); err != nil {
			return errors.Wrap(err, "GCS Close grpcClient")
		}
	}
	if err := gcs.client.Close(); err != nil {
		return errors.Wrap(err, "GCS Close")
	}
	return nil
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
					return errors.Wrap(err, "GCS WalkAbsolute process prefix")
				}
				continue
			}
			if err := process(ctx, &gcsFile{
				size:         object.Size,
				lastModified: object.Updated,
				name:         strings.TrimPrefix(object.Name, rootPath),
			}); err != nil {
				return errors.Wrap(err, "GCS WalkAbsolute process object")
			}
		case errors.Is(err, iterator.Done):
			return nil
		default:
			return errors.Wrap(err, "GCS WalkAbsolute iterator.Next")
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
		return nil, errors.Wrap(err, "GCS GetFileReaderAbsolute BorrowObject")
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
			return nil, errors.Wrap(err, "GCS GetFileReaderAbsolute NewReader")
		}
	}
	if pErr := gcs.clientPool.ReturnObject(ctx, pClientObj); pErr != nil {
		log.Warn().Msgf("gcs.GetFileReader: gcs.clientPool.ReturnObject error: %v ", pErr)
	}
	return reader, nil
}

func (gcs *GCS) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	/* unfortunately, multipart download require allocate additional disk space
	and don't allow us to decompress data directly from stream */
	if gcs.Config.AllowMultipartDownload {
		log.Debug().Msgf("GCS->GetFileReaderWithLocalPath: multipart download %s, size=%d", key, remoteSize)
		writer, err := os.CreateTemp(localPath, strings.ReplaceAll(key, "/", "_"))
		if err != nil {
			return nil, errors.Wrap(err, "GCS GetFileReaderWithLocalPath CreateTemp")
		}
		if err = gcs.downloadMultipart(ctx, key, writer); err != nil {
			_ = writer.Close()
			_ = os.Remove(writer.Name())
			return nil, err
		}
		return writer, nil
	}
	return gcs.GetFileReader(ctx, key)
}

// downloadMultipart downloads the object as parallel range reads via transfermanager.Downloader,
// see https://github.com/Altinity/clickhouse-backup/issues/1028
func (gcs *GCS) downloadMultipart(ctx context.Context, key string, writer io.WriterAt) error {
	fullKey := path.Join(gcs.Config.Path, key)
	partSize := int64(gcs.Config.ChunkSize)
	if partSize <= 0 {
		partSize = 16 * 1024 * 1024
	}
	attempt := func(encryptionKey []byte) error {
		downloader, err := transfermanager.NewDownloader(gcs.client,
			transfermanager.WithWorkers(gcs.Config.DownloadConcurrency),
			transfermanager.WithPartSize(partSize),
		)
		if err != nil {
			return errors.Wrap(err, "GCS downloadMultipart NewDownloader")
		}
		if err = downloader.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
			Bucket:        gcs.Config.Bucket,
			Object:        fullKey,
			Destination:   writer,
			EncryptionKey: encryptionKey,
		}); err != nil {
			return errors.Wrap(err, "GCS downloadMultipart DownloadObject")
		}
		_, err = downloader.WaitAndClose()
		return err
	}
	// Do NOT apply encryption for object_disks files - they are not encrypted
	// because ClickHouse needs to read them directly without encryption key
	encryptionKey := gcs.encryptionKey
	if gcs.Config.ObjectDiskPath != "" && strings.HasPrefix(fullKey, gcs.Config.ObjectDiskPath) {
		encryptionKey = nil
	}
	err := attempt(encryptionKey)
	if err != nil && encryptionKey != nil && gcs.isNotEncryptedError(err) {
		// If the object is not encrypted but we tried to read it with encryption key,
		// retry without encryption (for backward compatibility with old backups)
		log.Warn().Msgf("gcs.downloadMultipart: object %s not encrypted, retrying without encryption key", fullKey)
		err = attempt(nil)
	}
	return errors.Wrap(err, "GCS downloadMultipart WaitAndClose")
}

func (gcs *GCS) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return gcs.PutFileAbsolute(ctx, path.Join(gcs.Config.Path, key), r, localSize)
}

func (gcs *GCS) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	if gcs.grpcClient != nil && localSize >= gcs.Config.MultipartUploadMinSize {
		return gcs.putFileMultipart(ctx, key, r, localSize)
	}
	pClientObj, err := gcs.clientPool.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("gcs.PutFile: gcs.clientPool.BorrowObject error: %+v", err)
		return errors.Wrap(err, "GCS PutFileAbsolute BorrowObject")
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
	uploadBufferSize := gcs.Config.UploadBufferSize
	if uploadBufferSize <= 0 {
		uploadBufferSize = 128 * 1024
	}
	buffer := make([]byte, uploadBufferSize)
	_, err = io.CopyBuffer(writer, r, buffer)
	if err != nil {
		log.Warn().Msgf("gcs.PutFile: can't copy buffer: %+v", err)
		return errors.Wrap(err, "GCS PutFileAbsolute CopyBuffer")
	}
	if err = writer.Close(); err != nil {
		log.Warn().Msgf("gcs.PutFile: can't close writer: %+v", err)
		return errors.Wrap(err, "GCS PutFileAbsolute writer.Close")
	}
	return nil
}

// putFileMultipart uploads a large file with the experimental parallel composite upload,
// temporary parts go to the `gcs-go-sdk-pu-tmp/` prefix in the bucket root and are composed
// into the final object, see https://github.com/Altinity/clickhouse-backup/issues/1028.
// encryption_key is rejected together with allow_multipart_upload in config validation, so no CSEK handling here.
func (gcs *GCS) putFileMultipart(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	log.Debug().Msgf("GCS->putFileMultipart %s, size=%d", key, localSize)
	obj := gcs.grpcClient.Bucket(gcs.Config.Bucket).Object(key)
	writer := obj.NewWriter(ctx)
	writer.EnableParallelUpload = true
	// PartSize <= 0 falls back to the SDK default 16MiB, values below 5MiB are bumped to 5MiB by the SDK
	writer.ParallelUploadConfig = storage.ParallelUploadConfig{
		PartSize:       gcs.Config.ChunkSize,
		MaxConcurrency: gcs.Config.UploadConcurrency,
	}
	writer.StorageClass = gcs.Config.StorageClass
	if len(gcs.Config.ObjectLabels) > 0 {
		writer.Metadata = gcs.Config.ObjectLabels
	}
	uploadBufferSize := gcs.Config.UploadBufferSize
	if uploadBufferSize <= 0 {
		uploadBufferSize = 128 * 1024
	}
	buffer := make([]byte, uploadBufferSize)
	if _, err := io.CopyBuffer(writer, r, buffer); err != nil {
		log.Warn().Msgf("gcs.putFileMultipart: can't copy buffer: %+v", err)
		return errors.Wrap(err, "GCS putFileMultipart CopyBuffer")
	}
	if err := writer.Close(); err != nil {
		log.Warn().Msgf("gcs.putFileMultipart: can't close writer: %+v", err)
		return errors.Wrap(err, "GCS putFileMultipart writer.Close")
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
				return nil, NewErrNotFound(key)
			}
			return nil, errors.Wrap(err, "GCS StatFileAbsolute Attrs")
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
		return errors.Wrap(err, "GCS deleteKey BorrowObject")
	}
	pClient := pClientObj.(*clientObject).Client
	object := pClient.Bucket(gcs.Config.Bucket).Object(key)
	err = object.Delete(ctx)
	if err != nil {
		if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
			log.Warn().Msgf("gcs.deleteKey: gcs.clientPool.InvalidateObject error: %+v", pErr)
		}
		return errors.Wrap(err, "GCS deleteKey Delete")
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
				failures = append(failures, KeyError{Key: key, Err: errors.Wrap(err, "failed to borrow client")})
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

// CopyObject server-side copy from srcBucket/srcKey to gcs.Config.Bucket/dstKey, both keys are absolute inside the bucket
func (gcs *GCS) CopyObject(ctx context.Context, _ int64, srcBucket, srcKey, dstKey string) (int64, error) {
	log.Debug().Msgf("GCS->CopyObject %s/%s -> %s/%s", srcBucket, srcKey, gcs.Config.Bucket, dstKey)
	pClientObj, err := gcs.clientPool.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("gcs.CopyObject: gcs.clientPool.BorrowObject error: %+v", err)
		return 0, errors.Wrap(err, "GCS CopyObject BorrowObject")
	}
	pClient := pClientObj.(*clientObject).Client
	// Do NOT apply encryption for object_disks files - they must be readable by ClickHouse
	// which doesn't have access to the encryption key; backup path objects are encrypted with CSEK
	isSrcObjectDiskPath := gcs.Config.ObjectDiskPath != "" && strings.HasPrefix(srcKey, gcs.Config.ObjectDiskPath)
	isDstObjectDiskPath := gcs.Config.ObjectDiskPath != "" && strings.HasPrefix(dstKey, gcs.Config.ObjectDiskPath)
	src := pClient.Bucket(srcBucket).Object(srcKey)
	if !isSrcObjectDiskPath {
		src = gcs.applyEncryption(src)
	}
	dst := pClient.Bucket(gcs.Config.Bucket).Object(dstKey)
	if !isDstObjectDiskPath {
		dst = gcs.applyEncryption(dst)
	}
	// always retry transient errors to mitigate retry logic bugs.
	dst = dst.Retryer(storage.WithPolicy(storage.RetryAlways))
	attrs, err := src.Attrs(ctx)
	if err != nil {
		// If the source is not encrypted but we tried to read it with encryption key,
		// retry without encryption (for backward compatibility with old backups)
		if !isSrcObjectDiskPath && gcs.encryptionKey != nil && gcs.isNotEncryptedError(err) {
			log.Warn().Msgf("gcs.CopyObject: object %s not encrypted, retrying without encryption key", srcKey)
			src = pClient.Bucket(srcBucket).Object(srcKey)
			attrs, err = src.Attrs(ctx)
		}
		if err != nil {
			if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
				log.Warn().Msgf("gcs.CopyObject: gcs.clientPool.InvalidateObject error: %+v", pErr)
			}
			return 0, errors.Wrap(err, "GCS CopyObject src.Attrs")
		}
	}
	copier := dst.CopierFrom(src)
	if _, err = copier.Run(ctx); err != nil {
		// If the source is not encrypted but we tried to copy it with encryption key,
		// retry without encryption (for backward compatibility with old backups)
		if !isSrcObjectDiskPath && gcs.encryptionKey != nil && gcs.isNotEncryptedError(err) {
			log.Warn().Msgf("gcs.CopyObject: object %s not encrypted, retrying without encryption key", srcKey)
			copier = dst.CopierFrom(pClient.Bucket(srcBucket).Object(srcKey))
			_, err = copier.Run(ctx)
		}
		if err != nil {
			if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
				log.Warn().Msgf("gcs.CopyObject: gcs.clientPool.InvalidateObject error: %+v", pErr)
			}
			return 0, errors.Wrap(err, "GCS CopyObject copier.Run")
		}
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
