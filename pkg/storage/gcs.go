package storage

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"google.golang.org/api/iterator"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	pool "github.com/jolestar/go-commons-pool/v2"
	"google.golang.org/api/option/internaloption"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
	googleHTTPTransport "google.golang.org/api/transport/http"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client     *storage.Client
	Config     *config.GCSConfig
	clientPool *pool.ObjectPool
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
	clientOptions := make([]option.ClientOption, 0)
	clientOptions = append(clientOptions, option.WithTelemetryDisabled())
	endpoint := "https://storage.googleapis.com/storage/v1/"

	if gcs.Config.Endpoint != "" {
		endpoint = gcs.Config.Endpoint
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
	}

	if gcs.Config.CredentialsJSON != "" {
		clientOptions = append(clientOptions, option.WithCredentialsJSON([]byte(gcs.Config.CredentialsJSON)))
	} else if gcs.Config.CredentialsJSONEncoded != "" {
		d, _ := base64.StdEncoding.DecodeString(gcs.Config.CredentialsJSONEncoded)
		clientOptions = append(clientOptions, option.WithCredentialsJSON(d))
	} else if gcs.Config.CredentialsFile != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(gcs.Config.CredentialsFile))
	} else if gcs.Config.SkipCredentials {
		clientOptions = append(clientOptions, option.WithoutAuthentication())
	}

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
		// These clientOptions are passed in by storage.NewClient. However, to set a custom HTTP client
		// we must pass all these in manually.

		if gcs.Config.Endpoint == "" {
			clientOptions = append([]option.ClientOption{option.WithScopes(storage.ScopeFullControl)}, clientOptions...)
		}
		clientOptions = append(clientOptions, internaloption.WithDefaultEndpoint(endpoint))

		customRoundTripper := &rewriteTransport{base: customTransport}
		gcpTransport, _, err := googleHTTPTransport.NewClient(ctx, clientOptions...)
		transport, err := googleHTTPTransport.NewTransport(ctx, customRoundTripper, clientOptions...)
		gcpTransport.Transport = transport
		if err != nil {
			return fmt.Errorf("failed to create GCP transport: %v", err)
		}

		clientOptions = append(clientOptions, option.WithHTTPClient(gcpTransport))

	}

	if gcs.Config.Debug {
		if gcs.Config.Endpoint == "" {
			clientOptions = append([]option.ClientOption{option.WithScopes(storage.ScopeFullControl)}, clientOptions...)
		}
		clientOptions = append(clientOptions, internaloption.WithDefaultEndpoint(endpoint))
		if strings.HasPrefix(endpoint, "https://") {
			clientOptions = append(clientOptions, internaloption.WithDefaultMTLSEndpoint(endpoint))
		}

		debugClient, _, err := googleHTTPTransport.NewClient(ctx, clientOptions...)
		if err != nil {
			return fmt.Errorf("googleHTTPTransport.NewClient error: %v", err)
		}
		debugClient.Transport = debugGCSTransport{base: debugClient.Transport}
		clientOptions = append(clientOptions, option.WithHTTPClient(debugClient))
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
	return err
}

func (gcs *GCS) Close(ctx context.Context) error {
	gcs.clientPool.Close(ctx)
	return gcs.client.Close()
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
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
			log.Warn().Msgf("gcs.GetFileReader: gcs.clientPool.InvalidateObject error: %v ", pErr)
		}
		return nil, err
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
	objAttr, err := gcs.client.Bucket(gcs.Config.Bucket).Object(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
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
	dst := pClient.Bucket(gcs.Config.Bucket).Object(dstKey)
	attrs, err := src.Attrs(ctx)
	if err != nil {
		if pErr := gcs.clientPool.InvalidateObject(ctx, pClientObj); pErr != nil {
			log.Warn().Msgf("gcs.CopyObject: gcs.clientPool.InvalidateObject error: %+v", pErr)
		}
		return 0, err
	}
	if _, err = dst.CopierFrom(src).Run(ctx); err != nil {
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
