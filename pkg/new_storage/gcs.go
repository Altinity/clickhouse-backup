package new_storage

import (
	"context"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"google.golang.org/api/option/internaloption"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/apex/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	googleHTTPTransport "google.golang.org/api/transport/http"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client *storage.Client
	Config *config.GCSConfig
}

type debugGCSTransport struct {
	base http.RoundTripper
}

func (w debugGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	logMsg := fmt.Sprintf(">>> [GCS_REQUEST] >>> %v %v\n", r.Method, r.URL.String())
	for h, values := range r.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Info(logMsg)

	resp, err := w.base.RoundTrip(r)
	if err != nil {
		log.Errorf("GCS_ERROR: %v", err)
		return resp, err
	}
	logMsg = fmt.Sprintf("<<< [GCS_RESPONSE] <<< %v %v\n", r.Method, r.URL.String())
	for h, values := range resp.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Info(logMsg)
	return resp, err
}

// Connect - connect to GCS
func (gcs *GCS) Connect() error {
	var err error
	clientOptions := make([]option.ClientOption, 0)
	ctx := context.Background()
	clientOptions = append(clientOptions, option.WithTelemetryDisabled())
	endpoint := "https://storage.googleapis.com/storage/v1/"
	if gcs.Config.Endpoint != "" {
		endpoint = gcs.Config.Endpoint
		clientOptions = append([]option.ClientOption{option.WithoutAuthentication()}, clientOptions...)
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
	} else if gcs.Config.CredentialsJSON != "" {
		clientOptions = append(clientOptions, option.WithCredentialsJSON([]byte(gcs.Config.CredentialsJSON)))
	} else if gcs.Config.CredentialsFile != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(gcs.Config.CredentialsFile))
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

	gcs.client, err = storage.NewClient(ctx, clientOptions...)
	return err
}

func (gcs *GCS) Walk(gcsPath string, recursive bool, process func(r RemoteFile) error) error {
	ctx := context.Background()
	rootPath := path.Join(gcs.Config.Path, gcsPath)
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
		switch err {
		case nil:
			if object.Prefix != "" {
				if err := process(&gcsFile{
					name: strings.TrimPrefix(object.Prefix, rootPath),
				}); err != nil {
					return err
				}
				continue
			}
			if err := process(&gcsFile{
				size:         object.Size,
				lastModified: object.Updated,
				name:         strings.TrimPrefix(object.Name, rootPath),
			}); err != nil {
				return err
			}
		case iterator.Done:
			return nil
		default:
			return err
		}
	}
}

func (gcs *GCS) Kind() string {
	return "GCS"
}

func (gcs *GCS) GetFileReader(key string) (io.ReadCloser, error) {
	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key))
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (gcs *GCS) GetFileReaderWithLocalPath(key, _ string) (io.ReadCloser, error) {
	return gcs.GetFileReader(key)
}

func (gcs *GCS) GetFileWriter(key string) io.WriteCloser {
	ctx := context.Background()
	key = path.Join(gcs.Config.Path, key)
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	return obj.NewWriter(ctx)
}

func (gcs *GCS) PutFile(key string, r io.ReadCloser) error {
	ctx := context.Background()
	key = path.Join(gcs.Config.Path, key)
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	writer := obj.NewWriter(ctx)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Warnf("can't close writer: %+v", err)
		}
	}()
	buffer := make([]byte, 4*1024*1024)
	_, err := io.CopyBuffer(writer, r, buffer)
	return err
}

func (gcs *GCS) StatFile(key string) (RemoteFile, error) {
	ctx := context.Background()
	objAttr, err := gcs.client.Bucket(gcs.Config.Bucket).Object(path.Join(gcs.Config.Path, key)).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
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

func (gcs *GCS) DeleteFile(key string) error {
	ctx := context.Background()
	key = path.Join(gcs.Config.Path, key)
	object := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	return object.Delete(ctx)
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
