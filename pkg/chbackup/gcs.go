package chbackup

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client *storage.Client
	Config *GCSConfig
}

// Connect - connect to GCS
func (gcs *GCS) Connect() error {
	var err error
	var clientOption option.ClientOption

	ctx := context.Background()

	if gcs.Config.CredentialsJSON != "" {
		clientOption = option.WithCredentialsJSON([]byte(gcs.Config.CredentialsJSON))
		gcs.client, err = storage.NewClient(ctx, clientOption)
	} else if gcs.Config.CredentialsFile != "" {
		clientOption = option.WithCredentialsFile(gcs.Config.CredentialsFile)
		gcs.client, err = storage.NewClient(ctx, clientOption)
	} else {
		gcs.client, err = storage.NewClient(ctx)
	}
	return err
}

func (gcs *GCS) Walk(gcsPath string, process func(r RemoteFile)) error {
	ctx := context.Background()
	it := gcs.client.Bucket(gcs.Config.Bucket).Objects(ctx, nil)
	for {
		object, err := it.Next()
		switch err {
		case nil:
			process(&gcsFile{object})
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
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (gcs *GCS) GetFileWriter(key string) io.WriteCloser {
	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	return obj.NewWriter(ctx)
}

func (gcs *GCS) PutFile(key string, r io.ReadCloser) error {
	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	writer := obj.NewWriter(ctx)
	if _, err := io.Copy(writer, r); err != nil {
		return err
	}
	return writer.Close()
}

func (gcs *GCS) GetFile(key string) (RemoteFile, error) {
	ctx := context.Background()
	objAttr, err := gcs.client.Bucket(gcs.Config.Bucket).Object(key).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &gcsFile{objAttr}, nil
}

func (gcs *GCS) DeleteFile(key string) error {
	ctx := context.Background()
	object := gcs.client.Bucket(gcs.Config.Bucket).Object(key)
	return object.Delete(ctx)
}

type gcsFile struct {
	objAttr *storage.ObjectAttrs
}

func (f *gcsFile) Size() int64 {
	return f.objAttr.Size
}

func (f *gcsFile) Name() string {
	return f.objAttr.Name
}

func (f *gcsFile) LastModified() time.Time {
	return f.objAttr.Updated
}
