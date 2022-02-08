package storage

import (
	"crypto/tls"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

// S3 - presents methods for manipulate data on s3
type S3 struct {
	session *session.Session
	Config  *config.S3Config
	Debug   bool
}

// Connect - connect to s3
func (s *S3) Connect() error {
	var err error

	awsDefaults := defaults.Get()
	defaultCredProviders := defaults.CredProviders(awsDefaults.Config, awsDefaults.Handlers)

	// Define custom static cred provider
	staticCreds := &credentials.StaticProvider{Value: credentials.Value{
		AccessKeyID:     s.Config.AccessKey,
		SecretAccessKey: s.Config.SecretKey,
	}}

	// Append static creds to the defaults
	customCredProviders := append([]credentials.Provider{staticCreds}, defaultCredProviders...)
	creds := credentials.NewChainCredentials(customCredProviders)

	var awsConfig = &aws.Config{
		Credentials:      creds,
		Region:           aws.String(s.Config.Region),
		Endpoint:         aws.String(s.Config.Endpoint),
		DisableSSL:       aws.Bool(s.Config.DisableSSL),
		S3ForcePathStyle: aws.Bool(s.Config.ForcePathStyle),
		MaxRetries:       aws.Int(30),
	}

	if s.Config.DisableCertVerification {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			Proxy:           http.ProxyFromEnvironment,
		}
		awsConfig.HTTPClient = &http.Client{Transport: tr}
	}

	if s.Debug {
		awsConfig.LogLevel = aws.LogLevel(aws.LogDebugWithRequestErrors)
	}

	if s.session, err = session.NewSession(awsConfig); err != nil {
		return err
	}
	return nil
}

func (s *S3) Kind() string {
	return "S3"
}

func (s *S3) GetFileReader(key string) (io.ReadCloser, error) {
	svc := s3.New(s.session)
	req, resp := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	})
	if err := req.Send(); err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3) PutFile(key string, r io.ReadCloser) error {
	uploader := s3manager.NewUploader(s.session)
	uploader.Concurrency = 1
	var sse *string
	if s.Config.SSE != "" {
		sse = aws.String(s.Config.SSE)
	}
	_, err := uploader.Upload(&s3manager.UploadInput{
		ACL:                  aws.String(s.Config.ACL),
		Bucket:               aws.String(s.Config.Bucket),
		Key:                  aws.String(key),
		Body:                 r,
		ServerSideEncryption: sse,
		StorageClass:         aws.String(strings.ToUpper(s.Config.StorageClass)),
	})
	return err
}

func (s *S3) DeleteFile(key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	}

	_, err := s3.New(s.session).DeleteObject(params)
	if err != nil {
		return errors.Wrapf(err, "DeleteFile, deleting object %+v", params)
	}
	return nil
}

func (s *S3) GetFile(key string) (RemoteFile, error) {
	svc := s3.New(s.session)
	head, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == "NotFound" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &s3File{*head.ContentLength, *head.LastModified, key}, nil
}

func (s *S3) Walk(s3Path string, process func(r RemoteFile)) error {
	return s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			process(&s3File{*c.Size, *c.LastModified, *c.Key})
		}
	})
}

func (s *S3) remotePager(s3Path string, delim bool, pager func(page *s3.ListObjectsV2Output)) error {
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Config.Bucket), // Required
		MaxKeys: aws.Int64(1000),
	}
	if s3Path != "" && s3Path != "/" {
		params.Prefix = aws.String(s3Path)
	}
	if delim {
		params.Delimiter = aws.String("/")
	}
	wrapper := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		pager(page)
		return true
	}
	return s3.New(s.session).ListObjectsV2Pages(params, wrapper)
}

type s3File struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *s3File) Size() int64 {
	return f.size
}

func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) LastModified() time.Time {
	return f.lastModified
}
