package new_storage

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"golang.org/x/sync/errgroup"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

type S3LogToApexLogAdapter struct {
	apexLog *log.Logger
}

func newS3Logger() *S3LogToApexLogAdapter {
	return &S3LogToApexLogAdapter{
		apexLog: log.Log.(*log.Logger),
	}
}

func (S3LogToApexLogAdapter *S3LogToApexLogAdapter) Log(args ...interface{}) {
	if len(args) > 1 {
		S3LogToApexLogAdapter.apexLog.Infof(args[0].(string), args[1:]...)
	} else {
		S3LogToApexLogAdapter.apexLog.Infof(args[0].(string))
	}
}

// S3 - presents methods for manipulate data on s3
type S3 struct {
	session     *session.Session
	uploader    *s3manager.Uploader
	Config      *config.S3Config
	PartSize    int64
	Concurrency int
	BufferSize  int
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
	if s.Config.Debug {
		awsConfig.Logger = newS3Logger()
		awsConfig.LogLevel = aws.LogLevel(aws.LogDebug)
	}

	if s.Config.DisableCertVerification {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.HTTPClient = &http.Client{Transport: tr}
	}
	if s.session, err = session.NewSession(awsConfig); err != nil {
		return err
	}

	s.uploader = s3manager.NewUploader(s.session)
	s.uploader.Concurrency = s.Concurrency
	s.uploader.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(s.BufferSize)
	s.uploader.PartSize = s.PartSize

	return nil
}

func (s *S3) Kind() string {
	return "S3"
}

func (s *S3) GetFileReader(key string) (io.ReadCloser, error) {
	// downloader := s3manager.NewDownloader(s.session)
	// downloader.Concurrency = s.Concurrency
	// downloader.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(s.BufferSize)
	// w:= aws.NewWriteAt()
	// downloader.
	// downloader.Download(w, &s3.GetObjectInput{
	// 	Bucket: aws.String(s.Config.Bucket),
	// 	Key:    aws.String(path.Join(s.Config.Path, key)),
	// }

	// )

	svc := s3.New(s.session)
	req, resp := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	})
	if err := req.Send(); err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (s *S3) PutFile(key string, r io.ReadCloser) error {
	var sse *string
	if s.Config.SSE != "" {
		sse = aws.String(s.Config.SSE)
	}
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		ACL:                  aws.String(s.Config.ACL),
		Bucket:               aws.String(s.Config.Bucket),
		Key:                  aws.String(path.Join(s.Config.Path, key)),
		Body:                 r,
		ServerSideEncryption: sse,
		StorageClass:         aws.String(strings.ToUpper(s.Config.StorageClass)),
	})
	return err
}

func (s *S3) DeleteFile(key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	if _, err := s3.New(s.session).DeleteObject(params); err != nil {
		return errors.Wrapf(err, "DeleteFile, deleting object %+v", params)
	}
	return nil
}

func (s *S3) StatFile(key string) (RemoteFile, error) {
	svc := s3.New(s.session)
	head, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
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

func (s *S3) Walk(s3Path string, recursive bool, process func(r RemoteFile) error) error {
	g, _ := errgroup.WithContext(context.Background())
	s3Files := make(chan *s3File)
	g.Go(func() error {
		defer close(s3Files)
		return s.remotePager(path.Join(s.Config.Path, s3Path), recursive, func(page *s3.ListObjectsV2Output) {
			for _, cp := range page.CommonPrefixes {
				s3Files <- &s3File{
					name: strings.TrimPrefix(*cp.Prefix, path.Join(s.Config.Path, s3Path)),
				}
			}
			for _, c := range page.Contents {
				s3Files <- &s3File{
					*c.Size,
					*c.LastModified,
					strings.TrimPrefix(*c.Key, path.Join(s.Config.Path, s3Path)),
				}
			}
		})
	})
	g.Go(func() error {
		for s3File := range s3Files {
			if err := process(s3File); err != nil {
				return err
			}
		}
		return nil
	})
	return g.Wait()
}

func (s *S3) remotePager(s3Path string, recursive bool, pager func(page *s3.ListObjectsV2Output)) error {
	prefix := s3Path + "/"
	if s3Path == "" || s3Path == "/" {
		prefix = ""
	}
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Config.Bucket), // Required
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(prefix),
	}
	if !recursive {
		params.SetDelimiter("/")
	}
	wrapper := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		pager(page)
		return !lastPage
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
