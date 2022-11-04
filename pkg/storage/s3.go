package storage

import (
	"context"
	"crypto/tls"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	apexLog "github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pkg/errors"
)

type S3LogToApexLogAdapter struct {
	apexLog *apexLog.Logger
}

func newS3Logger(log *apexLog.Entry) *S3LogToApexLogAdapter {
	return &S3LogToApexLogAdapter{
		apexLog: log.Logger,
	}
}

func (S3LogToApexLogAdapter *S3LogToApexLogAdapter) Log(args ...interface{}) {
	if len(args) > 1 {
		S3LogToApexLogAdapter.apexLog.Infof(args[0].(string), args[1:]...)
	} else {
		S3LogToApexLogAdapter.apexLog.Info(args[0].(string))
	}
}

// S3 - presents methods for manipulate data on s3
type S3 struct {
	session     *session.Session
	uploader    *s3manager.Uploader
	downloader  *s3manager.Downloader
	Config      *config.S3Config
	Log         *apexLog.Entry
	PartSize    int64
	Concurrency int
	BufferSize  int
	versioning  bool
}

func (s *S3) Kind() string {
	return "S3"
}

// Connect - connect to s3
func (s *S3) Connect(ctx context.Context) error {
	var err error

	awsDefaults := defaults.Get()
	defaultCredProviders := defaults.CredProviders(awsDefaults.Config, awsDefaults.Handlers)
	customCredProviders := defaultCredProviders

	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		// Define custom static cred provider
		staticCreds := &credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     s.Config.AccessKey,
			SecretAccessKey: s.Config.SecretKey,
		}}
		// Append static creds to the defaults
		customCredProviders = append([]credentials.Provider{staticCreds}, customCredProviders...)
	}

	awsRoleARN := os.Getenv("AWS_ROLE_ARN")
	awsWebIdentityTokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if awsRoleARN != "" && awsWebIdentityTokenFile != "" {
		cfg := &aws.Config{
			Region: aws.String(s.Config.Region),
		}
		stsSvc := sts.New(session.Must(session.NewSession(cfg)))
		stsProvider := stscreds.NewWebIdentityRoleProviderWithOptions(stsSvc, awsRoleARN, "", stscreds.FetchTokenPath(awsWebIdentityTokenFile))
		customCredProviders = append([]credentials.Provider{stsProvider}, customCredProviders...)
	}

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
		awsConfig.Logger = newS3Logger(s.Log)
		awsConfig.LogLevel = aws.LogLevel(aws.LogDebug)
	}

	if s.Config.DisableCertVerification {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.HTTPClient = &http.Client{Transport: tr}
	}

	if s.Config.AssumeRoleARN != "" {
		// Reference to regular credentials chain is to be copied into `stscreds` credentials.
		awsConfig.Credentials = stscreds.NewCredentials(session.Must(session.NewSession(awsConfig)), s.Config.AssumeRoleARN)
	}

	if s.session, err = session.NewSession(awsConfig); err != nil {
		return err
	}

	s.uploader = s3manager.NewUploader(s.session)
	s.uploader.Concurrency = s.Concurrency
	s.uploader.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(s.BufferSize)
	s.uploader.PartSize = s.PartSize

	s.downloader = s3manager.NewDownloader(s.session)
	s.downloader.Concurrency = s.Concurrency
	s.downloader.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(s.BufferSize)
	s.downloader.PartSize = s.PartSize

	s.versioning = s.isVersioningEnabled()

	return nil
}

func (s *S3) Close(ctx context.Context) error {
	return nil
}

func (s *S3) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
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

func (s *S3) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string) (io.ReadCloser, error) {
	/* unfortunately, multipart download require allocate additional disk space
	and don't allow us to decompress data directly from stream */
	if s.Config.AllowMultipartDownload {
		writer, err := os.CreateTemp(localPath, strings.ReplaceAll(key, "/", "_"))
		if err != nil {
			return nil, err
		}
		_, err = s.downloader.Download(writer, &s3.GetObjectInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(path.Join(s.Config.Path, key)),
		})
		if err != nil {
			return nil, err
		}
		return writer, nil
	} else {
		return s.GetFileReader(ctx, key)
	}
}

func (s *S3) PutFile(ctx context.Context, key string, r io.ReadCloser) error {
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

func (s *S3) DeleteFile(ctx context.Context, key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	if s.versioning {
		objVersion, err := s.getObjectVersion(key)
		if err != nil {
			return errors.Wrapf(err, "DeleteFile, obtaining object version %+v", params)
		}
		params.VersionId = objVersion
	}
	if _, err := s3.New(s.session).DeleteObject(params); err != nil {
		return errors.Wrapf(err, "DeleteFile, deleting object %+v", params)
	}
	return nil
}

func (s *S3) isVersioningEnabled() bool {
	output, err := s3.New(s.session).GetBucketVersioning(&s3.GetBucketVersioningInput{
		Bucket: aws.String(s.Config.Bucket),
	})
	if err != nil {
		return false
	}
	return output.Status != nil && *output.Status == s3.BucketVersioningStatusEnabled
}

func (s *S3) getObjectVersion(key string) (*string, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	object, err := s3.New(s.session).GetObject(params)
	if err != nil {
		return nil, err
	}
	return object.VersionId, nil
}

func (s *S3) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	svc := s3.New(s.session)
	head, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	})
	if err != nil {
		awsError, ok := err.(awserr.Error)
		if ok && awsError.Code() == "NotFound" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &s3File{*head.ContentLength, *head.LastModified, key}, nil
}

func (s *S3) Walk(ctx context.Context, s3Path string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	g, ctx := errgroup.WithContext(ctx)
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
		var err error
		for s3File := range s3Files {
			if err == nil {
				err = process(ctx, s3File)
			}
		}
		return err
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
