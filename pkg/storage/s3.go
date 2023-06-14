package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsV2Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	awsV2Logging "github.com/aws/smithy-go/logging"
	awsV2http "github.com/aws/smithy-go/transport/http"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type S3LogToZeroLogAdapter struct {
	logger zerolog.Logger
}

func newS3Logger(logger zerolog.Logger) S3LogToZeroLogAdapter {
	return S3LogToZeroLogAdapter{
		logger: logger,
	}
}

func (S3LogToApexLogAdapter S3LogToZeroLogAdapter) Logf(severity awsV2Logging.Classification, msg string, args ...interface{}) {
	msg = fmt.Sprintf("[s3:%s] %s", severity, msg)
	if len(args) > 0 {
		S3LogToApexLogAdapter.logger.Info().Msgf(msg, args...)
	} else {
		S3LogToApexLogAdapter.logger.Info().Msg(msg)
	}
}

// S3 - presents methods for manipulate data on s3
type S3 struct {
	client      *s3.Client
	uploader    *s3manager.Uploader
	downloader  *s3manager.Downloader
	Config      *config.S3Config
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
	var awsConfig aws.Config
	awsConfig, err = awsV2Config.LoadDefaultConfig(
		ctx,
		awsV2Config.WithRetryMode(aws.RetryModeAdaptive),
	)
	if err != nil {
		return err
	}
	if s.Config.Region != "" {
		awsConfig.Region = s.Config.Region
	}
	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		awsConfig.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     s.Config.AccessKey,
				SecretAccessKey: s.Config.SecretKey,
			},
		}
	}

	awsRoleARN := os.Getenv("AWS_ROLE_ARN")
	if s.Config.AssumeRoleARN != "" || awsRoleARN != "" {
		stsClient := sts.NewFromConfig(awsConfig)
		if awsRoleARN != "" {
			awsConfig.Credentials = stscreds.NewAssumeRoleProvider(stsClient, awsRoleARN)
		} else {
			awsConfig.Credentials = stscreds.NewAssumeRoleProvider(stsClient, s.Config.AssumeRoleARN)
		}
	}

	awsWebIdentityTokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if awsRoleARN != "" && awsWebIdentityTokenFile != "" {
		stsClient := sts.NewFromConfig(awsConfig)
		awsConfig.Credentials = stscreds.NewWebIdentityRoleProvider(
			stsClient, awsRoleARN, stscreds.IdentityTokenFile(awsWebIdentityTokenFile),
		)
	}

	if s.Config.Debug {
		awsConfig.Logger = newS3Logger(log.Logger)
		awsConfig.ClientLogMode = aws.LogRetries | aws.LogRequest | aws.LogResponse
	}

	if s.Config.DisableCertVerification {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.HTTPClient = &http.Client{Transport: tr}
	}

	if s.Config.Endpoint != "" {
		awsConfig.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               s.Config.Endpoint,
				SigningRegion:     s.Config.Region,
				HostnameImmutable: true,
				Source:            aws.EndpointSourceCustom,
			}, nil
		})

	}
	s.client = s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.UsePathStyle = s.Config.ForcePathStyle
		o.EndpointOptions.DisableHTTPS = s.Config.DisableSSL
	})

	s.uploader = s3manager.NewUploader(s.client)
	s.uploader.Concurrency = s.Concurrency
	s.uploader.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(s.BufferSize)
	s.uploader.PartSize = s.PartSize

	s.downloader = s3manager.NewDownloader(s.client)
	s.downloader.Concurrency = s.Concurrency
	s.downloader.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(s.BufferSize)
	s.downloader.PartSize = s.PartSize

	s.versioning = s.isVersioningEnabled(ctx)

	return nil
}

func (s *S3) Close(ctx context.Context) error {
	return nil
}

func (s *S3) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	})
	if err != nil {
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
		_, err = s.downloader.Download(ctx, writer, &s3.GetObjectInput{
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
	params := s3.PutObjectInput{
		ACL:          s3types.ObjectCannedACL(s.Config.ACL),
		Bucket:       aws.String(s.Config.Bucket),
		Key:          aws.String(path.Join(s.Config.Path, key)),
		Body:         r,
		StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
	}
	// https://github.com/Altinity/clickhouse-backup/issues/588
	if len(s.Config.ObjectLabels) > 0 {
		tags := ""
		for k, v := range s.Config.ObjectLabels {
			if tags != "" {
				tags += "&"
			}
			tags += k + "=" + v
		}
		params.Tagging = aws.String(tags)
	}
	if s.Config.SSE != "" {
		params.ServerSideEncryption = s3types.ServerSideEncryption(s.Config.SSE)
	}
	if s.Config.SSEKMSKeyId != "" {
		params.SSEKMSKeyId = aws.String(s.Config.SSEKMSKeyId)
	}
	if s.Config.SSECustomerAlgorithm != "" {
		params.SSECustomerAlgorithm = aws.String(s.Config.SSECustomerAlgorithm)
	}
	if s.Config.SSECustomerKey != "" {
		params.SSECustomerKey = aws.String(s.Config.SSECustomerKey)
	}
	if s.Config.SSECustomerKeyMD5 != "" {
		params.SSECustomerKeyMD5 = aws.String(s.Config.SSECustomerKeyMD5)
	}
	if s.Config.SSEKMSEncryptionContext != "" {
		params.SSEKMSEncryptionContext = aws.String(s.Config.SSEKMSEncryptionContext)
	}
	_, err := s.uploader.Upload(ctx, &params)
	return err
}

func (s *S3) DeleteFile(ctx context.Context, key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	if s.versioning {
		objVersion, err := s.getObjectVersion(ctx, key)
		if err != nil {
			return errors.Wrapf(err, "DeleteFile, obtaining object version %+v", params)
		}
		params.VersionId = objVersion
	}
	if _, err := s.client.DeleteObject(ctx, params); err != nil {
		return errors.Wrapf(err, "DeleteFile, deleting object %+v", params)
	}
	return nil
}

func (s *S3) isVersioningEnabled(ctx context.Context) bool {
	output, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(s.Config.Bucket),
	})
	if err != nil {
		return false
	}
	return output.Status == s3types.BucketVersioningStatusEnabled
}

func (s *S3) getObjectVersion(ctx context.Context, key string) (*string, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	object, err := s.client.HeadObject(ctx, params)
	if err != nil {
		return nil, err
	}
	return object.VersionId, nil
}

func (s *S3) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	})
	if err != nil {
		var opError *smithy.OperationError
		if errors.As(err, &opError) {
			var httpErr *awsV2http.ResponseError
			if errors.As(opError.Err, &httpErr) {
				if httpErr.Response.StatusCode == http.StatusNotFound {
					return nil, ErrNotFound
				}
			}
		}
		return nil, err
	}
	return &s3File{head.ContentLength, *head.LastModified, key}, nil
}

func (s *S3) Walk(ctx context.Context, s3Path string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	g, ctx := errgroup.WithContext(ctx)
	s3Files := make(chan *s3File)
	g.Go(func() error {
		defer close(s3Files)
		return s.remotePager(ctx, path.Join(s.Config.Path, s3Path), recursive, func(page *s3.ListObjectsV2Output) {
			for _, cp := range page.CommonPrefixes {
				s3Files <- &s3File{
					name: strings.TrimPrefix(*cp.Prefix, path.Join(s.Config.Path, s3Path)),
				}
			}
			for _, c := range page.Contents {
				s3Files <- &s3File{
					c.Size,
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

func (s *S3) remotePager(ctx context.Context, s3Path string, recursive bool, process func(page *s3.ListObjectsV2Output)) error {
	prefix := s3Path + "/"
	if s3Path == "" || s3Path == "/" {
		prefix = ""
	}
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Config.Bucket), // Required
		MaxKeys: 1000,
		Prefix:  aws.String(prefix),
	}
	if !recursive {
		params.Delimiter = aws.String("/")
	}
	pager := s3.NewListObjectsV2Paginator(s.client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 1000
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}
		process(page)
	}
	return nil
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
