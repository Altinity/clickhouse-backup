package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsV2Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	awsV2Logging "github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

func (adapter S3LogToZeroLogAdapter) Logf(severity awsV2Logging.Classification, msg string, args ...interface{}) {
	msg = fmt.Sprintf("[s3:%s] %s", severity, msg)
	if len(args) > 0 {
		adapter.logger.Info().Msgf(msg, args...)
	} else {
		adapter.logger.Info().Msg(msg)
	}
}

// RecalculateV4Signature allow GCS over S3, remove Accept-Encoding header from sign https://stackoverflow.com/a/74382598/1204665, https://github.com/aws/aws-sdk-go-v2/issues/1816
type RecalculateV4Signature struct {
	next      http.RoundTripper
	signer    *v4.Signer
	awsConfig aws.Config
}

func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error) {
	// store for later use
	acceptEncodingValue := req.Header.Get("Accept-Encoding")

	// delete the header so the header doesn't account for in the signature
	req.Header.Del("Accept-Encoding")

	// sign with the same date
	timeString := req.Header.Get("X-Amz-Date")
	timeDate, _ := time.Parse("20060102T150405Z", timeString)

	creds, err := lt.awsConfig.Credentials.Retrieve(req.Context())
	if err != nil {
		return nil, err
	}
	err = lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.awsConfig.Region, timeDate)
	if err != nil {
		return nil, err
	}
	// Reset Accept-Encoding if desired
	req.Header.Set("Accept-Encoding", acceptEncodingValue)

	// follows up the original round tripper
	return lt.next.RoundTrip(req)
}

// S3 - presents methods for manipulate data on s3
type S3 struct {
	client      *s3.Client
	Config      *config.S3Config
	Concurrency int
	BufferSize  int
	versioning  bool
}

func (s *S3) Kind() string {

	return "S3"
}

func (s *S3) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (endpoint smithyendpoints.Endpoint, err error) {
	baseResolver := s3.NewDefaultEndpointResolverV2()
	if s.Config.Endpoint != "" {
		params.Endpoint = &s.Config.Endpoint
	}
	params.ForcePathStyle = &s.Config.ForcePathStyle

	resolvedEndpoint, err := baseResolver.ResolveEndpoint(ctx, params)
	if err != nil {
		return resolvedEndpoint, err
	}
	return resolvedEndpoint, nil
}

// Connect - connect to s3
func (s *S3) Connect(ctx context.Context) error {
	var err error
	var awsConfig aws.Config
	awsConfig, err = awsV2Config.LoadDefaultConfig(
		ctx,
		awsV2Config.WithRetryMode(aws.RetryModeStandard),
	)
	if err != nil {
		return err
	}
	if s.Config.Region != "" {
		awsConfig.Region = s.Config.Region
	}
	// AWS IRSA handling, look https://github.com/Altinity/clickhouse-backup/issues/798
	awsRoleARN := os.Getenv("AWS_ROLE_ARN")
	awsWebIdentityTokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	stsClient := sts.NewFromConfig(awsConfig)
	if awsRoleARN != "" && awsWebIdentityTokenFile != "" {
		awsConfig.Credentials = stscreds.NewWebIdentityRoleProvider(
			stsClient, awsRoleARN, stscreds.IdentityTokenFile(awsWebIdentityTokenFile),
		)
		// inherit IRSA and try assume role https://github.com/Altinity/clickhouse-backup/issues/1191
		if s.Config.AssumeRoleARN != "" && s.Config.AssumeRoleARN != awsRoleARN {
			stsClient = sts.NewFromConfig(awsConfig)
			awsConfig.Credentials = stscreds.NewAssumeRoleProvider(stsClient, s.Config.AssumeRoleARN)
		}
	} else if s.Config.AssumeRoleARN != "" {
		// backup role S3_ASSUME_ROLE_ARN have high priority than AWS_ROLE_ARN see https://github.com/Altinity/clickhouse-backup/issues/898
		awsConfig.Credentials = stscreds.NewAssumeRoleProvider(stsClient, s.Config.AssumeRoleARN)
	} else if awsRoleARN != "" {
		awsConfig.Credentials = stscreds.NewAssumeRoleProvider(stsClient, awsRoleARN)
	}

	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		awsConfig.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     s.Config.AccessKey,
				SecretAccessKey: s.Config.SecretKey,
			},
		}
	}

	if s.Config.Debug {
		awsConfig.Logger = newS3Logger(log.Logger)
		awsConfig.ClientLogMode = aws.LogRetries | aws.LogRequest | aws.LogResponse
	}

	httpTransport := http.DefaultTransport
	if s.Config.DisableCertVerification {
		httpTransport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.HTTPClient = &http.Client{Transport: httpTransport}
	}

	// allow GCS over S3, remove Accept-Encoding header from sign https://stackoverflow.com/a/74382598/1204665, https://github.com/aws/aws-sdk-go-v2/issues/1816
	if strings.Contains(s.Config.Endpoint, "storage.googleapis.com") {
		// Assign custom client with our own transport
		awsConfig.HTTPClient = &http.Client{Transport: &RecalculateV4Signature{httpTransport, v4.NewSigner(func(signer *v4.SignerOptions) {
			signer.DisableURIPathEscaping = true
		}), awsConfig}}
	}
	s.client = s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.UsePathStyle = s.Config.ForcePathStyle
		o.EndpointOptions.DisableHTTPS = s.Config.DisableSSL
		o.EndpointResolverV2 = s
	})

	s.versioning = s.isVersioningEnabled(ctx)

	return nil
}

func (s *S3) Close(ctx context.Context) error {
	return nil
}

func (s *S3) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.GetFileReaderAbsolute(ctx, path.Join(s.Config.Path, key))
}

func (s *S3) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	}
	s.enrichGetObjectParams(params)
	resp, err := s.client.GetObject(ctx, params)
	if err != nil {
		var opError *smithy.OperationError
		if errors.As(err, &opError) {
			var httpErr *smithyhttp.ResponseError
			if errors.As(opError.Err, &httpErr) {
				var stateErr *s3types.InvalidObjectState
				if errors.As(httpErr, &stateErr) {
					if strings.Contains(string(stateErr.StorageClass), "GLACIER") {
						log.Warn().Msgf("GetFileReader %s, storageClass %s receive error: %s", key, stateErr.StorageClass, stateErr.Error())
						if restoreErr := s.restoreObject(ctx, key); restoreErr != nil {
							log.Warn().Msgf("restoreObject %s, return error: %v", key, restoreErr)
							return nil, err
						}
						if resp, err = s.client.GetObject(ctx, params); err != nil {
							log.Warn().Msgf("second GetObject %s, return error: %v", key, err)
							return nil, err
						}
						return resp.Body, nil
					}
				}
			}
			return nil, err
		}
		return nil, err
	}
	return resp.Body, nil
}

func (s *S3) enrichGetObjectParams(params *s3.GetObjectInput) {
	if s.Config.SSECustomerAlgorithm != "" {
		params.SSECustomerAlgorithm = aws.String(s.Config.SSECustomerAlgorithm)
	}
	if s.Config.SSECustomerKey != "" {
		params.SSECustomerKey = aws.String(s.Config.SSECustomerKey)
	}
	if s.Config.SSECustomerKeyMD5 != "" {
		params.SSECustomerKeyMD5 = aws.String(s.Config.SSECustomerKeyMD5)
	}
	if s.Config.RequestPayer != "" {
		params.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
}

func (s *S3) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	/* unfortunately, multipart download require allocate additional disk space
	and don't allow us to decompress data directly from stream */
	if s.Config.AllowMultipartDownload {
		writer, err := os.CreateTemp(localPath, strings.ReplaceAll(key, "/", "_"))
		if err != nil {
			return nil, err
		}

		downloader := s3manager.NewDownloader(s.client)
		downloader.Concurrency = s.Concurrency
		downloader.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(s.BufferSize)
		var partSize int64
		if s.Config.ChunkSize > 0 && (remoteSize+s.Config.ChunkSize-1)/s.Config.ChunkSize < s.Config.MaxPartsCount {
			// Use configured chunk size
			partSize = s.Config.ChunkSize
		} else {
			partSize = remoteSize / s.Config.MaxPartsCount
			if remoteSize%s.Config.MaxPartsCount > 0 {
				partSize += max(1, (remoteSize%s.Config.MaxPartsCount)/s.Config.MaxPartsCount)
			}
		}
		downloader.PartSize = AdjustValueByRange(partSize, 5*1024*1024, 5*1024*1024*1024)

		_, err = downloader.Download(ctx, writer, &s3.GetObjectInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(path.Join(s.Config.Path, key)),
		})
		if err != nil {
			return nil, err
		}
		return writer, nil
	}

	return s.GetFileReader(ctx, key)
}

func (s *S3) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return s.PutFileAbsolute(ctx, path.Join(s.Config.Path, key), r, localSize)
}

func (s *S3) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	params := s3.PutObjectInput{
		Bucket:       aws.String(s.Config.Bucket),
		Key:          aws.String(key),
		Body:         r,
		StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
	}
	if s.Config.CheckSumAlgorithm != "" {
		params.ChecksumAlgorithm = s3types.ChecksumAlgorithm(s.Config.CheckSumAlgorithm)
	}

	// ACL shall be optional, fix https://github.com/Altinity/clickhouse-backup/issues/785
	if s.Config.ACL != "" {
		params.ACL = s3types.ObjectCannedACL(s.Config.ACL)
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
	uploader := s3manager.NewUploader(s.client)
	uploader.Concurrency = s.Concurrency
	uploader.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(s.BufferSize)
	var partSize int64
	if s.Config.ChunkSize > 0 && (localSize+s.Config.ChunkSize-1)/s.Config.ChunkSize < s.Config.MaxPartsCount {
		partSize = s.Config.ChunkSize
	} else {
		partSize = localSize / s.Config.MaxPartsCount
		if localSize%s.Config.MaxPartsCount > 0 {
			partSize += max(1, (localSize%s.Config.MaxPartsCount)/s.Config.MaxPartsCount)
		}
	}
	uploader.PartSize = AdjustValueByRange(partSize, 5*1024*1024, 5*1024*1024*1024)

	_, err := uploader.Upload(ctx, &params)
	return err
}

func (s *S3) deleteKey(ctx context.Context, key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	}
	if s.Config.RequestPayer != "" {
		params.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	if s.versioning {
		objVersions, err := s.getObjectAllVersions(ctx, key)
		if err != nil {
			return errors.Wrapf(err, "deleteKey, obtaining object version bucket: %s key: %s", s.Config.Bucket, key)
		}
		for _, objVersion := range objVersions {
			params.VersionId = &objVersion
			if _, err := s.client.DeleteObject(ctx, params); err != nil {
				return errors.Wrapf(err, "deleteKey, deleting object bucket: %s key: %s version: %v", s.Config.Bucket, key, params.VersionId)
			}
		}
		if len(objVersions) > 0 {
			return nil
		}
	}
	if _, err := s.client.DeleteObject(ctx, params); err != nil {
		return errors.Wrapf(err, "deleteKey, deleting object bucket: %s key: %s version: %v", s.Config.Bucket, key, params.VersionId)
	}
	return nil
}

func (s *S3) DeleteFile(ctx context.Context, key string) error {
	key = path.Join(s.Config.Path, key)
	return s.deleteKey(ctx, key)
}

func (s *S3) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	key = path.Join(s.Config.ObjectDiskPath, key)
	return s.deleteKey(ctx, key)
}

// DeleteKeysBatch implements BatchDeleter interface for S3
// Uses DeleteObjects API to delete up to 1000 keys per request
func (s *S3) DeleteKeysBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Prepend path to all keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = path.Join(s.Config.Path, key)
	}
	return s.deleteKeys(ctx, fullKeys)
}

// DeleteKeysFromObjectDiskBackupBatch implements BatchDeleter interface for S3
func (s *S3) DeleteKeysFromObjectDiskBackupBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// Prepend object disk path to all keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = path.Join(s.Config.ObjectDiskPath, key)
	}
	return s.deleteKeys(ctx, fullKeys)
}

// deleteKeys performs batch deletion using DeleteObjects API
// Uses S3Config.DeleteConcurrency for parallel version listing (for versioned buckets)
// AWS S3 DeleteObjects API limit is 1000 keys per request
func (s *S3) deleteKeys(ctx context.Context, keys []string) error {
	const maxBatchSize = 1000 // AWS S3 limit
	concurrency := s.Config.DeleteConcurrency

	// Build list of objects to delete, handling versioning
	var objectsToDelete []s3types.ObjectIdentifier
	if s.versioning {
		// For versioned buckets, we need to get all versions of each key
		// Use concurrency for version listing
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)
		var mu sync.Mutex

		for _, key := range keys {
			key := key
			g.Go(func() error {
				versions, err := s.getObjectAllVersions(ctx, key)
				if err != nil {
					// If we can't list versions, try deleting without version ID
					log.Warn().Msgf("S3 deleteKeys: can't get versions for %s: %v, will try without version", key, err)
					mu.Lock()
					objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
						Key: aws.String(key),
					})
					mu.Unlock()
					return nil
				}
				mu.Lock()
				if len(versions) == 0 {
					// No versions found, delete without version ID
					objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
						Key: aws.String(key),
					})
				} else {
					// Add each version as a separate object to delete
					for _, version := range versions {
						objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
							Key:       aws.String(key),
							VersionId: aws.String(version),
						})
					}
				}
				mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	} else {
		// Non-versioned: simple key list
		for _, key := range keys {
			objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
				Key: aws.String(key),
			})
		}
	}

	if len(objectsToDelete) == 0 {
		return nil
	}

	// Process in batches of maxBatchSize (AWS limit)
	var allFailures []KeyError
	for i := 0; i < len(objectsToDelete); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}
		batch := objectsToDelete[i:end]

		failures, err := s.executeBatchDelete(ctx, batch)
		if err != nil {
			// Entire batch failed
			return errors.Wrapf(err, "S3 batch delete failed for batch starting at index %d", i)
		}
		allFailures = append(allFailures, failures...)
	}

	if len(allFailures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("S3 batch delete: %d keys deleted, %d failed", len(objectsToDelete)-len(allFailures), len(allFailures)),
			Failures: allFailures,
		}
	}

	log.Debug().Msgf("S3 batch delete: successfully deleted %d objects", len(objectsToDelete))
	return nil
}

// withContentMD5 removes all flexible checksum procedures from an operation,
// instead computing an MD5 checksum for the request payload.
// This is needed for S3-compatible storage that requires Content-MD5 header.
// See https://github.com/aws/aws-sdk-go-v2/discussions/2960
func withContentMD5(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		_, _ = stack.Initialize.Remove("AWSChecksum:SetupInputContext")
		_, _ = stack.Build.Remove("AWSChecksum:RequestMetricsTracking")
		_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
		_, _ = stack.Finalize.Remove("addInputChecksumTrailer")
		return smithyhttp.AddContentChecksumMiddleware(stack)
	})
}

// executeBatchDelete executes a single batch delete operation
func (s *S3) executeBatchDelete(ctx context.Context, objects []s3types.ObjectIdentifier) ([]KeyError, error) {
	params := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.Config.Bucket),
		Delete: &s3types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true), // Only return errors, not successes
		},
	}

	if s.Config.RequestPayer != "" {
		params.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	if s.Config.CheckSumAlgorithm != "" {
		params.ChecksumAlgorithm = s3types.ChecksumAlgorithm(s.Config.CheckSumAlgorithm)
	}

	var output *s3.DeleteObjectsOutput
	var err error
	if s.Config.RequestContentMD5 {
		output, err = s.client.DeleteObjects(ctx, params, withContentMD5)
	} else {
		output, err = s.client.DeleteObjects(ctx, params)
	}
	if err != nil {
		return nil, errors.Wrap(err, "DeleteObjects API call failed")
	}

	// Parse per-object failures
	var failures []KeyError
	for _, delErr := range output.Errors {
		key := aws.ToString(delErr.Key)
		code := aws.ToString(delErr.Code)
		message := aws.ToString(delErr.Message)
		log.Warn().Msgf("S3 batch delete: failed to delete %s: %s - %s", key, code, message)
		failures = append(failures, KeyError{
			Key: key,
			Err: fmt.Errorf("%s: %s", code, message),
		})
	}

	return failures, nil
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

func (s *S3) getObjectAllVersions(ctx context.Context, key string) ([]string, error) {
	listParams := &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.Config.Bucket),
		Prefix: aws.String(key),
	}
	if s.Config.RequestPayer != "" {
		listParams.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	var versions []string
	pager := s3.NewListObjectVersionsPaginator(s.client, listParams)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "listing object versions bucket: %s key: %s", s.Config.Bucket, key)
		}
		for _, version := range page.Versions {
			if *version.Key == key {
				versions = append(versions, *version.VersionId)
			}
		}
	}
	return versions, nil
}

func (s *S3) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return s.StatFileAbsolute(ctx, path.Join(s.Config.Path, key))
}

func (s *S3) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(key),
	}
	s.enrichHeadParams(params)
	head, err := s.client.HeadObject(ctx, params)
	if err != nil {
		var opError *smithy.OperationError
		if errors.As(err, &opError) {
			var httpErr *smithyhttp.ResponseError
			if errors.As(opError.Err, &httpErr) {
				if httpErr.Response.StatusCode == http.StatusNotFound {
					return nil, ErrNotFound
				}
			}
		}
		return nil, err
	}
	return &s3File{*head.ContentLength, *head.LastModified, string(head.StorageClass), key}, nil
}

func (s *S3) Walk(ctx context.Context, s3Path string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	prefix := path.Join(s.Config.Path, s3Path)
	return s.WalkAbsolute(ctx, prefix, recursive, process)
}

func (s *S3) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(ctx context.Context, r RemoteFile) error) error {
	g, ctx := errgroup.WithContext(ctx)
	s3Files := make(chan *s3File)
	g.Go(func() error {
		defer close(s3Files)
		return s.remotePager(ctx, prefix, recursive, func(page *s3.ListObjectsV2Output) {
			for _, cp := range page.CommonPrefixes {
				s3Files <- &s3File{
					name: strings.TrimPrefix(*cp.Prefix, prefix),
				}
			}
			for _, c := range page.Contents {
				s3Files <- &s3File{
					*c.Size,
					*c.LastModified,
					string(c.StorageClass),
					strings.TrimPrefix(*c.Key, prefix),
				}
			}
		})
	})
	g.Go(func() error {
		var err error
		for s3FileItem := range s3Files {
			if err == nil {
				err = process(ctx, s3FileItem)
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
		MaxKeys: aws.Int32(1000),
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

func (s *S3) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	dstKey = path.Join(s.Config.ObjectDiskPath, dstKey)
	log.Debug().Msgf("S3->CopyObject %s/%s -> %s/%s", srcBucket, srcKey, s.Config.Bucket, dstKey)
	// just copy object without multipart
	if srcSize < 5*1024*1024*1024 || strings.Contains(s.Config.Endpoint, "storage.googleapis.com") {
		params := &s3.CopyObjectInput{
			Bucket:       aws.String(s.Config.Bucket),
			Key:          aws.String(dstKey),
			CopySource:   aws.String(path.Join(srcBucket, srcKey)),
			StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
		}
		s.enrichCopyObjectParams(params)
		_, err := s.client.CopyObject(ctx, params)
		if err != nil {
			return 0, errors.Wrapf(err, "S3->CopyObject %s/%s -> %s/%s return error", srcBucket, srcKey, s.Config.Bucket, dstKey)
		}
		return srcSize, nil
	}
	// Initiate a multipart upload
	createMultipartUploadParams := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(s.Config.Bucket),
		Key:          aws.String(dstKey),
		StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
	}
	s.enrichCreateMultipartUploadParams(createMultipartUploadParams)
	initResp, err := s.client.CreateMultipartUpload(ctx, createMultipartUploadParams)
	if err != nil {
		return 0, errors.Wrapf(err, "S3->CopyObject %s/%s -> %s/%s, CreateMultipartUpload return error", srcBucket, srcKey, s.Config.Bucket, dstKey)
	}

	// Get the upload ID
	uploadID := initResp.UploadId

	// Set the part size (128 MB minimum for CopyObject, or use configured chunk size)
	var partSize int64
	if s.Config.ChunkSize > 0 && (srcSize+s.Config.ChunkSize-1)/s.Config.ChunkSize < s.Config.MaxPartsCount {
		partSize = s.Config.ChunkSize
	} else {
		partSize = srcSize / s.Config.MaxPartsCount
		if srcSize%s.Config.MaxPartsCount > 0 {
			partSize += max(1, (srcSize%s.Config.MaxPartsCount)/s.Config.MaxPartsCount)
		}
	}
	partSize = AdjustValueByRange(partSize, 128*1024*1024, 5*1024*1024*1024)

	// Calculate the number of parts
	numParts := (srcSize + partSize - 1) / partSize

	copyPartErrGroup, ctx := errgroup.WithContext(ctx)
	copyPartErrGroup.SetLimit(s.Config.Concurrency)

	var mu sync.Mutex
	var parts []s3types.CompletedPart

	// Copy each part of the object
	for partNumber := int64(1); partNumber <= numParts; partNumber++ {
		// Calculate the byte range for the part
		start := (partNumber - 1) * partSize
		end := partNumber * partSize
		if end > srcSize {
			end = srcSize
		}
		currentPartNumber := int32(partNumber)

		copyPartErrGroup.Go(func() error {
			// Copy the part
			uploadPartParams := &s3.UploadPartCopyInput{
				Bucket:          aws.String(s.Config.Bucket),
				Key:             aws.String(dstKey),
				CopySource:      aws.String(srcBucket + "/" + srcKey),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end-1)),
				UploadId:        uploadID,
				PartNumber:      aws.Int32(currentPartNumber),
			}
			if s.Config.RequestPayer != "" {
				uploadPartParams.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
			}
			partResp, err := s.client.UploadPartCopy(ctx, uploadPartParams)
			if err != nil {
				return errors.Wrapf(err, "S3->CopyObject %s/%s -> %s/%s, UploadPartCopy start=%d, end=%d return error", srcBucket, srcKey, s.Config.Bucket, dstKey, start, end-1)
			}
			mu.Lock()
			parts = append(parts, s3types.CompletedPart{
				ETag:           partResp.CopyPartResult.ETag,
				PartNumber:     aws.Int32(currentPartNumber),
				ChecksumCRC32:  partResp.CopyPartResult.ChecksumCRC32,
				ChecksumCRC32C: partResp.CopyPartResult.ChecksumCRC32C,
				ChecksumSHA1:   partResp.CopyPartResult.ChecksumSHA1,
				ChecksumSHA256: partResp.CopyPartResult.ChecksumSHA256,
			})
			mu.Unlock()
			return nil
		})
	}
	if wgWaitErr := copyPartErrGroup.Wait(); wgWaitErr != nil {
		abortParams := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.Config.Bucket),
			Key:      aws.String(dstKey),
			UploadId: uploadID,
		}
		if s.Config.RequestPayer != "" {
			abortParams.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
		}
		_, abortErr := s.client.AbortMultipartUpload(context.Background(), abortParams)
		if abortErr != nil {
			return 0, errors.Wrapf(wgWaitErr, "aborting CopyObject multipart upload: %v, original error was", abortErr)
		}
		return 0, errors.Wrap(wgWaitErr, "one of CopyObject/Multipart go-routine return error")
	}
	// Parts must be ordered by part number.
	sort.Slice(parts, func(i int, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})
	// Complete the multipart upload
	completeMultipartUploadParams := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.Config.Bucket),
		Key:             aws.String(dstKey),
		UploadId:        uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	}
	if s.Config.RequestPayer != "" {
		completeMultipartUploadParams.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	_, err = s.client.CompleteMultipartUpload(context.Background(), completeMultipartUploadParams)
	if err != nil {
		return 0, errors.Wrap(err, "complete CopyObject multipart upload")
	}
	return srcSize, nil
}

func (s *S3) enrichCreateMultipartUploadParams(params *s3.CreateMultipartUploadInput) {
	if s.Config.CheckSumAlgorithm != "" {
		params.ChecksumAlgorithm = s3types.ChecksumAlgorithm(s.Config.CheckSumAlgorithm)
	}
	if s.Config.RequestPayer != "" {
		params.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
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
}

func (s *S3) enrichCopyObjectParams(params *s3.CopyObjectInput) {
	if s.Config.CheckSumAlgorithm != "" {
		params.ChecksumAlgorithm = s3types.ChecksumAlgorithm(s.Config.CheckSumAlgorithm)
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
	if s.Config.RequestPayer != "" {
		params.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
}

func (s *S3) restoreObject(ctx context.Context, key string) error {
	restoreRequest := &s3.RestoreObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
		RestoreRequest: &s3types.RestoreRequest{
			Days: aws.Int32(1),
			GlacierJobParameters: &s3types.GlacierJobParameters{
				Tier: s3types.Tier("Expedited"),
			},
		},
	}
	if s.Config.RequestPayer != "" {
		restoreRequest.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	_, err := s.client.RestoreObject(ctx, restoreRequest)
	if err != nil {
		return err
	}
	i := 0
	for {
		restoreHeadParams := &s3.HeadObjectInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(path.Join(s.Config.Path, key)),
		}
		s.enrichHeadParams(restoreHeadParams)
		res, err := s.client.HeadObject(ctx, restoreHeadParams)
		if err != nil {
			return fmt.Errorf("restoreObject: failed to head %s object metadata, %v", path.Join(s.Config.Path, key), err)
		}

		if res.Restore != nil && *res.Restore == "ongoing-request=\"true\"" {
			i += 1
			log.Warn().Msgf("%s still not restored, will wait %d seconds", key, i*5)
			time.Sleep(time.Duration(i*5) * time.Second)
		} else {
			return nil
		}
	}
}

func (s *S3) enrichHeadParams(headParams *s3.HeadObjectInput) {
	if s.Config.RequestPayer != "" {
		headParams.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	if s.Config.SSECustomerAlgorithm != "" {
		headParams.SSECustomerAlgorithm = aws.String(s.Config.SSECustomerAlgorithm)
	}
	if s.Config.SSECustomerKey != "" {
		headParams.SSECustomerKey = aws.String(s.Config.SSECustomerKey)
	}
	if s.Config.SSECustomerKeyMD5 != "" {
		headParams.SSECustomerKeyMD5 = aws.String(s.Config.SSECustomerKeyMD5)
	}
}

type s3File struct {
	size         int64
	lastModified time.Time
	storageClass string
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

func (f *s3File) StorageClass() string {
	return f.storageClass
}
