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

	"github.com/Altinity/clickhouse-backup/pkg/config"
	apexLog "github.com/apex/log"
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
	awsV2Logging "github.com/aws/smithy-go/logging"
	awsV2http "github.com/aws/smithy-go/transport/http"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type S3LogToApexLogAdapter struct {
	apexLog *apexLog.Logger
}

func newS3Logger(log *apexLog.Entry) S3LogToApexLogAdapter {
	return S3LogToApexLogAdapter{
		apexLog: log.Logger,
	}
}

func (S3LogToApexLogAdapter S3LogToApexLogAdapter) Logf(severity awsV2Logging.Classification, msg string, args ...interface{}) {
	msg = fmt.Sprintf("[s3:%s] %s", severity, msg)
	if len(args) > 0 {
		S3LogToApexLogAdapter.apexLog.Infof(msg, args...)
	} else {
		S3LogToApexLogAdapter.apexLog.Info(msg)
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

	if s.Config.AccessKey != "" && s.Config.SecretKey != "" {
		awsConfig.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     s.Config.AccessKey,
				SecretAccessKey: s.Config.SecretKey,
			},
		}
	}

	if s.Config.Debug {
		awsConfig.Logger = newS3Logger(s.Log)
		awsConfig.ClientLogMode = aws.LogRetries | aws.LogRequest | aws.LogResponse
	}

	httpTransport := http.DefaultTransport
	if s.Config.DisableCertVerification {
		httpTransport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsConfig.HTTPClient = &http.Client{Transport: httpTransport}
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
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	s.enrichGetObjectParams(params)
	resp, err := s.client.GetObject(ctx, params)
	if err != nil {
		var opError *smithy.OperationError
		if errors.As(err, &opError) {
			var httpErr *awsV2http.ResponseError
			if errors.As(opError.Err, &httpErr) {
				var stateErr *s3types.InvalidObjectState
				if errors.As(httpErr, &stateErr) {
					if strings.Contains(string(stateErr.StorageClass), "GLACIER") {
						s.Log.Warnf("GetFileReader %s, storageClass %s receive error: %s", key, stateErr.StorageClass, stateErr.Error())
						if restoreErr := s.restoreObject(ctx, key); restoreErr != nil {
							s.Log.Warnf("restoreObject %s, return error: %v", key, restoreErr)
							return nil, err
						}
						if resp, err = s.client.GetObject(ctx, params); err != nil {
							s.Log.Warnf("second GetObject %s, return error: %v", key, err)
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
		Bucket:       aws.String(s.Config.Bucket),
		Key:          aws.String(path.Join(s.Config.Path, key)),
		Body:         r,
		StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
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
	_, err := s.uploader.Upload(ctx, &params)
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
		objVersion, err := s.getObjectVersion(ctx, key)
		if err != nil {
			return errors.Wrapf(err, "deleteKey, obtaining object version bucket: %s key: %s", s.Config.Bucket, key)
		}
		params.VersionId = objVersion
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
		Key:    aws.String(key),
	}
	if s.Config.RequestPayer != "" {
		params.RequestPayer = s3types.RequestPayer(s.Config.RequestPayer)
	}
	object, err := s.client.HeadObject(ctx, params)
	if err != nil {
		return nil, err
	}
	return object.VersionId, nil
}

func (s *S3) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, key)),
	}
	s.enrichHeadParams(params)
	head, err := s.client.HeadObject(ctx, params)
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
	return &s3File{*head.ContentLength, *head.LastModified, string(head.StorageClass), key}, nil
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
					*c.Size,
					*c.LastModified,
					string(c.StorageClass),
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

func (s *S3) CopyObject(ctx context.Context, srcBucket, srcKey, dstKey string) (int64, error) {
	dstKey = path.Join(s.Config.ObjectDiskPath, dstKey)
	if strings.Contains(s.Config.Endpoint, "storage.googleapis.com") {
		params := &s3.CopyObjectInput{
			Bucket:       aws.String(s.Config.Bucket),
			Key:          aws.String(dstKey),
			CopySource:   aws.String(path.Join(srcBucket, srcKey)),
			StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
		}
		s.enrichCopyObjectParams(params)
		_, err := s.client.CopyObject(ctx, params)
		if err != nil {
			return 0, err
		}
		dstHeadParams := &s3.HeadObjectInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(dstKey),
		}
		s.enrichHeadParams(dstHeadParams)
		dstObjResp, err := s.client.HeadObject(ctx, dstHeadParams)
		if err != nil {
			return 0, err
		}
		return *dstObjResp.ContentLength, nil
	}
	// Get the size of the source object
	headParams := &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(srcKey),
	}
	s.enrichHeadParams(headParams)
	sourceObjResp, err := s.client.HeadObject(ctx, headParams)
	if err != nil {
		return 0, err
	}
	srcSize := *sourceObjResp.ContentLength
	// Initiate a multipart upload
	createMultipartUploadParams := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(s.Config.Bucket),
		Key:          aws.String(dstKey),
		StorageClass: s3types.StorageClass(strings.ToUpper(s.Config.StorageClass)),
	}
	s.enrichCreateMultipartUploadParams(createMultipartUploadParams)
	initResp, err := s.client.CreateMultipartUpload(ctx, createMultipartUploadParams)
	if err != nil {
		return 0, err
	}

	// Get the upload ID
	uploadID := initResp.UploadId

	// Set the part size (e.g., 5 MB)
	partSize := srcSize / s.Config.MaxPartsCount
	if srcSize%s.Config.MaxPartsCount > 0 {
		partSize++
	}
	if partSize < 5*1024*1024 {
		partSize = 5 * 1024 * 1024
	}

	// Calculate the number of parts
	numParts := (srcSize + partSize - 1) / partSize

	copyPartSemaphore := semaphore.NewWeighted(int64(s.Config.Concurrency))
	copyPartErrGroup, ctx := errgroup.WithContext(ctx)

	var mu sync.Mutex
	var parts []s3types.CompletedPart

	// Copy each part of the object
	for partNumber := int64(1); partNumber <= numParts; partNumber++ {
		if err := copyPartSemaphore.Acquire(ctx, 1); err != nil {
			apexLog.Errorf("can't acquire semaphore during CopyObject data parts: %v", err)
			break
		}
		// Calculate the byte range for the part
		start := (partNumber - 1) * partSize
		end := partNumber * partSize
		if end > srcSize {
			end = srcSize
		}
		currentPartNumber := int32(partNumber)

		copyPartErrGroup.Go(func() error {
			defer copyPartSemaphore.Release(1)
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
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			parts = append(parts, s3types.CompletedPart{
				ETag:       partResp.CopyPartResult.ETag,
				PartNumber: aws.Int32(currentPartNumber),
			})
			return nil
		})
	}
	if err := copyPartErrGroup.Wait(); err != nil {
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
			return 0, fmt.Errorf("aborting CopyObject multipart upload: %v, original error was: %v", abortErr, err)
		}
		return 0, fmt.Errorf("one of CopyObject go-routine return error: %v", err)
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
		return 0, fmt.Errorf("complete CopyObject multipart upload: %v", err)
	}
	s.Log.Debugf("S3->CopyObject %s/%s -> %s/%s", srcBucket, srcKey, s.Config.Bucket, dstKey)
	return srcSize, nil
}

func (s *S3) enrichCreateMultipartUploadParams(params *s3.CreateMultipartUploadInput) {
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
			s.Log.Warnf("%s still not restored, will wait %d seconds", key, i*5)
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
