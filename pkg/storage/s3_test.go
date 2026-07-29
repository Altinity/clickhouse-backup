package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type fakeS3MultipartAPI struct {
	uploaded  bytes.Buffer
	completed bool
	aborted   bool
}

func (f *fakeS3MultipartAPI) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("test-upload-id")}, nil
}

func (f *fakeS3MultipartAPI) UploadPart(_ context.Context, params *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if _, err := f.uploaded.ReadFrom(params.Body); err != nil {
		return nil, err
	}
	return &s3.UploadPartOutput{
		ETag:          aws.String(fmt.Sprintf("etag-%d", *params.PartNumber)),
		ChecksumCRC32: params.ChecksumCRC32,
	}, nil
}

func (f *fakeS3MultipartAPI) CompleteMultipartUpload(_ context.Context, _ *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.completed = true
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3MultipartAPI) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.aborted = true
	return &s3.AbortMultipartUploadOutput{}, nil
}

// TestPutFileMultipartCRC32ReadsUntilEOF - https://github.com/Altinity/clickhouse-backup/issues/1471
// UploadCompressedStream declares localSize as the sum of raw on-disk file sizes, but the actual
// tar stream is larger (512-byte header per file, 512-byte content padding, 1024-byte trailer).
// The upload must consume the stream until EOF instead of stopping at the declared size,
// otherwise the archive tail is silently truncated.
func TestPutFileMultipartCRC32ReadsUntilEOF(t *testing.T) {
	const partSize = 8 * 1024
	const declaredSize = 3*partSize + 100 // sum of raw file sizes, what UploadCompressedStream computes
	const trueSize = declaredSize + 2560  // + tar headers/padding/trailer

	data := make([]byte, trueSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	fake := &fakeS3MultipartAPI{}
	s := &S3{Config: &config.S3Config{Bucket: "bucket"}}
	params := &s3.PutObjectInput{Bucket: aws.String("bucket"), Key: aws.String("backup/shadow/default/table/default_all_1_1_0.tar")}
	if err := s.putFileMultipartCRC32(context.Background(), fake, params, bytes.NewReader(data), declaredSize, partSize); err != nil {
		t.Fatalf("putFileMultipartCRC32 error: %v", err)
	}
	if !fake.completed {
		t.Fatal("CompleteMultipartUpload was not called")
	}
	if fake.aborted {
		t.Fatal("AbortMultipartUpload was called unexpectedly")
	}
	if !bytes.Equal(fake.uploaded.Bytes(), data) {
		t.Fatalf("uploaded %d bytes, want %d - the tail of the stream was truncated", fake.uploaded.Len(), trueSize)
	}
}

// TestS3CopySource - `x-amz-copy-source` must be URL-encoded, keys with literal `%`/`#`/non-ASCII
// (TablePathEncode names in shadow paths) were decoded server side into a different key, so
// CopyObject failed with NoSuchKey while StatFile on the same key succeeded
func TestS3CopySource(t *testing.T) {
	tests := []struct {
		srcBucket string
		srcKey    string
		want      string
	}{
		{"bucket", "backup/shadow/db/table/default_part.tar", "bucket/backup/shadow/db/table/default_part.tar"},
		{"bucket", "backup/shadow/_test%23%24%2E%D0%94%D0%91/t/default_0_0_0_0.tar", "bucket/backup/shadow/_test%2523%2524%252E%25D0%2594%25D0%2591/t/default_0_0_0_0.tar"},
		{"bucket", "backup/shadow/db#1/part name.tar", "bucket/backup/shadow/db%231/part%20name.tar"},
	}
	for _, tt := range tests {
		if got := s3CopySource(tt.srcBucket, tt.srcKey); got != tt.want {
			t.Fatalf("s3CopySource(%q, %q) = %q, want %q", tt.srcBucket, tt.srcKey, got, tt.want)
		}
	}
}

func TestIsDeleteObjectsMissingContentMD5Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "smithy api error invalid request content md5",
			err: &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "Missing required header for this request: Content-MD5",
			},
			want: true,
		},
		{
			name: "wrapped smithy api error invalid request content md5",
			err: fmt.Errorf("wrapped: %w", &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "Missing required header for this request: Content-MD5",
			}),
			want: true,
		},
		{
			name: "generic wrapped string content md5",
			err:  errors.New("DeleteObjects API call failed: Missing required header for this request: Content-MD5"),
			want: true,
		},
		{
			name: "different invalid request",
			err: &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "Request body malformed",
			},
			want: false,
		},
		{
			name: "different code",
			err: &smithy.GenericAPIError{
				Code:    "AccessDenied",
				Message: "Missing required header for this request: Content-MD5",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDeleteObjectsMissingContentMD5Error(tt.err)
			if got != tt.want {
				t.Fatalf("isDeleteObjectsMissingContentMD5Error() = %v, want %v", got, tt.want)
			}
		})
	}
}
