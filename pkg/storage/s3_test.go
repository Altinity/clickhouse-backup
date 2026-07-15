package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/aws/smithy-go"
)

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

func TestCommonObjectParams_PreservesAllSSEFields(t *testing.T) {
	s := &S3{Config: &config.S3Config{
		ACL:                     "bucket-owner-full-control",
		SSE:                     "aws:kms",
		SSEKMSKeyId:             "alias/my-key",
		SSECustomerAlgorithm:    "AES256",
		SSECustomerKey:          "raw-key-material",
		SSECustomerKeyMD5:       "key-md5",
		SSEKMSEncryptionContext: "ctx-base64",
		ObjectLabels:            map[string]string{"env": "prod"},
	}}
	p := s.commonObjectParams()

	if p.ACL != "bucket-owner-full-control" {
		t.Errorf("ACL: %q", p.ACL)
	}
	if p.ServerSideEncryption != "aws:kms" {
		t.Errorf("SSE: %q", p.ServerSideEncryption)
	}
	if p.SSEKMSKeyId == nil || *p.SSEKMSKeyId != "alias/my-key" {
		t.Errorf("SSEKMSKeyId: %v", p.SSEKMSKeyId)
	}
	if p.SSECustomerAlgorithm == nil || *p.SSECustomerAlgorithm != "AES256" {
		t.Errorf("SSECustomerAlgorithm: %v", p.SSECustomerAlgorithm)
	}
	if p.SSECustomerKey == nil || *p.SSECustomerKey != "raw-key-material" {
		t.Errorf("SSECustomerKey: %v", p.SSECustomerKey)
	}
	if p.SSECustomerKeyMD5 == nil || *p.SSECustomerKeyMD5 != "key-md5" {
		t.Errorf("SSECustomerKeyMD5: %v", p.SSECustomerKeyMD5)
	}
	if p.SSEKMSEncryptionContext == nil || *p.SSEKMSEncryptionContext != "ctx-base64" {
		t.Errorf("SSEKMSEncryptionContext: %v", p.SSEKMSEncryptionContext)
	}
	if p.Tagging == nil || *p.Tagging != "env=prod" {
		t.Errorf("Tagging: %v", p.Tagging)
	}
}

func TestCommonObjectParams_NilSafe(t *testing.T) {
	s := &S3{Config: &config.S3Config{}} // no fields set
	p := s.commonObjectParams()
	if p.SSEKMSKeyId != nil || p.SSECustomerKey != nil || p.Tagging != nil {
		t.Error("expected all fields to remain unset when config has no values")
	}
}
