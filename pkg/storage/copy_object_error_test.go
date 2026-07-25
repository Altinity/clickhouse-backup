package storage

import (
	"context"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestIsPermanentCopyObjectError(t *testing.T) {
	testcases := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil", nil, false},
		{"sentinel", ErrCopyObjectUnsupported, true},
		{"wrapped sentinel", errors.Wrapf(ErrCopyObjectUnsupported, "CopyObject from bucket b for FTP"), true},
		{"s3 AccessDenied wrapped", errors.Wrapf(&smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}, "S3->CopyObject src -> dst return error"), true},
		{"s3 NotImplemented", &smithy.GenericAPIError{Code: "NotImplemented"}, true},
		{"s3 InvalidRequest", &smithy.GenericAPIError{Code: "InvalidRequest"}, true},
		{"s3 SlowDown", &smithy.GenericAPIError{Code: "SlowDown"}, false},
		{"s3 RequestTimeout", &smithy.GenericAPIError{Code: "RequestTimeout"}, false},
		{"s3 ExpiredToken", &smithy.GenericAPIError{Code: "ExpiredToken"}, false},
		{"gcs 403 wrapped", errors.Wrap(&googleapi.Error{Code: http.StatusForbidden}, "GCS->CopyObject"), true},
		{"gcs 429", &googleapi.Error{Code: http.StatusTooManyRequests}, false},
		{"azure CannotVerifyCopySource", &azcore.ResponseError{ErrorCode: "CannotVerifyCopySource", StatusCode: http.StatusNotFound}, true},
		{"azure 403", &azcore.ResponseError{StatusCode: http.StatusForbidden}, true},
		{"azure 500", &azcore.ResponseError{ErrorCode: "InternalError", StatusCode: http.StatusInternalServerError}, false},
		{"plain network error", errors.New("write tcp 1.2.3.4:1->5.6.7.8:443: use of closed network connection"), false},
		{"retry quota exhausted", errors.New("failed to get rate limit token, retry quota exceeded, 0 available, 5 requested"), false},
		{"context canceled", context.Canceled, false},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, IsPermanentCopyObjectError(tc.err))
		})
	}
}
