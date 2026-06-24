package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
)

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
