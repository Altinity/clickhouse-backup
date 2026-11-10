package storage

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestS3DeleteBatchVersioned(t *testing.T) {
	for _, outcome := range []string{"present", "already_absent", "listing_denied"} {
		t.Run(outcome, func(t *testing.T) {
			var mu sync.Mutex
			var deleted []string
			var deleteCalls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Has("versioning"):
					_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`))
				case r.Method == http.MethodGet && r.URL.Query().Has("versions"):
					if outcome == "listing_denied" {
						w.WriteHeader(http.StatusForbidden)
						_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`))
						return
					}
					key := r.URL.Query().Get("prefix")
					_, _ = fmt.Fprint(w, `<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
					if outcome == "present" {
						_, _ = fmt.Fprintf(w, `<Version><Key>%s</Key><VersionId>v1</VersionId></Version><DeleteMarker><Key>%s</Key><VersionId>marker</VersionId></DeleteMarker>`, key, key)
					}
					// A prefix match is not enough: this belongs to a different key.
					_, _ = fmt.Fprintf(w, `<Version><Key>%s-suffix</Key><VersionId>unrelated</VersionId></Version></ListVersionsResult>`, key)
				case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
					var request struct {
						Objects []struct{ Key, VersionId string } `xml:"Object"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Errorf("decode delete: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					deleteCalls++
					for _, object := range request.Objects {
						deleted = append(deleted, object.Key+":"+object.VersionId)
					}
					mu.Unlock()
					_, _ = w.Write([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`))
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			s3 := &S3{Config: &config.S3Config{
				Endpoint: server.URL, Region: "us-east-1", Bucket: "test-bucket",
				AccessKey: "test-access-key", SecretKey: "test-secret-key", ForcePathStyle: true,
				Path: "native", DeleteConcurrency: 2,
			}}
			require.NoError(t, s3.Connect(context.Background()))
			defer func() { require.NoError(t, s3.Close(context.Background())) }()
			err := s3.DeleteKeysBatch(context.Background(), []string{"a", "b"})
			if outcome == "listing_denied" {
				require.ErrorContains(t, err, "AccessDenied")
			} else {
				require.NoError(t, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if outcome == "present" {
				require.Equal(t, 1, deleteCalls)
				require.ElementsMatch(t, []string{"native/a:v1", "native/a:marker", "native/b:v1", "native/b:marker"}, deleted)
			} else {
				require.Zero(t, deleteCalls, "do not add a delete marker for absent objects or after a version-list error")
				require.Empty(t, deleted)
			}
		})
	}
}
