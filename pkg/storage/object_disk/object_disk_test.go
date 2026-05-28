package object_disk

import (
	"crypto/md5"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1374
// ClickHouse object_disk source carries only the base64 customer key in
// storage.xml. The S3 source client must enrich it with algorithm = AES256
// and SSECustomerKeyMD5 = base64(md5(raw_key)); without that, HeadObject
// against an SSE-C encrypted object returns 400 Bad Request.
func TestBuildS3SSECustomerHeaders_DerivesAlgorithmAndMD5(t *testing.T) {
	r := require.New(t)

	rawKey := []byte(strings.Repeat("k", 32)) // 32 bytes = AES-256
	b64Key := base64.StdEncoding.EncodeToString(rawKey)
	expectedMD5Sum := md5.Sum(rawKey)
	expectedMD5 := base64.StdEncoding.EncodeToString(expectedMD5Sum[:])

	algo, key, keyMD5, err := BuildS3SSECustomerHeaders(b64Key)

	r.NoError(err)
	r.Equal("AES256", algo, "ClickHouse only supports AES256 for SSE-C")
	r.Equal(b64Key, key, "passed-through base64 key must be returned unchanged")
	r.Equal(expectedMD5, keyMD5, "key MD5 must be base64(md5(raw_key))")
}

func TestBuildS3SSECustomerHeaders_RejectsInvalidBase64(t *testing.T) {
	r := require.New(t)

	_, _, _, err := BuildS3SSECustomerHeaders("not-base64!!!")
	r.Error(err)
}

// Sanity check: the MD5 we produce matches the exact value AWS computes for a
// known fixture (32 zero bytes), so a wrong implementation does not silently
// pass.
func TestBuildS3SSECustomerHeaders_KnownFixture(t *testing.T) {
	r := require.New(t)

	zero32 := make([]byte, 32)
	b64Key := base64.StdEncoding.EncodeToString(zero32)

	algo, key, keyMD5, err := BuildS3SSECustomerHeaders(b64Key)

	r.NoError(err)
	r.Equal("AES256", algo)
	r.Equal(b64Key, key)
	// md5(32×0x00) = 70bc8f4b72a86921468bf8e8441dce51
	r.Equal("cLyPS3KoaSFGi/joRB3OUQ==", keyMD5)
}
