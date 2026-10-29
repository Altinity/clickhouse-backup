package cas

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/Altinity/clickhouse-backup/v2/pkg/checksumstxt"
)

// Hash128 is an alias for the parser's hash type so CAS callers don't need
// two imports.
type Hash128 = checksumstxt.Hash128

// hashHex returns the 32-char lowercase hex representation. Byte order: the
// 16 bytes are emitted as Low (8 bytes little-endian) followed by High (8
// bytes little-endian). This convention is CAS-internal (write and read both
// use this function); it does not need to match any other system's hex
// representation.
func hashHex(h Hash128) string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], h.Low)
	binary.LittleEndian.PutUint64(b[8:16], h.High)
	return hex.EncodeToString(b[:])
}

// ShardPrefix returns the 2-char shard segment of the blob path.
func ShardPrefix(h Hash128) string {
	return hashHex(h)[:2]
}

// BlobPath returns the full object key for a blob. clusterPrefix MUST already
// end with "/" (it is the value of cas.Config.ClusterPrefix()).
func BlobPath(clusterPrefix string, h Hash128) string {
	s := hashHex(h)
	return clusterPrefix + "blob/" + s[:2] + "/" + s[2:]
}
