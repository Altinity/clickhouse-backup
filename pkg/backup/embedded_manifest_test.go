package backup

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// expected values are taken from .backup written by `BACKUP TABLE ... TO File()` in ClickHouse 26.10
func TestEmbeddedChecksum(t *testing.T) {
	r := require.New(t)
	short := "CREATE TABLE d.t UUID 'a031a359-d34c-4931-a1f1-761796d6422d' (`a` UInt64, `s` String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192"
	r.Len(short, 149)
	r.Equal("68c34cffa2b25ea8572cc006d5f4fced", embeddedChecksum([]byte(short)))
	// longer than two 2048 bytes hashing blocks
	long := "CREATE TABLE d.long UUID '99359ff7-bda7-4f05-8e4a-a5e63215646c' (`a` UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192 COMMENT '" + strings.Repeat("a", 5000) + "'"
	r.Len(long, 5151)
	r.Equal("9dc5f8ffbc75f6a9bbaadc26611c2f99", embeddedChecksum([]byte(long)))
	r.Equal("00000000000000000000000000000000", embeddedChecksum(nil))
}

func TestProcessEmbeddedManifest(t *testing.T) {
	r := require.New(t)
	manifest := `<config><version>1</version><deduplicate_files>1</deduplicate_files><contents>` +
		`<file><name>data/d/t/all_1_1_0/count.txt</name><size>6</size><checksum>0123456789abcdef0123456789abcdef</checksum></file>` +
		`<file><name>metadata/d/t.sql</name><size>149</size><checksum>68c34cffa2b25ea8572cc006d5f4fced</checksum></file>` +
		`<file><name>shards/1/replicas/2/metadata/d/t.sql</name><size>149</size><checksum>68c34cffa2b25ea8572cc006d5f4fced</checksum><data_file>metadata/d/t.sql</data_file></file>` +
		`<file><name>metadata/d/base.sql</name><size>10</size><checksum>0123456789abcdef0123456789abcdef</checksum><use_base>true</use_base></file>` +
		`<file><name>metadata/d/empty.sql</name><size>0</size></file>` +
		`</contents></config>`

	seen := map[string]embeddedManifestFile{}
	r.NoError(processEmbeddedManifest(strings.NewReader(manifest), &bytes.Buffer{}, nil, func(dataFile string, f embeddedManifestFile) {
		if embeddedManifestSQLRE.MatchString(dataFile) {
			seen[dataFile] = f
		}
	}))
	// base backup and empty files are skipped, data_file is reported instead of name
	r.Equal(map[string]embeddedManifestFile{"metadata/d/t.sql": {size: "149", checksum: "68c34cffa2b25ea8572cc006d5f4fced"}}, seen)

	out := &bytes.Buffer{}
	changed := map[string]embeddedManifestFile{"metadata/d/t.sql": {size: "200", checksum: "ffffffffffffffffffffffffffffffff"}}
	r.NoError(processEmbeddedManifest(strings.NewReader(manifest), out, changed, nil))
	expected := strings.ReplaceAll(manifest, `<size>149</size><checksum>68c34cffa2b25ea8572cc006d5f4fced</checksum>`, `<size>200</size><checksum>ffffffffffffffffffffffffffffffff</checksum>`)
	r.Equal(expected, out.String())
}
