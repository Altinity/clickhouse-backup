package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/943
// credentials of a `SETTINGS disk = disk(...)` table exist only in its DDL, the parser is the only way to get them

func TestParseCustomDisk_NoCustomDisk(t *testing.T) {
	r := require.New(t)
	queries := []string{
		"CREATE TABLE default.t (`id` UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		"CREATE TABLE default.t (`id` UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'jbod', index_granularity = 8192",
		"",
	}
	for _, query := range queries {
		r.False(HasCustomDisk(query), query)
		disk, err := ParseCustomDisk(query)
		r.NoError(err, query)
		r.Nil(disk, query)
	}
}

func TestParseCustomDisk_PlainS3(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_s3 (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/t_s3/', " +
		"access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key', " +
		"metadata_path = '/var/lib/clickhouse/disks/s3_disk/t_s3/'), index_granularity = 8192"

	r.True(HasCustomDisk(query))
	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.NotNil(disk)
	r.Nil(disk.Nested)
	r.Equal("s3", disk.Type())
	r.Equal(disk, disk.Leaf())
	r.Equal(map[string]string{
		"type":              "s3",
		"endpoint":          "http://minio943:9000/bucket/t_s3/",
		"access_key_id":     "access_key",
		"secret_access_key": "it_is_my_super_secret_key",
		"metadata_path":     "/var/lib/clickhouse/disks/s3_disk/t_s3/",
	}, disk.Args)
	r.Equal([]string{"type", "endpoint", "access_key_id", "secret_access_key", "metadata_path"}, disk.Order)
}

func TestParseCustomDisk_CacheOverS3(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_cache (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = cache, max_size = '1Gi', path = '/var/lib/clickhouse/caches/t_cache/', " +
		"disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/t_cache/', " +
		"access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key', " +
		"metadata_path = '/var/lib/clickhouse/disks/s3_disk/t_cache/')), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.NotNil(disk)
	r.Equal("cache", disk.Type())
	r.Equal("1Gi", disk.Args["max_size"])
	r.Equal("/var/lib/clickhouse/caches/t_cache/", disk.Args["path"])
	r.NotContains(disk.Args, "disk", "a nested disk(...) must not leak into Args")
	r.Equal([]string{"type", "max_size", "path"}, disk.Order)

	leaf := disk.Leaf()
	r.NotNil(disk.Nested)
	r.Equal(disk.Nested, leaf)
	r.Equal("s3", leaf.Type())
	r.Equal("http://minio943:9000/bucket/t_cache/", leaf.Args["endpoint"])
	r.Equal("it_is_my_super_secret_key", leaf.Args["secret_access_key"])
	r.Equal("/var/lib/clickhouse/disks/s3_disk/t_cache/", leaf.Args["metadata_path"])
}

func TestParseCustomDisk_EncryptedOverS3(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_enc (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = encrypted, key = '1234567812345678', path = 'enc/', " +
		"disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/t_enc/', " +
		"access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key')), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("encrypted", disk.Type())
	r.Equal("1234567812345678", disk.Args["key"])
	r.Equal("enc/", disk.Args["path"])
	r.Equal("s3", disk.Leaf().Type())
	r.Equal("access_key", disk.Leaf().Args["access_key_id"])
}

func TestParseCustomDisk_NamedDisk(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_named (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(name = 'my_named_s3', type = s3, endpoint = 'http://minio943:9000/bucket/t_named/', " +
		"access_key_id = 'access_key', secret_access_key = 'it_is_my_super_secret_key'), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("my_named_s3", disk.Args["name"])
	r.Equal("s3", disk.Type())
}

func TestParseCustomDisk_ReferenceToNamedDisk(t *testing.T) {
	r := require.New(t)
	// `disk = 'name'` / `disk = name` reference an already declared disk, both forms stay in Args
	quoted := "CREATE TABLE default.t_ref (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = cache, max_size = '1Gi', path = '/var/lib/clickhouse/caches/t_ref/', disk = 'my_named_s3'), index_granularity = 8192"
	bare := "CREATE TABLE default.t_ref (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = cache, max_size = '1Gi', path = '/var/lib/clickhouse/caches/t_ref/', disk = my_named_s3), index_granularity = 8192"

	for _, query := range []string{quoted, bare} {
		disk, err := ParseCustomDisk(query)
		r.NoError(err, query)
		r.Nil(disk.Nested, query)
		r.Equal(disk, disk.Leaf(), query)
		r.Equal("my_named_s3", disk.Args["disk"], query)
		r.Equal([]string{"type", "max_size", "path", "disk"}, disk.Order, query)
	}
}

func TestParseCustomDisk_BareIdentifiersAndNumbers(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_bare (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/t_bare/', access_key_id = clickhouse, " +
		"secret_access_key = clickhouse, readonly = 1, s3_max_single_part_upload_size = 33554432), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("clickhouse", disk.Args["access_key_id"])
	r.Equal("clickhouse", disk.Args["secret_access_key"])
	r.Equal("1", disk.Args["readonly"])
	r.Equal("33554432", disk.Args["s3_max_single_part_upload_size"])
}

func TestParseCustomDisk_GenericObjectStorage(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_generic (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = object_storage, object_storage_type = s3, metadata_type = local, " +
		"endpoint = 'http://minio943:9000/bucket/t_generic/', access_key_id = 'access_key', " +
		"secret_access_key = 'it_is_my_super_secret_key'), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("object_storage", disk.Type())
	r.Equal("s3", disk.Args["object_storage_type"])
	r.Equal("local", disk.Args["metadata_type"])
}

func TestParseCustomDisk_Azure(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_azure (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = azure_blob_storage, storage_account_url = 'http://azure:10000/devstoreaccount1', " +
		"container_name = 'azure-disk', account_name = 'devstoreaccount1', account_key = 'Eby8vdM02xNOcqF=='), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("azure_blob_storage", disk.Type())
	r.Equal("http://azure:10000/devstoreaccount1", disk.Args["storage_account_url"])
	r.Equal("azure-disk", disk.Args["container_name"])
	r.Equal("Eby8vdM02xNOcqF==", disk.Args["account_key"])
}

func TestParseCustomDisk_QuotesCommasParenthesesAndEscapes(t *testing.T) {
	r := require.New(t)
	// a comma, a parenthesis and both quote escape forms inside a string literal must not terminate the argument list
	query := "CREATE TABLE default.t_esc (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		`SETTINGS disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/a,b)c/', ` +
		`secret_access_key = 'wi\'th''quotes', region = 'back\\slash', ` +
		`server_side_encryption_kms_encryption_context = '{"a":"b"}'), index_granularity = 8192`

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("http://minio943:9000/bucket/a,b)c/", disk.Args["endpoint"])
	r.Equal(`wi'th'quotes`, disk.Args["secret_access_key"])
	r.Equal(`back\slash`, disk.Args["region"])
	r.Equal(`{"a":"b"}`, disk.Args["server_side_encryption_kms_encryption_context"])
}

func TestParseCustomDisk_MaskedDDL(t *testing.T) {
	r := require.New(t)
	// SHOW CREATE masks every argument except type, disk and name, the DDL must still parse
	query := "CREATE TABLE default.t_cache (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(type = cache, max_size = '[HIDDEN]', path = '[HIDDEN]', " +
		"disk = disk(type = s3, endpoint = '[HIDDEN]', access_key_id = '[HIDDEN]', secret_access_key = '[HIDDEN]')), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("cache", disk.Type())
	r.Equal("[HIDDEN]", disk.Args["max_size"])
	r.Equal("s3", disk.Leaf().Type())
	r.Equal("[HIDDEN]", disk.Leaf().Args["endpoint"])
	r.Equal("[HIDDEN]", disk.Leaf().Args["secret_access_key"])
}

func TestParseCustomDisk_DefaultTypeIsLocal(t *testing.T) {
	r := require.New(t)
	query := "CREATE TABLE default.t_local (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
		"SETTINGS disk = disk(path = '/var/lib/clickhouse/disks/local943/'), index_granularity = 8192"

	disk, err := ParseCustomDisk(query)
	r.NoError(err)
	r.Equal("local", disk.Type())
}

func TestParseCustomDisk_Malformed(t *testing.T) {
	r := require.New(t)
	queries := map[string]string{
		"unterminated argument list": "SETTINGS disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/'",
		"unterminated string":        "SETTINGS disk = disk(type = s3, endpoint = 'http://minio943:9000/bucket/)",
		"missing equals":             "SETTINGS disk = disk(type s3)",
		"positional argument":        "SETTINGS disk = disk('s3')",
		"missing value":              "SETTINGS disk = disk(type = )",
		"unterminated nested":        "SETTINGS disk = disk(type = cache, disk = disk(type = s3, endpoint = 'x'), index_granularity = 8192",
	}
	for name, query := range queries {
		disk, err := ParseCustomDisk(query)
		r.Error(err, name)
		r.Nil(disk, name)
	}
}
