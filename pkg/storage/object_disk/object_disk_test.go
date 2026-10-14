package object_disk

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"

	"github.com/antchfx/xmlquery"
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

// https://github.com/Altinity/clickhouse-backup/issues/943
// credentialsFromDiskArgs is shared by the `storage_configuration/disks` XML source and by the
// `SETTINGS disk = disk(...)` table DDL source, the same settings must produce the same credentials.
// version is kept below 23.3 on purpose, so the builder never calls ApplyMacros and needs no live connection.
const credentialsParityVersion = 23002000

// credentialsTestClient - never queried: macro expansion of the endpoint starts at 23.3, credentialsParityVersion is below it
var credentialsTestClient = &clickhouse.ClickHouse{}

func xmlDiskGetter(t *testing.T, diskName, diskXML string) func(key string) (string, bool) {
	t.Helper()
	doc, err := xmlquery.Parse(strings.NewReader(
		"<clickhouse><storage_configuration><disks><" + diskName + ">" + diskXML + "</" + diskName + "></disks></storage_configuration></clickhouse>",
	))
	require.NoError(t, err)
	d := xmlquery.FindOne(doc, "/clickhouse/storage_configuration/disks/"+diskName)
	require.NotNil(t, d)
	return func(key string) (string, bool) {
		node := d.SelectElement(key)
		if node == nil {
			return "", false
		}
		return strings.Trim(node.InnerText(), "\r\n \t"), true
	}
}

func mapGetter(args map[string]string) func(key string) (string, bool) {
	return func(key string) (string, bool) {
		value, exists := args[key]
		return value, exists
	}
}

func TestCredentialsFromDiskArgs_XMLAndSQLParity(t *testing.T) {
	testCases := []struct {
		name    string
		diskXML string
		args    map[string]string
	}{
		{
			name: "s3",
			diskXML: `
				<type>s3</type>
				<endpoint>http://minio:9000/clickhouse/disk_s3/</endpoint>
				<access_key_id>access_key</access_key_id>
				<secret_access_key>it_is_my_super_secret_key</secret_access_key>
				<region>eu-west-1</region>
				<s3_storage_class>INTELLIGENT_TIERING</s3_storage_class>
				<server_side_encryption_customer_key_base64>c3NlLWMta2V5</server_side_encryption_customer_key_base64>
				<server_side_encryption_kms_key_id>kms-key</server_side_encryption_kms_key_id>
				<server_side_encryption_kms_encryption_context>ctx</server_side_encryption_kms_encryption_context>
			`,
			args: map[string]string{
				"type":              "s3",
				"endpoint":          "http://minio:9000/clickhouse/disk_s3/",
				"access_key_id":     "access_key",
				"secret_access_key": "it_is_my_super_secret_key",
				"region":            "eu-west-1",
				"s3_storage_class":  "INTELLIGENT_TIERING",
				"server_side_encryption_customer_key_base64":    "c3NlLWMta2V5",
				"server_side_encryption_kms_key_id":             "kms-key",
				"server_side_encryption_kms_encryption_context": "ctx",
			},
		},
		{
			name: "gcs via support_batch_delete",
			diskXML: `
				<type>s3</type>
				<support_batch_delete>false</support_batch_delete>
				<endpoint>https://storage.googleapis.com/bucket/disk_gcs/</endpoint>
				<access_key_id>access_key</access_key_id>
				<secret_access_key>it_is_my_super_secret_key</secret_access_key>
			`,
			args: map[string]string{
				"type":                 "s3",
				"support_batch_delete": "false",
				"endpoint":             "https://storage.googleapis.com/bucket/disk_gcs/",
				"access_key_id":        "access_key",
				"secret_access_key":    "it_is_my_super_secret_key",
			},
		},
		{
			name: "generic object_storage",
			diskXML: `
				<type>object_storage</type>
				<object_storage_type>s3</object_storage_type>
				<metadata_type>local</metadata_type>
				<endpoint>http://minio:9000/clickhouse/disk_generic/</endpoint>
				<access_key_id>access_key</access_key_id>
				<secret_access_key>it_is_my_super_secret_key</secret_access_key>
			`,
			args: map[string]string{
				"type":                "object_storage",
				"object_storage_type": "s3",
				"metadata_type":       "local",
				"endpoint":            "http://minio:9000/clickhouse/disk_generic/",
				"access_key_id":       "access_key",
				"secret_access_key":   "it_is_my_super_secret_key",
			},
		},
		{
			name: "s3 without keys uses environment",
			diskXML: `
				<type>s3</type>
				<endpoint>http://minio:9000/clickhouse/disk_env/</endpoint>
				<use_environment_credentials>1</use_environment_credentials>
			`,
			args: map[string]string{
				"type":                        "s3",
				"endpoint":                    "http://minio:9000/clickhouse/disk_env/",
				"use_environment_credentials": "1",
			},
		},
		{
			name: "azure",
			diskXML: `
				<type>azure_blob_storage</type>
				<storage_account_url>http://azure:10000/devstoreaccount1</storage_account_url>
				<container_name>azure-disk</container_name>
				<account_name>devstoreaccount1</account_name>
				<account_key>Eby8vdM02xNOcqF==</account_key>
			`,
			args: map[string]string{
				"type":                "azure_blob_storage",
				"storage_account_url": "http://azure:10000/devstoreaccount1",
				"container_name":      "azure-disk",
				"account_name":        "devstoreaccount1",
				"account_key":         "Eby8vdM02xNOcqF==",
			},
		},
		{
			// https://github.com/Altinity/clickhouse-backup/issues/1604
			name: "azure workload identity",
			diskXML: `
				<type>object_storage</type>
				<object_storage_type>azure_blob_storage</object_storage_type>
				<metadata_type>local</metadata_type>
				<storage_account_url>http://azure:10000/devstoreaccount1</storage_account_url>
				<container_name>azure-wi-disk</container_name>
				<account_name>devstoreaccount1</account_name>
				<use_workload_identity>true</use_workload_identity>
			`,
			args: map[string]string{
				"type":                  "object_storage",
				"object_storage_type":   "azure_blob_storage",
				"metadata_type":         "local",
				"storage_account_url":   "http://azure:10000/devstoreaccount1",
				"container_name":        "azure-wi-disk",
				"account_name":          "devstoreaccount1",
				"use_workload_identity": "true",
			},
		},
	}
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::000000000000:role/backup")
	t.Setenv("AWS_ACCESS_KEY_ID", "env_access_key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env_secret_key")
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			xmlCreds, xmlOk, err := credentialsFromDiskArgs(context.Background(), credentialsTestClient, credentialsParityVersion, "xml", xmlDiskGetter(t, "disk_"+strings.ReplaceAll(tc.name, " ", "_"), tc.diskXML))
			r.NoError(err)
			r.True(xmlOk)
			sqlCreds, sqlOk, err := credentialsFromDiskArgs(context.Background(), credentialsTestClient, credentialsParityVersion, "sql", mapGetter(tc.args))
			r.NoError(err)
			r.True(sqlOk)
			r.Equal(xmlCreds, sqlCreds, "XML and `disk = disk(...)` sources must build identical credentials")
			r.NotEmpty(xmlCreds.Type)
			r.NotEmpty(xmlCreds.EndPoint)
		})
	}
}

func TestCredentialsFromDiskArgs_NonObjectDiskTypes(t *testing.T) {
	r := require.New(t)
	for _, args := range []map[string]string{
		{"path": "/var/lib/clickhouse/disks/local/"},
		{"type": "local", "path": "/var/lib/clickhouse/disks/local/"},
		{"type": "cache", "max_size": "1Gi", "path": "/var/lib/clickhouse/caches/c/"},
		{"type": "encrypted", "key": "1234567812345678", "path": "enc/"},
	} {
		creds, ok, err := credentialsFromDiskArgs(context.Background(), credentialsTestClient, credentialsParityVersion, "sql", mapGetter(args))
		r.NoError(err)
		r.False(ok)
		r.Empty(creds.Type)
	}
}

// every endpoint and authentication form which clickhouse-server accepts for azure disks
// https://github.com/Altinity/clickhouse-backup/issues/1604
func TestCredentialsFromDiskArgs_AzureForms(t *testing.T) {
	const key = "Eby8vdM02xNOcqF=="
	testCases := map[string]struct {
		args     map[string]string
		version  int
		expected ObjectStorageCredentials
	}{
		"storage_account_url without account_key": {
			args:     map[string]string{"storage_account_url": "http://azure:10000/devstoreaccount1", "container_name": "c", "account_name": "devstoreaccount1", "use_workload_identity": "true"},
			expected: ObjectStorageCredentials{EndPoint: "http://azure:10000/devstoreaccount1", AzureAccountName: "devstoreaccount1", AzureContainerName: "c"},
		},
		"storage_account_url without account_name": {
			args:     map[string]string{"storage_account_url": "https://acc.blob.core.windows.net", "container": "c"},
			expected: ObjectStorageCredentials{EndPoint: "https://acc.blob.core.windows.net", AzureAccountName: "acc", AzureContainerName: "c"},
		},
		"storage_account_url azurite without account_name": {
			args:     map[string]string{"storage_account_url": "http://azure:10000/devstoreaccount1", "container_name": "c", "account_key": key},
			expected: ObjectStorageCredentials{EndPoint: "http://azure:10000/devstoreaccount1", AzureAccountName: "devstoreaccount1", AzureAccountKey: key, AzureContainerName: "c"},
		},
		"endpoint with prefix and subpath": {
			args:     map[string]string{"endpoint": "http://azure:10000/devstoreaccount1/c/data/", "endpoint_subpath": "sub", "account_name": "devstoreaccount1", "account_key": key, "container_name": "ignored"},
			version:  25001000,
			expected: ObjectStorageCredentials{EndPoint: "http://azure:10000/devstoreaccount1/data/sub", AzureAccountName: "devstoreaccount1", AzureAccountKey: key, AzureContainerName: "c"},
		},
		"endpoint_subpath ignored before 25.1": {
			args:     map[string]string{"endpoint": "http://azure:10000/devstoreaccount1/c/data", "endpoint_subpath": "sub", "account_key": key},
			version:  24008000,
			expected: ObjectStorageCredentials{EndPoint: "http://azure:10000/devstoreaccount1/data", AzureAccountName: "devstoreaccount1", AzureAccountKey: key, AzureContainerName: "c"},
		},
		"endpoint without account_name": {
			args:     map[string]string{"endpoint": "https://acc.blob.core.windows.net/c", "endpoint_contains_account_name": "false"},
			expected: ObjectStorageCredentials{EndPoint: "https://acc.blob.core.windows.net", AzureAccountName: "acc", AzureContainerName: "c"},
		},
		"connection_string with BlobEndpoint": {
			args:     map[string]string{"connection_string": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + key + ";BlobEndpoint=http://azure:10000/devstoreaccount1;", "container_name": "c"},
			expected: ObjectStorageCredentials{EndPoint: "http://azure:10000/devstoreaccount1", AzureAccountName: "devstoreaccount1", AzureAccountKey: key, AzureContainerName: "c"},
		},
		"connection_string with SAS and EndpointSuffix": {
			args:     map[string]string{"connection_string": "AccountName=acc;SharedAccessSignature=sv=2022-11-02&sig=abc%3D;EndpointSuffix=core.chinacloudapi.cn", "container_name": "c"},
			expected: ObjectStorageCredentials{EndPoint: "https://acc.blob.core.chinacloudapi.cn", AzureAccountName: "acc", AzureSAS: "sv=2022-11-02&sig=abc%3D", AzureContainerName: "c"},
		},
		"endpoint has priority over connection_string and storage_account_url": {
			args:     map[string]string{"endpoint": "http://azure:10000/devstoreaccount1/c", "connection_string": "AccountName=other", "storage_account_url": "http://other:10000/other", "account_key": key},
			expected: ObjectStorageCredentials{EndPoint: "http://azure:10000/devstoreaccount1", AzureAccountName: "devstoreaccount1", AzureAccountKey: key, AzureContainerName: "c"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			r := require.New(t)
			tc.args["type"] = "azure_blob_storage"
			if tc.version == 0 {
				tc.version = credentialsParityVersion
			}
			// azure branch never calls ApplyMacros, any version works without a live connection
			creds, ok, err := credentialsFromDiskArgs(context.Background(), credentialsTestClient, tc.version, "unit_test_disk", mapGetter(tc.args))
			r.NoError(err)
			r.True(ok)
			tc.expected.Type = "azblob"
			r.Equal(tc.expected, creds)
		})
	}
}

func TestCredentialsFromDiskArgs_Errors(t *testing.T) {
	r := require.New(t)
	testCases := map[string]map[string]string{
		"s3 without endpoint":               {"type": "s3", "access_key_id": "a", "secret_access_key": "b"},
		"object_storage without type":       {"type": "object_storage", "endpoint": "http://minio:9000/b/"},
		"object_storage bad metadata_type":  {"type": "object_storage", "object_storage_type": "s3", "metadata_type": "keeper", "endpoint": "http://minio:9000/b/"},
		"azure without storage_account_url": {"type": "azure_blob_storage", "container_name": "c"},
		"azure without container_name":      {"type": "azure_blob_storage", "storage_account_url": "http://azure:10000/x"},
		"azure without account_name in url": {"type": "azure_blob_storage", "storage_account_url": "http://azure:10000/", "container_name": "c"},
		"azure endpoint without container":  {"type": "azure_blob_storage", "endpoint": "http://azure:10000/account"},
		"azure connection_string w/o name":  {"type": "azure_blob_storage", "connection_string": "AccountKey=k", "container_name": "c"},
	}
	for name, args := range testCases {
		_, ok, err := credentialsFromDiskArgs(context.Background(), credentialsTestClient, credentialsParityVersion, "unit_test_disk", mapGetter(args))
		r.Error(err, name)
		r.False(ok, name)
		r.Contains(err.Error(), "unit_test_disk", name)
	}
}
