package backup

import (
	"os"
	"path"
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/keeper"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/881
func TestConvertKeeperDumpToLocalSQL(t *testing.T) {
	tempDir := t.TempDir()
	accessPath := path.Join(tempDir, "access")
	require.NoError(t, os.Mkdir(accessPath, 0750))
	jsonLFile := path.Join(tempDir, "replicated.jsonl")
	// root node, `uuid` node, a user entity, its `U/<name>` index node,
	// an ignored entity and a legacy DumpNodeString formatted entity
	content := `{"path":"","value":null}
{"path":"uuid","value":null}
{"path":"uuid/2d449952-fca4-c9f2-2949-b83880124bbc","value":"QVRUQUNIIFVTRVIgdGVzdDsK"}
{"path":"U/test","value":"MmQ0NDk5NTItZmNhNC1jOWYyLTI5NDktYjgzODgwMTI0YmJj"}
{"path":"uuid/11111111-1111-1111-1111-111111111111","value":"QVRUQUNIIFJPTEUgaWdub3JlZDsK"}
{"path":"uuid/22222222-2222-2222-2222-222222222222","value":"ATTACH ROLE legacy;\n"}
`
	require.NoError(t, os.WriteFile(jsonLFile, []byte(content), 0600))

	b := &Backuper{ch: &clickhouse.ClickHouse{}}
	disks := []clickhouse.Disk{{Name: "default", Path: tempDir}}
	ignored := map[string]struct{}{"11111111-1111-1111-1111-111111111111": {}}

	converted, err := b.convertKeeperDumpToLocalSQL(jsonLFile, accessPath, ignored, disks)
	require.NoError(t, err)
	assert.Equal(t, 2, converted)

	sqlFiles, globErr := os.ReadDir(accessPath)
	require.NoError(t, globErr)
	assert.Len(t, sqlFiles, 2)

	userSQL, readErr := os.ReadFile(path.Join(accessPath, "2d449952-fca4-c9f2-2949-b83880124bbc.sql"))
	require.NoError(t, readErr)
	assert.Equal(t, "ATTACH USER test;\n", string(userSQL))

	legacySQL, readErr := os.ReadFile(path.Join(accessPath, "22222222-2222-2222-2222-222222222222.sql"))
	require.NoError(t, readErr)
	assert.Equal(t, "ATTACH ROLE legacy;\n", string(legacySQL))

	_, statErr := os.Stat(path.Join(accessPath, "11111111-1111-1111-1111-111111111111.sql"))
	assert.True(t, os.IsNotExist(statErr), "ignored uuid must not be converted")
}

// https://github.com/Altinity/clickhouse-backup/issues/881
func TestPlanKeeperNodesForSQL(t *testing.T) {
	b := &Backuper{}
	const uuid = "2d449952-fca4-c9f2-2949-b83880124bbc"
	testCases := []struct {
		name         string
		sql          string
		expectedPath string
	}{
		{
			name:         "user",
			sql:          "ATTACH USER `test.rbac-name` IDENTIFIED WITH sha256_hash BY 'hash';\n",
			expectedPath: "U/test%2Erbac%2Dname",
		},
		{
			name:         "role",
			sql:          "ATTACH ROLE `test.rbac-name`;\n",
			expectedPath: "R/test%2Erbac%2Dname",
		},
		{
			name:         "settings profile",
			sql:          "ATTACH SETTINGS PROFILE `test.rbac-name` SETTINGS max_execution_time = 60;\n",
			expectedPath: "S/test%2Erbac%2Dname",
		},
		{
			name:         "quota",
			sql:          "ATTACH QUOTA `test.rbac-name` KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS;\n",
			expectedPath: "Q/test%2Erbac%2Dname",
		},
		{
			name:         "row policy",
			sql:          "ATTACH ROW POLICY `test.rbac-name` ON test_rbac.test_rbac AS restrictive FOR SELECT USING v >= 0;\n",
			expectedPath: "P/%60test%2Erbac%2Dname%60%20ON%20test_rbac%2Etest_rbac",
		},
		{
			name:         "masking policy",
			sql:          "ATTACH MASKING POLICY `test.rbac-name` ON test_rbac.test_rbac USING sha256(v);\n",
			expectedPath: "M/%60test%2Erbac%2Dname%60%20ON%20test_rbac%2Etest_rbac",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodes, err := b.planKeeperNodesForSQL(uuid, tc.sql)
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			assert.Equal(t, keeper.DumpNode{Path: "uuid/" + uuid, Value: []byte(tc.sql)}, nodes[0])
			assert.Equal(t, keeper.DumpNode{Path: tc.expectedPath, Value: []byte(uuid)}, nodes[1])
		})
	}
}

func TestPlanKeeperNodesForSQLUndetectable(t *testing.T) {
	b := &Backuper{}
	_, err := b.planKeeperNodesForSQL("2d449952-fca4-c9f2-2949-b83880124bbc", "ATTACH SOMETHING unknown;\n")
	assert.Error(t, err)
}
