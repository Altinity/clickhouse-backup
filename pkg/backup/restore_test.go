package backup

import (
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDetectRBACObject(t *testing.T) {
	b := &Backuper{} // Create an instance of Backuper for testing

	testCases := []struct {
		inputSQL     string
		expectedKind string
		expectedName string
		expectedErr  error
	}{
		{
			inputSQL:     "ATTACH ROLE `admin`",
			expectedKind: "ROLE",
			expectedName: "admin",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH USER `user1` WITH PASSWORD 'password'",
			expectedKind: "USER",
			expectedName: "user1",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH ROLE test_rbac SETTINGS PROFILE ID('4949fb42-97bb-4841-4b5b-c05d4b0cb685');\n",
			expectedKind: "ROLE",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH ROW POLICY `test_rbac` ON default.test_rbac AS restrictive FOR SELECT USING 1 = 1 TO ID('e1469fb8-e014-c22b-4e5c-406134320f91');\n",
			expectedKind: "ROW POLICY",
			expectedName: "`test_rbac` ON default.test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH SETTINGS PROFILE `test_rbac` SETTINGS max_execution_time = 60.;\n",
			expectedKind: "SETTINGS PROFILE",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH QUOTA test_rbac KEYED BY user_name TO ID('e1469fb8-e014-c22b-4e5c-406134320f91');\n",
			expectedKind: "QUOTA",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "ATTACH USER test_rbac IDENTIFIED WITH sha256_hash BY '256A6D6B157C014A70BE5C62ACA0FE4A6183BFBD45895F62287447B55E519BAD' DEFAULT ROLE ID('2d449952-fca4-c9f2-2949-b83880124bbc');\nATTACH GRANT ID('2d449952-fca4-c9f2-2949-b83880124bbc') TO test_rbac;\n",
			expectedKind: "USER",
			expectedName: "test_rbac",
			expectedErr:  nil,
		},
		{
			inputSQL:     "INVALID SQL",
			expectedKind: "",
			expectedName: "",
			expectedErr:  fmt.Errorf("unable to detect RBAC object kind from SQL query: INVALID SQL"),
		},
		{
			inputSQL:     "ATTACH USER  ",
			expectedKind: "USER",
			expectedName: "",
			expectedErr:  fmt.Errorf("unable to detect RBAC object name from SQL query: ATTACH USER  "),
		},
	}

	for _, tc := range testCases {
		kind, name, err := b.detectRBACObject(tc.inputSQL)
		assert.Equal(t, tc.expectedKind, kind)
		assert.Equal(t, tc.expectedName, name)
		if tc.expectedName != "" && tc.expectedKind != "" {
			assert.NoError(t, err)
		}
		if err != nil {
			assert.Equal(t, tc.expectedErr.Error(), err.Error())
		}
	}
}

func TestChangeTablePatternFromRestoreMapping(t *testing.T) {
	testCases := []struct {
		name                   string
		tablePattern           string
		objType                string
		restoreDatabaseMapping map[string]string
		restoreTableMapping    map[string]string
		expected               string
	}{
		// Database mapping tests
		{
			name:                   "database mapping with matching pattern",
			tablePattern:           "db1.*",
			objType:                "database",
			restoreDatabaseMapping: map[string]string{"db1": "db2"},
			expected:               "db2.*",
		},
		{
			name:                   "database mapping without matching pattern appends",
			tablePattern:           "other.*",
			objType:                "database",
			restoreDatabaseMapping: map[string]string{"db1": "db2"},
			expected:               "other.*,db2.*",
		},
		{
			name:                   "database mapping with empty pattern",
			tablePattern:           "",
			objType:                "database",
			restoreDatabaseMapping: map[string]string{"db1": "db2"},
			expected:               "db2.*",
		},
		{
			name:                   "database mapping with comma-separated patterns",
			tablePattern:           "other.*,db1.table1",
			objType:                "database",
			restoreDatabaseMapping: map[string]string{"db1": "db2"},
			expected:               "other.*,db2.*",
		},
		// Table mapping tests
		{
			name:                "table mapping with matching pattern",
			tablePattern:        "db1.t1",
			objType:             "table",
			restoreTableMapping: map[string]string{"t1": "t2"},
			expected:            "db1.t2",
		},
		{
			name:                "table mapping without matching pattern appends",
			tablePattern:        "db1.other",
			objType:             "table",
			restoreTableMapping: map[string]string{"t1": "t2"},
			expected:            "db1.other,*.t2",
		},
		{
			name:                "table mapping with empty pattern",
			tablePattern:        "",
			objType:             "table",
			restoreTableMapping: map[string]string{"t1": "t2"},
			expected:            "*.t2",
		},
		{
			name:                "table mapping with comma-separated patterns",
			tablePattern:        "db1.other,db2.t1",
			objType:             "table",
			restoreTableMapping: map[string]string{"t1": "t2"},
			expected:            "db1.other,db2.t2",
		},
		{
			name:                "table mapping preserves database name",
			tablePattern:        "mydb.t1",
			objType:             "table",
			restoreTableMapping: map[string]string{"t1": "renamed_t1"},
			expected:            "mydb.renamed_t1",
		},
		// Default/unknown objType tests
		{
			name:                   "unknown objType returns pattern unchanged",
			tablePattern:           "db1.*",
			objType:                "unknown",
			restoreDatabaseMapping: map[string]string{"db1": "db2"},
			restoreTableMapping:    map[string]string{"t1": "t2"},
			expected:               "db1.*",
		},
		{
			name:                   "empty objType returns pattern unchanged",
			tablePattern:           "db1.*",
			objType:                "",
			restoreDatabaseMapping: map[string]string{"db1": "db2"},
			expected:               "db1.*",
		},
		// Multiple mappings tests
		{
			name:                   "multiple database mappings",
			tablePattern:           "db1.*,db3.*",
			objType:                "database",
			restoreDatabaseMapping: map[string]string{"db1": "db2", "db3": "db4"},
			expected:               "db2.*,db4.*",
		},
		{
			name:                "multiple table mappings",
			tablePattern:        "db1.t1,db2.t3",
			objType:             "table",
			restoreTableMapping: map[string]string{"t1": "t2", "t3": "t4"},
			expected:            "db1.t2,db2.t4",
		},
		// Empty mapping tests
		{
			name:                   "empty database mapping returns pattern unchanged",
			tablePattern:           "db1.*",
			objType:                "database",
			restoreDatabaseMapping: map[string]string{},
			expected:               "db1.*",
		},
		{
			name:                "empty table mapping returns pattern unchanged",
			tablePattern:        "db1.t1",
			objType:             "table",
			restoreTableMapping: map[string]string{},
			expected:            "db1.t1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				General: config.GeneralConfig{
					RestoreDatabaseMapping: tc.restoreDatabaseMapping,
					RestoreTableMapping:    tc.restoreTableMapping,
				},
			}
			b := &Backuper{cfg: cfg}
			result := b.changeTablePatternFromRestoreMapping(tc.tablePattern, tc.objType)
			assert.Equal(t, tc.expected, result)
		})
	}
}
