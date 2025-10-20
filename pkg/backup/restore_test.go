package backup

import (
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDetectRBACObject(t *testing.T) {
	b := &Backuper{}

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

func TestPrepareRestoreMapping(t *testing.T) {
	testCases := []struct {
		name                    string
		objectMapping           []string
		objectType              string
		expectedTableMapping    map[string]string
		expectedDatabaseMapping map[string]string
		expectedErr             error
	}{
		{
			name:          "Simple table mapping - just table names",
			objectMapping: []string{"transaction_archive:transaction_archive_v4"},
			objectType:    "table",
			expectedTableMapping: map[string]string{
				"transaction_archive": "transaction_archive_v4",
			},
			expectedDatabaseMapping: map[string]string{},
			expectedErr:             nil,
		},
		{
			name:          "Fully qualified table mapping - database.table format",
			objectMapping: []string{"m_views_tables.transaction_archive:m_views_tables.transaction_archive_v4"},
			objectType:    "table",
			expectedTableMapping: map[string]string{
				"transaction_archive": "transaction_archive_v4",
			},
			expectedDatabaseMapping: map[string]string{},
			expectedErr:             nil,
		},
		{
			name:          "Mixed format table mapping",
			objectMapping: []string{"db1.table1:table1_new,table2:table2_new"},
			objectType:    "table",
			expectedTableMapping: map[string]string{
				"table1": "table1_new",
				"table2": "table2_new",
			},
			expectedDatabaseMapping: map[string]string{},
			expectedErr:             nil,
		},
		{
			name:                 "Simple database mapping",
			objectMapping:        []string{"olddb:newdb"},
			objectType:           "database",
			expectedTableMapping: map[string]string{},
			expectedDatabaseMapping: map[string]string{
				"olddb": "newdb",
			},
			expectedErr: nil,
		},
		{
			name:          "Multiple table mappings",
			objectMapping: []string{"db.t1:db.t1_new,db.t2:db.t2_new,t3:t3_new"},
			objectType:    "table",
			expectedTableMapping: map[string]string{
				"t1": "t1_new",
				"t2": "t2_new",
				"t3": "t3_new",
			},
			expectedDatabaseMapping: map[string]string{},
			expectedErr:             nil,
		},
		{
			name:                    "Invalid format - missing colon",
			objectMapping:           []string{"invalid_mapping"},
			objectType:              "table",
			expectedTableMapping:    map[string]string{},
			expectedDatabaseMapping: map[string]string{},
			expectedErr:             fmt.Errorf("restore-table-mapping invalid_mapping should only have srcTable:destinationTable format for each map rule"),
		},
		{
			name:                    "Invalid object type",
			objectMapping:           []string{"a:b"},
			objectType:              "invalid",
			expectedTableMapping:    map[string]string{},
			expectedDatabaseMapping: map[string]string{},
			expectedErr:             fmt.Errorf("objectType must be one of `database` or `table`"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Backuper{
				cfg: &config.Config{
					General: config.GeneralConfig{
						RestoreTableMapping:    make(map[string]string),
						RestoreDatabaseMapping: make(map[string]string),
					},
				},
			}

			err := b.prepareRestoreMapping(tc.objectMapping, tc.objectType)

			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedTableMapping, b.cfg.General.RestoreTableMapping)
				assert.Equal(t, tc.expectedDatabaseMapping, b.cfg.General.RestoreDatabaseMapping)
			}
		})
	}
}

func TestChangeTablePatternFromRestoreMapping(t *testing.T) {
	testCases := []struct {
		name            string
		tablePattern    string
		objType         string
		tableMapping    map[string]string
		databaseMapping map[string]string
		expectedPattern string
	}{
		{
			name:         "Simple table name mapping",
			tablePattern: "transaction_archive",
			objType:      "table",
			tableMapping: map[string]string{
				"transaction_archive": "transaction_archive_v4",
			},
			databaseMapping: map[string]string{},
			expectedPattern: "transaction_archive_v4",
		},
		{
			name:         "Database.table pattern mapping",
			tablePattern: "m_views_tables.transaction_archive",
			objType:      "table",
			tableMapping: map[string]string{
				"transaction_archive": "transaction_archive_v4",
			},
			databaseMapping: map[string]string{},
			expectedPattern: "m_views_tables.transaction_archive_v4",
		},
		{
			name:         "Multiple tables with comma",
			tablePattern: "db1.table1,db2.table2",
			objType:      "table",
			tableMapping: map[string]string{
				"table1": "table1_new",
				"table2": "table2_new",
			},
			databaseMapping: map[string]string{},
			expectedPattern: "db1.table1_new,db2.table2_new",
		},
		{
			name:         "Table not in mapping - no change",
			tablePattern: "some_other_table",
			objType:      "table",
			tableMapping: map[string]string{
				"transaction_archive": "transaction_archive_v4",
			},
			databaseMapping: map[string]string{},
			expectedPattern: "some_other_table",
		},
		{
			name:         "Database mapping",
			tablePattern: "olddb.*",
			objType:      "database",
			tableMapping: map[string]string{},
			databaseMapping: map[string]string{
				"olddb": "newdb",
			},
			expectedPattern: "newdb.*",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Backuper{
				cfg: &config.Config{
					General: config.GeneralConfig{
						RestoreTableMapping:    tc.tableMapping,
						RestoreDatabaseMapping: tc.databaseMapping,
					},
				},
			}

			result := b.changeTablePatternFromRestoreMapping(tc.tablePattern, tc.objType)
			assert.Equal(t, tc.expectedPattern, result)
		})
	}
}
