package filesystemhelper

import (
	"testing"
)

func TestIsSkipProjections(t *testing.T) {
	tests := []struct {
		name            string
		skipProjections []string
		relativePath    string
		expectedResult  bool
	}{
		{
			name:            "Not match with nil",
			skipProjections: nil,
			relativePath:    "db/table/part/projection.proj/file",
			expectedResult:  false,
		},
		{
			name:            "Not match with empty pattern",
			skipProjections: []string{},
			relativePath:    "db/table/part/projection.proj/file",
			expectedResult:  false,
		},
		{
			name:            "Match with wildcard pattern",
			skipProjections: []string{"*"},
			relativePath:    "db/table/part/projection.proj/file",
			expectedResult:  true,
		},
		{
			name:            "Match with specific db and table",
			skipProjections: []string{"db.table:projection"},
			relativePath:    "db/table/part/projection.proj/file",
			expectedResult:  true,
		},
		{
			name:            "No match with specific db and table",
			skipProjections: []string{"db.table:projection"},
			relativePath:    "db/table/part/other.proj/file",
			expectedResult:  false,
		},
		{
			name:            "Match with specific table and wildcard projection",
			skipProjections: []string{"table:*"},
			relativePath:    "db/table/part/projection.proj/file",
			expectedResult:  true,
		},
		{
			name:            "No match with specific table and wildcard projection",
			skipProjections: []string{"table:*"},
			relativePath:    "db/othertable/part/projection.proj/file",
			expectedResult:  false,
		},
		{
			name:            "Match with multiple patterns",
			skipProjections: []string{"db.table:projection", "otherdb.othertable:otherprojection"},
			relativePath:    "otherdb/othertable/part/otherprojection.proj/file",
			expectedResult:  true,
		},
		{
			name:            "No match with multiple patterns",
			skipProjections: []string{"db.table:projection", "otherdb.othertable:otherprojection"},
			relativePath:    "db/table/part/otherprojection.proj/file",
			expectedResult:  false,
		},
		{
			name:            "Match with multiple wildcard patterns",
			skipProjections: []string{"db.table:*", "db2.*:othe?projection,other*.*table:*projection"},
			relativePath:    "otherdb/othertable/part/otherprojection.proj/file",
			expectedResult:  true,
		},
		{
			name:            "No match with multiple wildcard patterns",
			skipProjections: []string{"db*.table:?projection", "otherd?.othertab*:otherprojection"},
			relativePath:    "db/table/part/otherprojection.proj/file",
			expectedResult:  false,
		},
		{
			name:            "Match with real file",
			skipProjections: []string{"default.*"},
			relativePath:    "default/table_with_projection/20250124_2_2_0/x.proj/columns.txt",
			expectedResult:  true,
		},
		{
			name:            "Match with real dir",
			skipProjections: []string{"default.*"},
			relativePath:    "default/table_with_projection/20250124_2_2_0/x.proj",
			expectedResult:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSkipProjections(tt.skipProjections, tt.relativePath)
			if result != tt.expectedResult {
				t.Errorf("IsSkipProjections() = %v, want %v", result, tt.expectedResult)
			}
		})
	}
}
