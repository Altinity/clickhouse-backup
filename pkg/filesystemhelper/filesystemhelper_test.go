package filesystemhelper

import (
	"os"
	"path/filepath"
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

func TestHardlinkPart(t *testing.T) {
	srcPart := filepath.Join(t.TempDir(), "store", "abc", "abcdef", "all_1_1_0")
	files := map[string]string{
		"checksums.txt":               "checksums",
		"data.bin":                    "data",
		"delete-on-destroy.txt":       "",
		"txn_version.txt":             "txn",
		"metadata_version.txt":        "0",
		"p.proj/checksums.txt":        "projection checksums",
		"p.proj/metadata_version.txt": "0",
	}
	for name, content := range files {
		filePath := filepath.Join(srcPart, name)
		if err := os.MkdirAll(filepath.Dir(filePath), 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filePath, []byte(content), 0640); err != nil {
			t.Fatal(err)
		}
	}
	dstPart := filepath.Join(t.TempDir(), "shadow", "uuid", "store", "abc", "abcdef", "all_1_1_0")
	if err := HardlinkPart(srcPart+"/", dstPart); err != nil {
		t.Fatalf("HardlinkPart: %v", err)
	}
	for _, name := range []string{"checksums.txt", "data.bin", "p.proj/checksums.txt", "p.proj/metadata_version.txt"} {
		srcInfo, err := os.Stat(filepath.Join(srcPart, name))
		if err != nil {
			t.Fatal(err)
		}
		dstInfo, err := os.Stat(filepath.Join(dstPart, name))
		if err != nil {
			t.Fatalf("%s is not linked: %v", name, err)
		}
		if !os.SameFile(srcInfo, dstInfo) {
			t.Errorf("%s is not a hardlink of the source", name)
		}
	}
	for _, name := range []string{"delete-on-destroy.txt", "txn_version.txt", "metadata_version.txt"} {
		if _, err := os.Stat(filepath.Join(dstPart, name)); !os.IsNotExist(err) {
			t.Errorf("%s must be excluded from the snapshot, stat error: %v", name, err)
		}
	}
	err := HardlinkPart(filepath.Join(t.TempDir(), "all_2_2_0"), filepath.Join(t.TempDir(), "dst"))
	if !os.IsNotExist(err) {
		t.Errorf("a missing part must return a not-exist error, got %v", err)
	}
}
