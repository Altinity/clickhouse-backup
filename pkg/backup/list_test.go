package backup

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// captureStdout redirects os.Stdout for the duration of fn and returns everything written to it.
// printLiveTableRows/printBackupSections write directly to os.Stdout (json/yaml/csv/tsv branches)
// or build their own tabwriter around os.Stdout (text branch), so there is no injectable io.Writer.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w
	fnErr := fn()
	require.NoError(t, w.Close())
	os.Stdout = orig
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, readErr := r.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if readErr != nil {
			break
		}
	}
	return string(buf), fnErr
}

func sampleTableRows() []TableRow {
	return []TableRow{
		{Database: "db1", Table: "t1", TotalBytes: 100, Size: "100B", Parts: 2, Disks: []string{"default"}, DisksStr: "default", Skip: false, BackupType: "full"},
		{Database: "db1", Table: "t2", TotalBytes: 200, Size: "200B", Parts: 1, Disks: []string{"default", "s3"}, DisksStr: "default,s3", Skip: true, BackupType: "schema"},
	}
}

func TestPrintLiveTableRows(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		rows := sampleTableRows()
		out, err := captureStdout(t, func() error { return printLiveTableRows(rows, "json") })
		require.NoError(t, err)
		var got []TableRow
		require.NoError(t, json.Unmarshal([]byte(out), &got))
		// DisksStr is `json:"-"`, so it never round-trips; only Disks does.
		want := rows
		for i := range want {
			want[i].DisksStr = ""
		}
		assert.Equal(t, want, got)
	})

	t.Run("yaml", func(t *testing.T) {
		rows := sampleTableRows()
		out, err := captureStdout(t, func() error { return printLiveTableRows(rows, "yaml") })
		require.NoError(t, err)
		var got []TableRow
		require.NoError(t, yaml.Unmarshal([]byte(out), &got))
		// DisksStr is `yaml:"-"`, so it never round-trips; only Disks does.
		want := rows
		for i := range want {
			want[i].DisksStr = ""
		}
		assert.Equal(t, want, got)
	})

	t.Run("csv", func(t *testing.T) {
		rows := sampleTableRows()
		out, err := captureStdout(t, func() error { return printLiveTableRows(rows, "csv") })
		require.NoError(t, err)
		assert.Contains(t, out, "database,table")
		assert.Contains(t, out, "db1,t1")
	})

	t.Run("tsv", func(t *testing.T) {
		rows := sampleTableRows()
		out, err := captureStdout(t, func() error { return printLiveTableRows(rows, "tsv") })
		require.NoError(t, err)
		assert.Contains(t, out, "database\ttable")
		assert.Contains(t, out, "db1\tt1")
	})

	t.Run("text default marker uses BackupType", func(t *testing.T) {
		rows := sampleTableRows()
		out, err := captureStdout(t, func() error { return printLiveTableRows(rows, "text") })
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(out), "\n")
		require.Len(t, lines, 2)
		assert.Contains(t, lines[0], "db1.t1")
		assert.Contains(t, lines[0], "full")
		// second row has Skip=true, so marker must be "skip" even though BackupType is "schema"
		assert.Contains(t, lines[1], "db1.t2")
		assert.Contains(t, lines[1], "skip")
		assert.NotContains(t, lines[1], "schema")
	})

	t.Run("empty format defaults to text", func(t *testing.T) {
		rows := sampleTableRows()
		out, err := captureStdout(t, func() error { return printLiveTableRows(rows, "") })
		require.NoError(t, err)
		assert.Contains(t, out, "db1.t1")
	})

	t.Run("unknown format returns error", func(t *testing.T) {
		out, err := captureStdout(t, func() error { return printLiveTableRows(sampleTableRows(), "xml") })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown format 'xml'")
		assert.Empty(t, out)
	})

	t.Run("no rows still succeeds for every format", func(t *testing.T) {
		for _, format := range []string{"json", "yaml", "csv", "tsv", "text", ""} {
			out, err := captureStdout(t, func() error { return printLiveTableRows(nil, format) })
			require.NoError(t, err, "format=%s", format)
			_ = out
		}
	})
}

func sampleSections() []tableSection {
	return []tableSection{
		{
			BackupName:   "backup2",
			BackupType:   "local",
			TablePattern: "db1.*",
			Rows: []TableRow{
				{Database: "db1", Table: "zz", TotalBytes: 50, Size: "50B", Parts: 1, DisksStr: "default"},
				{Database: "db1", Table: "aa", TotalBytes: 150, Size: "150B", Parts: 3, DisksStr: "default", Skip: true},
			},
		},
	}
}

func TestPrintBackupSections(t *testing.T) {
	t.Run("json single section unwraps to object, not array", func(t *testing.T) {
		out, err := captureStdout(t, func() error { return printBackupSections(sampleSections(), "json") })
		require.NoError(t, err)
		var got InfoResult
		require.NoError(t, json.Unmarshal([]byte(out), &got))
		assert.Equal(t, "backup2", got.BackupName)
		assert.Equal(t, 2, got.TableCount)
		assert.EqualValues(t, 200, got.TotalBytes)
		assert.Equal(t, 4, got.TotalParts)
		// rows must come back sorted by database.table (aa before zz)
		require.Len(t, got.Tables, 2)
		assert.Equal(t, "aa", got.Tables[0].Table)
		assert.Equal(t, "zz", got.Tables[1].Table)
	})

	t.Run("json multiple sections wraps in array", func(t *testing.T) {
		sections := append(sampleSections(), tableSection{BackupName: "backup3", BackupType: "remote"})
		out, err := captureStdout(t, func() error { return printBackupSections(sections, "json") })
		require.NoError(t, err)
		var got []InfoResult
		require.NoError(t, json.Unmarshal([]byte(out), &got))
		assert.Len(t, got, 2)
		assert.Equal(t, "backup2", got[0].BackupName)
		assert.Equal(t, "backup3", got[1].BackupName)
	})

	t.Run("yaml single section unwraps to object", func(t *testing.T) {
		out, err := captureStdout(t, func() error { return printBackupSections(sampleSections(), "yaml") })
		require.NoError(t, err)
		var got InfoResult
		require.NoError(t, yaml.Unmarshal([]byte(out), &got))
		assert.Equal(t, "backup2", got.BackupName)
	})

	t.Run("yaml multiple sections wraps in array", func(t *testing.T) {
		sections := append(sampleSections(), tableSection{BackupName: "backup3", BackupType: "remote"})
		out, err := captureStdout(t, func() error { return printBackupSections(sections, "yaml") })
		require.NoError(t, err)
		var got []InfoResult
		require.NoError(t, yaml.Unmarshal([]byte(out), &got))
		assert.Len(t, got, 2)
	})

	t.Run("csv separates multiple sections with a blank line", func(t *testing.T) {
		sections := append(sampleSections(), tableSection{BackupName: "backup3", BackupType: "remote", Rows: []TableRow{{Database: "d", Table: "t"}}})
		out, err := captureStdout(t, func() error { return printBackupSections(sections, "csv") })
		require.NoError(t, err)
		assert.Contains(t, out, "\n\n")
		assert.Contains(t, out, "database,table")
	})

	t.Run("tsv uses tab delimiter", func(t *testing.T) {
		out, err := captureStdout(t, func() error { return printBackupSections(sampleSections(), "tsv") })
		require.NoError(t, err)
		assert.Contains(t, out, "database\ttable")
	})

	t.Run("text renders header, filter, sorted rows and TOTAL", func(t *testing.T) {
		out, err := captureStdout(t, func() error { return printBackupSections(sampleSections(), "text") })
		require.NoError(t, err)
		assert.Contains(t, out, "Backup:")
		assert.Contains(t, out, "backup2")
		assert.Contains(t, out, "local")
		assert.Contains(t, out, "Filter:")
		assert.Contains(t, out, "db1.*")
		assert.Contains(t, out, "TOTAL (2 tables)")
		aaIdx := strings.Index(out, "db1.aa")
		zzIdx := strings.Index(out, "db1.zz")
		require.NotEqual(t, -1, aaIdx)
		require.NotEqual(t, -1, zzIdx)
		assert.Less(t, aaIdx, zzIdx, "rows must be sorted by database.table")
		assert.Contains(t, out, "skip")
	})

	t.Run("text with no tables prints (no tables) placeholder", func(t *testing.T) {
		sections := []tableSection{{BackupName: "empty-backup", BackupType: "local"}}
		out, err := captureStdout(t, func() error { return printBackupSections(sections, "text") })
		require.NoError(t, err)
		assert.Contains(t, out, "(no tables)")
		assert.NotContains(t, out, "TOTAL")
	})

	t.Run("text without table pattern omits Filter line", func(t *testing.T) {
		sections := []tableSection{{BackupName: "b", BackupType: "local", Rows: []TableRow{{Database: "d", Table: "t"}}}}
		out, err := captureStdout(t, func() error { return printBackupSections(sections, "text") })
		require.NoError(t, err)
		assert.NotContains(t, out, "Filter:")
	})

	t.Run("text separates multiple sections with a blank line", func(t *testing.T) {
		sections := append(sampleSections(), tableSection{BackupName: "backup3", BackupType: "remote", Rows: []TableRow{{Database: "d", Table: "t"}}})
		out, err := captureStdout(t, func() error { return printBackupSections(sections, "text") })
		require.NoError(t, err)
		assert.Contains(t, out, "backup2")
		assert.Contains(t, out, "backup3")
	})

	t.Run("unknown format returns error", func(t *testing.T) {
		out, err := captureStdout(t, func() error { return printBackupSections(sampleSections(), "xml") })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown format 'xml'")
		assert.Empty(t, out)
	})

	t.Run("no sections still succeeds for every format", func(t *testing.T) {
		for _, format := range []string{"json", "yaml", "csv", "tsv", "text", ""} {
			_, err := captureStdout(t, func() error { return printBackupSections(nil, format) })
			require.NoError(t, err, "format=%s", format)
		}
	})
}

func TestSortTableRows(t *testing.T) {
	rows := []TableRow{
		{Database: "b", Table: "a"},
		{Database: "a", Table: "z"},
		{Database: "a", Table: "a"},
	}
	sortTableRows(rows)
	assert.Equal(t, "a", rows[0].Database)
	assert.Equal(t, "a", rows[0].Table)
	assert.Equal(t, "a", rows[1].Database)
	assert.Equal(t, "z", rows[1].Table)
	assert.Equal(t, "b", rows[2].Database)
}

func TestFilterSkippedRows(t *testing.T) {
	rows := []TableRow{
		{Database: "db", Table: "keep", Skip: false},
		{Database: "db", Table: "drop", Skip: true},
	}
	filtered := filterSkippedRows(rows)
	require.Len(t, filtered, 1)
	assert.Equal(t, "keep", filtered[0].Table)
}

func TestBuildInfoResults(t *testing.T) {
	sections := sampleSections()
	results := buildInfoResults(sections)
	require.Len(t, results, 1)
	assert.Equal(t, "backup2", results[0].BackupName)
	assert.Equal(t, "local", results[0].BackupType)
	assert.Equal(t, "db1.*", results[0].TablePattern)
	assert.Equal(t, 2, results[0].TableCount)
	assert.EqualValues(t, 200, results[0].TotalBytes)
	assert.Equal(t, "200B", results[0].TotalSize)
	assert.Equal(t, 4, results[0].TotalParts)
}
