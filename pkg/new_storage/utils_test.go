package new_storage

import (
	"log"
	"testing"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/stretchr/testify/assert"
)

func timeParse(s string) time.Time {
	t, err := time.Parse("2006-01-02T15-04-05", s)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func TestGetBackupsToDelete(t *testing.T) {
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "three", CreationDate: timeParse("2019-03-28T19-50-12")}, false, "", ""},
		{metadata.BackupMetadata{BackupName: "one", CreationDate: timeParse("2019-01-28T19-50-12")}, false, "", ""},
		{metadata.BackupMetadata{BackupName: "five", CreationDate: timeParse("2019-05-28T19-50-12")}, false, "", ""},
		{metadata.BackupMetadata{BackupName: "two", CreationDate: timeParse("2019-02-28T19-50-12")}, false, "", ""},
		{metadata.BackupMetadata{BackupName: "four", CreationDate: timeParse("2019-04-28T19-50-12")}, false, "", ""},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "two", CreationDate: timeParse("2019-02-28T19-50-12")}, false, "", ""},
		{metadata.BackupMetadata{BackupName: "one", CreationDate: timeParse("2019-01-28T19-50-12")}, false, "", ""},
	}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDelete([]Backup{testData[0]}, 3))
}
