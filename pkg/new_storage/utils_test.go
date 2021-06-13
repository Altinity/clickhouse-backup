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
		{metadata.BackupMetadata{BackupName: "three"}, false, "", "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "one"}, false, "", "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "five"}, false, "", "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "two"}, false, "", "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "four"}, false, "", "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "two"}, false, "", "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "one"}, false, "", "", timeParse("2019-03-28T19-50-11")},
	}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDelete([]Backup{testData[0]}, 3))
}
