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

func TestGetBackupsToDeleteWithRequiredBackup(t *testing.T) {
	// fix https://github.com/AlexAkulov/clickhouse-backup/issues/111
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "3"}, false, "", "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "5", RequiredBackup: "2"}, false, "", "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "2"}, false, "", "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "4", RequiredBackup: "3"}, false, "", "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2019-03-28T19-50-11")},
	}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDelete([]Backup{testData[0]}, 3))

	// fix https://github.com/AlexAkulov/clickhouse-backup/issues/385
	testData = []Backup{
		{metadata.BackupMetadata{BackupName: "3", RequiredBackup: "2"}, false, "", "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "5", RequiredBackup: "4"}, false, "", "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "2", RequiredBackup: "1"}, false, "", "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "4", RequiredBackup: "3"}, false, "", "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData = []Backup{}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDelete([]Backup{testData[0]}, 3))

}

func TestGetBackupsToDeleteWithInvalidUploadDate(t *testing.T) {
	// fix https://github.com/AlexAkulov/clickhouse-backup/issues/409
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2022-03-03T18-08-01")},
		{metadata.BackupMetadata{BackupName: "2"}, false, "", "", timeParse("2022-03-03T18-08-02")},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "3"}, Legacy: false, FileExtension: "", Broken: ""}, // UploadDate initialized with default value
		{metadata.BackupMetadata{BackupName: "4"}, false, "", "", timeParse("2022-03-03T18-08-04")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2022-03-03T18-08-01")},
	}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 2))

}
