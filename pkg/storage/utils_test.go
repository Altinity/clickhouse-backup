package storage

import (
	"log"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
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
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDeleteRemote([]Backup{testData[0]}, 3))
}

func TestGetBackupsToDeleteWithRequiredBackup(t *testing.T) {
	// fix https://github.com/Altinity/clickhouse-backup/issues/111
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
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDeleteRemote([]Backup{testData[0]}, 3))

	// fix https://github.com/Altinity/clickhouse-backup/issues/385
	testData = []Backup{
		{metadata.BackupMetadata{BackupName: "3", RequiredBackup: "2"}, false, "", "", timeParse("2019-03-28T19-50-13")},
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2019-03-28T19-50-11")},
		{metadata.BackupMetadata{BackupName: "5", RequiredBackup: "4"}, false, "", "", timeParse("2019-03-28T19-50-15")},
		{metadata.BackupMetadata{BackupName: "2", RequiredBackup: "1"}, false, "", "", timeParse("2019-03-28T19-50-12")},
		{metadata.BackupMetadata{BackupName: "4", RequiredBackup: "3"}, false, "", "", timeParse("2019-03-28T19-50-14")},
	}
	expectedData = []Backup{}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDeleteRemote([]Backup{testData[0]}, 3))

}

func TestGetBackupsToDeleteWithInvalidUploadDate(t *testing.T) {
	// fix https://github.com/Altinity/clickhouse-backup/issues/409
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2022-03-03T18-08-01")},
		{metadata.BackupMetadata{BackupName: "2"}, false, "", "", timeParse("2022-03-03T18-08-02")},
		{BackupMetadata: metadata.BackupMetadata{BackupName: "3"}, Legacy: false, FileExtension: "", Broken: ""}, // UploadDate initialized with default value
		{metadata.BackupMetadata{BackupName: "4"}, false, "", "", timeParse("2022-03-03T18-08-04")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "1"}, false, "", "", timeParse("2022-03-03T18-08-01")},
	}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 2))

}

func TestGetBackupsToDeleteWithRecursiveRequiredBackups(t *testing.T) {
	// fix https://github.com/Altinity/clickhouse-backup/issues/525
	testData := []Backup{
		{metadata.BackupMetadata{BackupName: "2022-09-01T05-00-01"}, false, "", "", timeParse("2022-09-01T05-00-01")},
		{metadata.BackupMetadata{BackupName: "2022-09-01T21-00-03", RequiredBackup: "2022-09-01T05-00-01"}, false, "", "", timeParse("2022-09-01T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-02T21-00-02", RequiredBackup: "2022-09-01T21-00-03"}, false, "", "", timeParse("2022-09-02T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-03T21-00-03", RequiredBackup: "2022-09-02T21-00-02"}, false, "", "", timeParse("2022-09-03T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-04T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, false, "", "", timeParse("2022-09-04T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-05T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, false, "", "", timeParse("2022-09-05T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-06T21-00-03", RequiredBackup: "2022-09-05T21-00-03"}, false, "", "", timeParse("2022-09-06T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-07T21-00-03", RequiredBackup: "2022-09-06T21-00-03"}, false, "", "", timeParse("2022-09-07T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-08T21-00-03", RequiredBackup: "2022-09-07T21-00-03"}, false, "", "", timeParse("2022-09-08T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-09T21-00-03", RequiredBackup: "2022-09-08T21-00-03"}, false, "", "", timeParse("2022-09-09T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-10T21-00-03", RequiredBackup: "2022-09-09T21-00-03"}, false, "", "", timeParse("2022-09-10T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-11T21-00-03", RequiredBackup: "2022-09-10T21-00-03"}, false, "", "", timeParse("2022-09-11T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-12T21-00-02", RequiredBackup: "2022-09-11T21-00-03"}, false, "", "", timeParse("2022-09-12T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-13T21-00-03", RequiredBackup: "2022-09-12T21-00-02"}, false, "", "", timeParse("2022-09-13T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-14T21-00-03", RequiredBackup: "2022-09-13T21-00-03"}, false, "", "", timeParse("2022-09-14T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-30-20"}, false, "", "", timeParse("2022-10-03T09-30-20")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-39-37", RequiredBackup: "2022-10-03T09-30-20"}, false, "", "", timeParse("2022-10-03T09-39-37")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-40-03", RequiredBackup: "2022-10-03T09-39-37"}, false, "", "", timeParse("2022-10-03T09-40-03")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-41-31", RequiredBackup: "2022-10-03T09-40-03"}, false, "", "", timeParse("2022-10-03T09-41-31")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T09-52-12", RequiredBackup: "2022-10-03T09-41-31"}, false, "", "", timeParse("2022-10-03T09-52-12")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-11-15", RequiredBackup: "2022-10-03T09-52-12"}, false, "", "", timeParse("2022-10-03T10-11-15")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-12-38", RequiredBackup: "2022-10-03T10-11-15"}, false, "", "", timeParse("2022-10-03T10-12-38")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-12-57", RequiredBackup: "2022-10-03T10-12-38"}, false, "", "", timeParse("2022-10-03T10-12-57")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-13-16", RequiredBackup: "2022-10-03T10-12-57"}, false, "", "", timeParse("2022-10-03T10-13-16")},
		{metadata.BackupMetadata{BackupName: "2022-10-03T10-15-32", RequiredBackup: "2022-10-03T10-13-16"}, false, "", "", timeParse("2022-10-03T10-15-32")},
	}
	expectedData := []Backup{
		{metadata.BackupMetadata{BackupName: "2022-09-14T21-00-03", RequiredBackup: "2022-09-13T21-00-03"}, false, "", "", timeParse("2022-09-14T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-13T21-00-03", RequiredBackup: "2022-09-12T21-00-02"}, false, "", "", timeParse("2022-09-13T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-12T21-00-02", RequiredBackup: "2022-09-11T21-00-03"}, false, "", "", timeParse("2022-09-12T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-11T21-00-03", RequiredBackup: "2022-09-10T21-00-03"}, false, "", "", timeParse("2022-09-11T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-10T21-00-03", RequiredBackup: "2022-09-09T21-00-03"}, false, "", "", timeParse("2022-09-10T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-09T21-00-03", RequiredBackup: "2022-09-08T21-00-03"}, false, "", "", timeParse("2022-09-09T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-08T21-00-03", RequiredBackup: "2022-09-07T21-00-03"}, false, "", "", timeParse("2022-09-08T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-07T21-00-03", RequiredBackup: "2022-09-06T21-00-03"}, false, "", "", timeParse("2022-09-07T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-06T21-00-03", RequiredBackup: "2022-09-05T21-00-03"}, false, "", "", timeParse("2022-09-06T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-05T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, false, "", "", timeParse("2022-09-05T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-04T21-00-03", RequiredBackup: "2022-09-04T21-00-03"}, false, "", "", timeParse("2022-09-04T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-03T21-00-03", RequiredBackup: "2022-09-02T21-00-02"}, false, "", "", timeParse("2022-09-03T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-02T21-00-02", RequiredBackup: "2022-09-01T21-00-03"}, false, "", "", timeParse("2022-09-02T21-00-02")},
		{metadata.BackupMetadata{BackupName: "2022-09-01T21-00-03", RequiredBackup: "2022-09-01T05-00-01"}, false, "", "", timeParse("2022-09-01T21-00-03")},
		{metadata.BackupMetadata{BackupName: "2022-09-01T05-00-01"}, false, "", "", timeParse("2022-09-01T05-00-01")},
	}
	assert.Equal(t, expectedData, GetBackupsToDeleteRemote(testData, 6))
}
