package main

import (
	"log"
	"testing"
	"time"

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
		Backup{Name: "three", Date: timeParse("2019-03-28T19-50-12")},
		Backup{Name: "five", Date: timeParse("2019-05-28T19-50-12")},
		Backup{Name: "two", Date: timeParse("2019-02-28T19-50-12")},
		Backup{Name: "one", Date: timeParse("2019-01-28T19-50-12")},
		Backup{Name: "four", Date: timeParse("2019-04-28T19-50-12")},
	}
	expectedData := []Backup{
		Backup{Name: "four", Date: timeParse("2019-04-28T19-50-12")},
		Backup{Name: "five", Date: timeParse("2019-05-28T19-50-12")},
	}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 3))
	assert.Equal(t, []Backup{}, GetBackupsToDelete([]Backup{testData[0]}, 3))
}
