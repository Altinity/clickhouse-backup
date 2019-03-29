package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBackupsToDelete(t *testing.T) {
	testData := []string{
		"2019-04-28T19-50-12",
		"this-is-no-backup",
		"2019-03-28T19-50-12.tar",
		"2018-01-28T19-50-12",
		"2019-02-28T12-50-12",
		"2019-01-2819-50-12",
		"2019-03-28T19-50-12.back",
		"2019-06-28T19-50-13.tar",
	}
	expectedData := []string{
		"2018-01-28T19-50-12",
		"2019-02-28T12-50-12",
	}
	assert.Equal(t, expectedData, GetBackupsToDelete(testData, 3))
	assert.Equal(t, []string{}, GetBackupsToDelete([]string{"2019-03-28T19-50-12"}, 3))
}
