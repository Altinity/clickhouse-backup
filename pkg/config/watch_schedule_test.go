package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseWatchSchedule(t *testing.T) {
	s, err := ParseWatchSchedule("name=daily,full=0 0 * * 1,5,increment=30 * * * *,full_type=rebase,delete_previous_cycle=true")
	require.NoError(t, err)
	assert.Equal(t, "daily", s.Name)
	assert.Equal(t, "0 0 * * 1,5", s.Full)
	assert.Equal(t, "30 * * * *", s.Increment)
	assert.Equal(t, "rebase", s.FullType)
	assert.True(t, s.DeletePreviousCycle)

	s, err = ParseWatchSchedule("name=weekly,full=@weekly")
	require.NoError(t, err)
	assert.Equal(t, "weekly", s.Name)
	assert.Equal(t, "@weekly", s.Full)
	assert.Equal(t, "", s.Increment)
	assert.Equal(t, "create", s.FullType, "empty full_type shall normalize to create")
	assert.False(t, s.DeletePreviousCycle)

	// optional leading seconds field
	_, err = ParseWatchSchedule("name=fast,full=*/40 * * * * *,increment=*/10 * * * * *")
	require.NoError(t, err)
}

func TestParseWatchScheduleErrors(t *testing.T) {
	testCases := []struct {
		param    string
		expected string
	}{
		{"", "invalid schedule"},
		{"garbage", "invalid schedule"},
		{"garbage,name=x,full=* * * * *", "invalid schedule"},
		{"full=* * * * *", "non-empty `name`"},
		{"name=x", "requires `full` cron expression"},
		{"name=x,full=* * * * *,full_type=wrong", "invalid `full_type`"},
		{"name=x,full=invalid cron", "invalid `full` cron expression"},
		{"name=x,full=* * * * *,increment=invalid cron", "invalid `increment` cron expression"},
		{"name=x,full=* * * * *,name=y", "duplicate key `name`"},
		{"name=bad name!,full=* * * * *", "only [a-zA-Z0-9_-] allowed"},
		{"name=x,full=* * * * *,delete_previous_cycle=maybe", "`delete_previous_cycle` expects true or false"},
	}
	for _, tc := range testCases {
		_, err := ParseWatchSchedule(tc.param)
		require.Error(t, err, tc.param)
		assert.Contains(t, err.Error(), tc.expected, tc.param)
	}
}

func TestParseWatchSchedules(t *testing.T) {
	ws, err := ParseWatchSchedules([]string{"name=daily,full=0 0 * * *,increment=0 * * * *", "name=weekly,full=0 0 * * 0"})
	require.NoError(t, err)
	require.Len(t, ws, 2)

	_, err = ParseWatchSchedules([]string{"name=daily,full=0 0 * * *", "name=daily,full=0 0 * * 0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate watch schedule name")

	// name works as backup name prefix, one name shall not be a prefix of another
	_, err = ParseWatchSchedules([]string{"name=daily,full=0 0 * * *", "name=daily-extra,full=0 0 * * 0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup chains will overlap")
}

// TestWatchScheduleValidate - direct Validate() coverage for the yaml config path,
// where WatchSchedule structs come from yaml decoding without ParseWatchSchedule
func TestWatchScheduleValidate(t *testing.T) {
	s := WatchSchedule{Name: "daily", Full: "0 0 * * *"}
	require.NoError(t, s.Validate())
	assert.Equal(t, "create", s.FullType, "empty full_type shall normalize to create")

	testCases := []struct {
		schedule WatchSchedule
		expected string
	}{
		{WatchSchedule{Full: "0 0 * * *"}, "non-empty `name`"},
		{WatchSchedule{Name: "bad name!", Full: "0 0 * * *"}, "only [a-zA-Z0-9_-] allowed"},
		{WatchSchedule{Name: "x"}, "requires `full` cron expression"},
		{WatchSchedule{Name: "x", Full: "invalid cron"}, "invalid `full` cron expression"},
		{WatchSchedule{Name: "x", Full: "0 0 * * *", Increment: "invalid cron"}, "invalid `increment` cron expression"},
		{WatchSchedule{Name: "x", Full: "0 0 * * *", FullType: "wrong"}, "invalid `full_type`"},
	}
	for _, tc := range testCases {
		err := tc.schedule.Validate()
		require.Error(t, err, "%+v", tc.schedule)
		assert.Contains(t, err.Error(), tc.expected, "%+v", tc.schedule)
	}

	// slice level checks for yaml decoded config
	require.Error(t, WatchSchedules{{Name: "a", Full: "0 0 * * *"}, {Name: "a", Full: "@daily"}}.Validate())
	require.Error(t, WatchSchedules{{Name: "a", Full: "0 0 * * *"}, {Name: "a-b", Full: "@daily"}}.Validate())
	require.NoError(t, WatchSchedules{{Name: "a", Full: "0 0 * * *"}, {Name: "b", Full: "@daily"}}.Validate())
}

func TestWatchSchedulesDecode(t *testing.T) {
	ws := WatchSchedules{}
	require.NoError(t, ws.Decode("name=daily,full=0 0 * * 1,5,increment=0 * * * *;name=weekly,full=@weekly"))
	require.Len(t, ws, 2)
	assert.Equal(t, "0 0 * * 1,5", ws[0].Full)
	assert.Equal(t, "weekly", ws[1].Name)

	require.NoError(t, ws.Decode(""))
	assert.Nil(t, ws)
}
