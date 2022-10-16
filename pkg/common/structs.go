package common

const (
	// TimeFormat - clickhouse compatibility time format
	TimeFormat = "2006-01-02 15:04:05"
)

// EmptyMap - like a python set, for less memory usage
type EmptyMap map[string]struct{}
