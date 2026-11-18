package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetDefaultFIPSEndpoint(t *testing.T) {
	testCases := []struct {
		name     string
		version  string
		envValue *string
		expected *string
	}{
		{name: "fips build, unset defaults to true", version: "2.8.3-fips", envValue: nil, expected: new("true")},
		{name: "fips build, explicit false is kept", version: "2.8.3-fips", envValue: new("false"), expected: new("false")},
		{name: "fips build, explicit true is kept", version: "2.8.3-fips", envValue: new("true"), expected: new("true")},
		{name: "non-fips build, unset stays unset", version: "2.8.3", envValue: nil, expected: nil},
		{name: "non-fips build, explicit value is kept", version: "2.8.3", envValue: new("true"), expected: new("true")},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			if tc.envValue != nil {
				t.Setenv("AWS_USE_FIPS_ENDPOINT", *tc.envValue)
			} else {
				t.Setenv("AWS_USE_FIPS_ENDPOINT", "")
				r.NoError(os.Unsetenv("AWS_USE_FIPS_ENDPOINT"))
			}
			setDefaultFIPSEndpoint(tc.version)
			value, isSet := os.LookupEnv("AWS_USE_FIPS_ENDPOINT")
			if tc.expected == nil {
				r.False(isSet)
				return
			}
			r.True(isSet)
			r.Equal(*tc.expected, value)
		})
	}
}
