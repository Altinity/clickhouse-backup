package log_helper

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestSetLogLevelFromStringPositive tests valid log levels
func TestSetLogLevelFromStringPositive(t *testing.T) {
	// Test all valid log levels
	testCases := []struct {
		name          string
		inputLevel    string
		expectedLevel zerolog.Level
	}{
		{
			name:          "error level",
			inputLevel:    "error",
			expectedLevel: zerolog.ErrorLevel,
		},
		{
			name:          "warning level",
			inputLevel:    "warning",
			expectedLevel: zerolog.WarnLevel,
		},
		{
			name:          "info level",
			inputLevel:    "info",
			expectedLevel: zerolog.InfoLevel,
		},
		{
			name:          "debug level",
			inputLevel:    "debug",
			expectedLevel: zerolog.DebugLevel,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set the log level
			SetLogLevelFromString(tc.inputLevel)

			// Verify the global level was set correctly
			actualLevel := zerolog.GlobalLevel()
			require.Equal(t, tc.expectedLevel, actualLevel,
				"Expected level %v, got %v", tc.expectedLevel, actualLevel)
		})
	}
}

// TestSetLogLevelFromStringNegative tests invalid log level that triggers warning
func TestSetLogLevelFromStringNegative(t *testing.T) {
	testCases := []struct {
		name        string
		inputLevel  string
		expectWarn  bool
		warnMessage string
	}{
		{
			name:        "invalid log level - unknown",
			inputLevel:  "unknown",
			expectWarn:  true,
			warnMessage: "unexpected log_level=unknown, will apply `info`",
		},
		{
			name:        "invalid log level - empty string",
			inputLevel:  "",
			expectWarn:  true,
			warnMessage: "unexpected log_level=, will apply `info`",
		},
		{
			name:        "invalid log level - wrong case",
			inputLevel:  "ERROR",
			expectWarn:  true,
			warnMessage: "unexpected log_level=ERROR, will apply `info`",
		},
		{
			name:        "invalid log level - typo",
			inputLevel:  "infoo",
			expectWarn:  true,
			warnMessage: "unexpected log_level=infoo, will apply `info`",
		},
		{
			name:        "invalid log level - trace (not in map)",
			inputLevel:  "trace",
			expectWarn:  true,
			warnMessage: "unexpected log_level=trace, will apply `info`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Capture log output
			var buf bytes.Buffer
			originalLogger := log.Logger
			originalGlobalLevel := zerolog.GlobalLevel()
			defer func() {
				// Restore original logger and level
				log.Logger = originalLogger
				zerolog.SetGlobalLevel(originalGlobalLevel)
			}()

			// Set global level to warn to ensure warnings are logged
			zerolog.SetGlobalLevel(zerolog.WarnLevel)
			log.Logger = zerolog.New(&buf).With().Timestamp().Logger()

			// Set the invalid log level
			SetLogLevelFromString(tc.inputLevel)

			// Verify the global level was set to Info (default)
			actualLevel := zerolog.GlobalLevel()
			require.Equal(t, zerolog.InfoLevel, actualLevel,
				"Expected default level Info, got %v", actualLevel)

			// Verify warning was logged
			output := buf.String()
			if tc.expectWarn {
				require.Contains(t, output, "unexpected log_level",
					"Expected warning in output, got: %s", output)
				require.Contains(t, output, tc.inputLevel,
					"Expected input level %s in warning, got: %s", tc.inputLevel, output)
				require.Contains(t, output, "will apply `info`",
					"Expected 'will apply `info`' in warning, got: %s", output)
			}
		})
	}
}

// TestSetLogLevelFromStringCaseSensitivity ensures log level is case sensitive
func TestSetLogLevelFromStringCaseSensitivity(t *testing.T) {
	testCases := []struct {
		inputLevel    string
		shouldBeValid bool
	}{
		{"error", true},
		{"Error", false},
		{"ERROR", false},
		{"warning", true},
		{"Warning", false},
		{"WARNING", false},
		{"info", true},
		{"Info", false},
		{"INFO", false},
		{"debug", true},
		{"Debug", false},
		{"DEBUG", false},
	}

	for _, tc := range testCases {
		t.Run(tc.inputLevel, func(t *testing.T) {
			var buf bytes.Buffer
			originalLogger := log.Logger
			originalGlobalLevel := zerolog.GlobalLevel()
			defer func() {
				log.Logger = originalLogger
				zerolog.SetGlobalLevel(originalGlobalLevel)
			}()

			// Set global level to warn to ensure warnings are logged
			zerolog.SetGlobalLevel(zerolog.WarnLevel)
			log.Logger = zerolog.New(&buf).With().Timestamp().Logger()

			SetLogLevelFromString(tc.inputLevel)

			output := buf.String()
			hasWarning := strings.Contains(output, "unexpected log_level")

			if tc.shouldBeValid {
				require.False(t, hasWarning,
					"Expected no warning for valid level %s, got: %s", tc.inputLevel, output)
			} else {
				require.True(t, hasWarning,
					"Expected warning for invalid level %s, got: %s", tc.inputLevel, output)
			}
		})
	}
}

// TestSetLogLevelFromStringBehavior tests that the function actually changes global level
func TestSetLogLevelFromStringBehavior(t *testing.T) {
	// Set to debug
	SetLogLevelFromString("debug")
	require.Equal(t, zerolog.DebugLevel, zerolog.GlobalLevel())

	// Set to error
	SetLogLevelFromString("error")
	require.Equal(t, zerolog.ErrorLevel, zerolog.GlobalLevel())

	// Set to warning
	SetLogLevelFromString("warning")
	require.Equal(t, zerolog.WarnLevel, zerolog.GlobalLevel())

	// Set to info
	SetLogLevelFromString("info")
	require.Equal(t, zerolog.InfoLevel, zerolog.GlobalLevel())

	// Set invalid - should default to info
	SetLogLevelFromString("invalid")
	require.Equal(t, zerolog.InfoLevel, zerolog.GlobalLevel())
}

// TestSetLogLevelFromStringEmptyAndSpecialChars tests edge cases
func TestSetLogLevelFromStringEmptyAndSpecialChars(t *testing.T) {
	testCases := []string{
		"",
		" ",
		"  info  ",
		"info\n",
		"\tinfo",
		"inf o",
		"info debug",
		"null",
		"nil",
		"0",
		"-1",
		"fatal", // valid in zerolog but not in our map
		"panic", // valid in zerolog but not in our map
		"trace", // valid in zerolog but not in our map
	}

	for _, input := range testCases {
		t.Run("input_"+input, func(t *testing.T) {
			var buf bytes.Buffer
			originalLogger := log.Logger
			originalGlobalLevel := zerolog.GlobalLevel()
			defer func() {
				log.Logger = originalLogger
				zerolog.SetGlobalLevel(originalGlobalLevel)
			}()

			// Set global level to warn to ensure warnings are logged
			zerolog.SetGlobalLevel(zerolog.WarnLevel)
			log.Logger = zerolog.New(&buf).With().Timestamp().Logger()

			SetLogLevelFromString(input)

			// All invalid inputs should default to info
			require.Equal(t, zerolog.InfoLevel, zerolog.GlobalLevel(),
				"Expected default level Info for input %q", input)

			// All should produce warning
			output := buf.String()
			require.Contains(t, output, "unexpected log_level",
				"Expected warning for input %q, got: %s", input, output)
		})
	}
}
